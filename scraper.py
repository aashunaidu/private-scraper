import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import re
import time
import unicodedata
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp
import asyncpg
from bs4 import BeautifulSoup

LOGGER = logging.getLogger("layer2_scraper")

USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

SOFT_ERROR_CODES = {408, 425, 429, 500, 502, 503, 504}
HARD_ERROR_CODES = {404, 410}
DATE_LINE_RE = re.compile(
    r"(?im)^\s*(last\s+updated|date\s+modified|updated\s+on|modified\s+on|updated)\s*[:\-].*$"
)
TIMESTAMP_LINE_RE = re.compile(r"(?im)^\s*\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}(:\d{2})?.*$")


@dataclass(slots=True)
class ClaimedDocument:
    document_id: int
    url_id: int
    url: str
    etag: str | None
    source_last_modified: datetime | None


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=20)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def ensure_documents_seeded(self) -> None:
        assert self._pool
        sql = """
        INSERT INTO documents (url_id, scrape_status, next_scrape_at)
        SELECT u.id, 'pending', NOW()
        FROM urls u
        LEFT JOIN documents d ON d.url_id = u.id
        WHERE u.relevant = TRUE
          AND u.status = 'visited'
          AND d.id IS NULL
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    async def claim_documents(self, batch_size: int, worker_id: str) -> list[ClaimedDocument]:
        assert self._pool
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM claim_documents($1, $2)", batch_size, worker_id)
        return [
            ClaimedDocument(
                document_id=row["document_id"],
                url_id=row["url_id"],
                url=row["url"],
                etag=row["etag"],
                source_last_modified=row["source_last_modified"],
            )
            for row in rows
        ]

    async def get_latest_version(self, document_id: int) -> asyncpg.Record | None:
        assert self._pool
        sql = """
        SELECT id, version_number, content_hash
        FROM document_versions
        WHERE document_id = $1 AND is_latest = TRUE
        LIMIT 1
        """
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(sql, document_id)

    async def mark_not_modified(self, document_id: int, etag: str | None, last_modified: datetime | None) -> None:
        assert self._pool
        sql = """
        UPDATE documents
        SET scrape_status = 'success',
            error_count = 0,
            last_scraped_at = NOW(),
            next_scrape_at = NOW() + INTERVAL '24 hours',
            etag = COALESCE($2, etag),
            source_last_modified = COALESCE($3, source_last_modified),
            locked_at = NULL,
            locked_by = NULL
        WHERE id = $1
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql, document_id, etag, last_modified)

    async def save_new_version(
        self,
        document_id: int,
        markdown: str,
        content_hash: str,
        parser_metadata: dict[str, Any],
        title: str | None,
        doc_type: str,
        mime: str | None,
        etag: str | None,
        last_modified: datetime | None,
        chunks: list[dict[str, Any]],
    ) -> int:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                latest = await conn.fetchrow(
                    "SELECT id, version_number FROM document_versions WHERE document_id = $1 AND is_latest = TRUE LIMIT 1",
                    document_id,
                )
                if latest:
                    await conn.execute("UPDATE document_versions SET is_latest = FALSE WHERE id = $1", latest["id"])
                    next_version = latest["version_number"] + 1
                else:
                    next_version = 1

                version = await conn.fetchrow(
                    """
                    INSERT INTO document_versions (document_id, version_number, content_hash, markdown_content, parser_metadata, is_latest)
                    VALUES ($1, $2, $3, $4, $5::jsonb, TRUE)
                    ON CONFLICT (document_id, content_hash)
                    DO UPDATE SET parser_metadata = EXCLUDED.parser_metadata
                    RETURNING id
                    """,
                    document_id,
                    next_version,
                    content_hash,
                    markdown,
                    json.dumps(parser_metadata),
                )
                version_id = int(version["id"])

                await conn.execute("DELETE FROM chunks WHERE document_version_id = $1", version_id)
                if chunks:
                    await conn.executemany(
                        """
                        INSERT INTO chunks (document_version_id, chunk_index, chunk_text, heading_path, token_count)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        [
                            (version_id, c["chunk_index"], c["chunk_text"], c["heading_path"], c["token_count"])
                            for c in chunks
                        ],
                    )

                await conn.execute(
                    """
                    UPDATE documents
                    SET scrape_status = 'success',
                        last_scraped_at = NOW(),
                        next_scrape_at = NOW() + INTERVAL '24 hours',
                        error_count = 0,
                        last_error = NULL,
                        latest_version_id = $2,
                        title = COALESCE($3, title),
                        doc_type = $4,
                        detected_mime = COALESCE($5, detected_mime),
                        etag = COALESCE($6, etag),
                        source_last_modified = COALESCE($7, source_last_modified),
                        locked_at = NULL,
                        locked_by = NULL
                    WHERE id = $1
                    """,
                    document_id,
                    version_id,
                    title,
                    doc_type,
                    mime,
                    etag,
                    last_modified,
                )
        return version_id

    async def mark_unchanged(
        self,
        document_id: int,
        title: str | None,
        doc_type: str,
        mime: str | None,
        etag: str | None,
        last_modified: datetime | None,
    ) -> None:
        assert self._pool
        sql = """
        UPDATE documents
        SET scrape_status = 'success',
            last_scraped_at = NOW(),
            next_scrape_at = NOW() + INTERVAL '24 hours',
            error_count = 0,
            last_error = NULL,
            title = COALESCE($2, title),
            doc_type = $3,
            detected_mime = COALESCE($4, detected_mime),
            etag = COALESCE($5, etag),
            source_last_modified = COALESCE($6, source_last_modified),
            locked_at = NULL,
            locked_by = NULL
        WHERE id = $1
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql, document_id, title, doc_type, mime, etag, last_modified)

    async def mark_failure(self, document_id: int, status_code: int | None, error: str) -> None:
        assert self._pool
        if status_code in HARD_ERROR_CODES:
            sql = """
            UPDATE documents
            SET scrape_status = 'archived',
                next_scrape_at = NULL,
                error_count = error_count + 1,
                last_error = $2,
                locked_at = NULL,
                locked_by = NULL
            WHERE id = $1
            """
        else:
            sql = """
            UPDATE documents
            SET scrape_status = 'failed',
                error_count = error_count + 1,
                last_error = $2,
                next_scrape_at = CASE
                    WHEN error_count + 1 >= 10 THEN NULL
                    ELSE NOW() + INTERVAL '1 hour'
                END,
                locked_at = NULL,
                locked_by = NULL
            WHERE id = $1
            """
        async with self._pool.acquire() as conn:
            await conn.execute(sql, document_id, error[:4000])


class DomainRateLimiter:
    def __init__(self, delay_seconds: float = 2.0):
        self.delay_seconds = delay_seconds
        self._last_seen: dict[str, float] = {}
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def wait(self, domain: str) -> None:
        async with self._locks[domain]:
            now = time.monotonic()
            last = self._last_seen.get(domain)
            if last is not None:
                delta = now - last
                if delta < self.delay_seconds:
                    await asyncio.sleep(self.delay_seconds - delta)
            self._last_seen[domain] = time.monotonic()


class NormalizerHasher:
    @staticmethod
    def normalize(markdown: str) -> str:
        text = unicodedata.normalize("NFKC", markdown)
        text = DATE_LINE_RE.sub("", text)
        text = TIMESTAMP_LINE_RE.sub("", text)
        text = re.sub(r"(?im)^\s*(privacy|terms|all rights reserved).*$", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    @staticmethod
    def hash_content(normalized_markdown: str) -> str:
        return hashlib.sha256(normalized_markdown.encode("utf-8")).hexdigest()


class SemanticChunker:
    def __init__(self, min_tokens: int = 500, max_tokens: int = 1000, max_chars: int = 5000, overlap_tokens: int = 50):
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.max_chars = max_chars
        self.overlap_tokens = overlap_tokens

    @staticmethod
    def estimate_tokens(text: str) -> int:
        return max(1, len(text) // 4)

    def chunk(self, markdown: str) -> list[dict[str, Any]]:
        sections = self._split_sections(markdown)
        chunks: list[dict[str, Any]] = []
        chunk_index = 0

        for heading_path, section_text in sections:
            sentences = self._sentences(section_text)
            current: list[str] = []
            for sentence in sentences:
                candidate = " ".join(current + [sentence]).strip()
                if not candidate:
                    continue
                if len(candidate) > self.max_chars or self.estimate_tokens(candidate) > self.max_tokens:
                    if current:
                        chunk_text = " ".join(current).strip()
                        chunks.append(
                            {
                                "chunk_index": chunk_index,
                                "chunk_text": chunk_text,
                                "heading_path": heading_path,
                                "token_count": self.estimate_tokens(chunk_text),
                            }
                        )
                        chunk_index += 1

                        overlap = self._overlap_tail(current)
                        current = overlap + [sentence]
                    else:
                        chunks.append(
                            {
                                "chunk_index": chunk_index,
                                "chunk_text": sentence,
                                "heading_path": heading_path,
                                "token_count": self.estimate_tokens(sentence),
                            }
                        )
                        chunk_index += 1
                else:
                    current.append(sentence)
            if current:
                chunk_text = " ".join(current).strip()
                chunks.append(
                    {
                        "chunk_index": chunk_index,
                        "chunk_text": chunk_text,
                        "heading_path": heading_path,
                        "token_count": self.estimate_tokens(chunk_text),
                    }
                )
                chunk_index += 1

        return chunks

    def _overlap_tail(self, sentences: list[str]) -> list[str]:
        if not sentences:
            return []
        tail: deque[str] = deque()
        token_sum = 0
        for sentence in reversed(sentences):
            tokens = self.estimate_tokens(sentence)
            if token_sum + tokens > self.overlap_tokens:
                break
            tail.appendleft(sentence)
            token_sum += tokens
        return list(tail)

    def _split_sections(self, markdown: str) -> list[tuple[str, str]]:
        lines = markdown.splitlines()
        sections: list[tuple[str, str]] = []
        stack: list[str] = []
        buffer: list[str] = []

        def flush() -> None:
            if buffer:
                sections.append((" > ".join(stack) if stack else "Document", "\n".join(buffer).strip()))
                buffer.clear()

        for line in lines:
            heading = re.match(r"^(#{1,6})\s+(.+)$", line)
            if heading:
                flush()
                level = len(heading.group(1))
                title = heading.group(2).strip()
                stack[:] = stack[: level - 1]
                if title:
                    stack.append(title)
            else:
                if line.strip():
                    buffer.append(line)
        flush()

        return [s for s in sections if s[1]] or [("Document", markdown)]

    @staticmethod
    def _sentences(text: str) -> list[str]:
        parts = re.split(r"(?<=[.!?])\s+", text.strip())
        return [p.strip() for p in parts if p.strip()]


class Extractor:
    def __init__(self):
        self._ua_idx = 0

    def next_user_agent(self) -> str:
        ua = USER_AGENTS[self._ua_idx % len(USER_AGENTS)]
        self._ua_idx += 1
        return ua

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        doc: ClaimedDocument,
    ) -> tuple[int, dict[str, str], bytes]:
        headers = {"User-Agent": self.next_user_agent(), "Accept": "*/*"}
        if doc.etag:
            headers["If-None-Match"] = doc.etag
        if doc.source_last_modified:
            headers["If-Modified-Since"] = doc.source_last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")

        async with session.get(doc.url, headers=headers, allow_redirects=True) as resp:
            body = await resp.read()
            return resp.status, dict(resp.headers), body

    def parse(self, url: str, headers: dict[str, str], body: bytes) -> dict[str, Any]:
        mime = self._detect_mime(url, headers)
        if mime in {"text/html", "application/xhtml+xml"}:
            return self._parse_html(body, mime)
        if mime == "application/pdf":
            return self._parse_pdf(body, mime)
        if mime in {"application/json", "text/json"}:
            return self._parse_json(body, mime)
        if mime in {"text/csv", "application/csv"}:
            return self._parse_csv(body, mime)
        if mime in {"application/xml", "text/xml"}:
            return self._parse_xml(body, mime)
        return self._parse_text(body, mime)

    def _detect_mime(self, url: str, headers: dict[str, str]) -> str:
        content_type = headers.get("Content-Type", "").split(";")[0].strip().lower()
        if content_type:
            return content_type
        ext = url.lower().split("?")[0].rsplit(".", 1)[-1]
        return {
            "html": "text/html",
            "htm": "text/html",
            "pdf": "application/pdf",
            "json": "application/json",
            "csv": "text/csv",
            "xml": "application/xml",
            "txt": "text/plain",
        }.get(ext, "text/plain")

    def _parse_html(self, body: bytes, mime: str) -> dict[str, Any]:
        soup = BeautifulSoup(body, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "aside", "noscript"]):
            tag.decompose()
        for selector in ["[id*='cookie']", "[class*='cookie']", "[class*='banner']"]:
            for node in soup.select(selector):
                node.decompose()

        main = soup.find("main") or soup.find("article") or soup.find(attrs={"role": "main"}) or soup.body or soup
        title = (soup.title.string.strip() if soup.title and soup.title.string else None)
        markdown = self._html_to_markdown(main)

        return {
            "title": title,
            "doc_type": "html",
            "detected_mime": mime,
            "markdown": markdown,
            "parser_metadata": {"strategy": "beautifulsoup-main-extraction"},
        }

    def _html_to_markdown(self, root: BeautifulSoup) -> str:
        lines: list[str] = []
        for node in root.descendants:
            if getattr(node, "name", None) and re.fullmatch(r"h[1-6]", node.name):
                level = int(node.name[1])
                text = node.get_text(" ", strip=True)
                if text:
                    lines.append(f"{'#' * level} {text}")
            elif getattr(node, "name", None) == "li":
                text = node.get_text(" ", strip=True)
                if text:
                    lines.append(f"- {text}")
            elif getattr(node, "name", None) == "p":
                text = node.get_text(" ", strip=True)
                if text:
                    lines.append(text)
            elif getattr(node, "name", None) == "table":
                table_md = self._table_to_markdown(node)
                if table_md:
                    lines.append(table_md)
        return "\n\n".join(lines).strip()

    @staticmethod
    def _table_to_markdown(table: Any) -> str:
        rows = table.find_all("tr")
        parsed = []
        for row in rows:
            cols = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
            if cols:
                parsed.append(cols)
        if not parsed:
            return ""
        width = max(len(r) for r in parsed)
        normalized = [r + [""] * (width - len(r)) for r in parsed]
        header = normalized[0]
        sep = ["---"] * width
        body = normalized[1:] or [[""] * width]
        lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(sep) + " |"]
        lines.extend("| " + " | ".join(r) + " |" for r in body)
        return "\n".join(lines)

    def _parse_pdf(self, body: bytes, mime: str) -> dict[str, Any]:
        text = body.decode("latin-1", errors="ignore")
        text = re.sub(r"(?m)^\s*Page\s+\d+\s*$", "", text)
        text = re.sub(r"(?m)^\s*\d+\s*$", "", text)
        text = re.sub(r"\s{2,}", " ", text)
        return {
            "title": None,
            "doc_type": "pdf",
            "detected_mime": mime,
            "markdown": text.strip(),
            "parser_metadata": {"strategy": "stream-pdf-fallback", "note": "Install docling/markitdown for high fidelity"},
        }

    def _parse_json(self, body: bytes, mime: str) -> dict[str, Any]:
        payload = json.loads(body.decode("utf-8", errors="ignore"))
        markdown = self._json_to_markdown(payload)
        return {
            "title": None,
            "doc_type": "json",
            "detected_mime": mime,
            "markdown": markdown,
            "parser_metadata": {"strategy": "json-struct-to-markdown"},
        }

    def _json_to_markdown(self, payload: Any, level: int = 2) -> str:
        if isinstance(payload, dict):
            lines = []
            for key, value in payload.items():
                lines.append(f"{'#' * min(level, 6)} {key}")
                lines.append(self._json_to_markdown(value, level + 1))
            return "\n\n".join(lines)
        if isinstance(payload, list):
            if payload and all(isinstance(v, dict) for v in payload[:10]):
                keys = list({k for item in payload[:100] for k in item.keys()})
                rows = [keys] + [[str(item.get(k, ""))[:500] for k in keys] for item in payload[:100]]
                return "\n".join(
                    [
                        "| " + " | ".join(rows[0]) + " |",
                        "| " + " | ".join(["---"] * len(rows[0])) + " |",
                    ]
                    + ["| " + " | ".join(r) + " |" for r in rows[1:]]
                )
            return "\n".join(f"- {self._json_to_markdown(item, level + 1)}" for item in payload[:200])
        return str(payload)

    def _parse_csv(self, body: bytes, mime: str) -> dict[str, Any]:
        text = body.decode("utf-8", errors="ignore")
        rows = list(csv.reader(io.StringIO(text)))[:500]
        if not rows:
            markdown = ""
        else:
            width = len(rows[0])
            rows = [r + [""] * (width - len(r)) for r in rows]
            markdown = "\n".join(
                [
                    "| " + " | ".join(rows[0]) + " |",
                    "| " + " | ".join(["---"] * width) + " |",
                ]
                + ["| " + " | ".join(r) + " |" for r in rows[1:]]
            )
        return {
            "title": None,
            "doc_type": "csv",
            "detected_mime": mime,
            "markdown": markdown,
            "parser_metadata": {"strategy": "csv-table"},
        }

    def _parse_xml(self, body: bytes, mime: str) -> dict[str, Any]:
        soup = BeautifulSoup(body, "xml")
        markdown = []
        for tag in soup.find_all(recursive=False):
            markdown.append(f"## {tag.name}")
            markdown.append(tag.get_text(" ", strip=True))
        return {
            "title": None,
            "doc_type": "xml",
            "detected_mime": mime,
            "markdown": "\n\n".join(markdown).strip(),
            "parser_metadata": {"strategy": "xml-flattened"},
        }

    def _parse_text(self, body: bytes, mime: str) -> dict[str, Any]:
        text = body.decode("utf-8", errors="ignore")
        return {
            "title": None,
            "doc_type": "text",
            "detected_mime": mime,
            "markdown": text,
            "parser_metadata": {"strategy": "plain-text"},
        }


class Scheduler:
    def __init__(self, db: Database, batch_size: int = 20, worker_id: str = "worker-1", concurrency: int = 5):
        self.db = db
        self.batch_size = batch_size
        self.worker_id = worker_id
        self.concurrency = concurrency
        self.extractor = Extractor()
        self.normalizer = NormalizerHasher()
        self.chunker = SemanticChunker()
        self.rate_limiter = DomainRateLimiter(delay_seconds=2.0)

    async def run_forever(self) -> None:
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                await self.db.ensure_documents_seeded()
                claimed = await self.db.claim_documents(self.batch_size, self.worker_id)
                if not claimed:
                    await asyncio.sleep(15)
                    continue

                semaphore = asyncio.Semaphore(self.concurrency)
                tasks = [self._process_with_limit(semaphore, session, doc) for doc in claimed]
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_with_limit(self, semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, doc: ClaimedDocument) -> None:
        async with semaphore:
            await self.process_document(session, doc)

    async def process_document(self, session: aiohttp.ClientSession, doc: ClaimedDocument) -> None:
        domain = doc.url.split("/")[2] if "//" in doc.url else doc.url
        await self.rate_limiter.wait(domain)

        try:
            status, headers, body = await self.extractor.fetch(session, doc)
            if status == 304:
                await self.db.mark_not_modified(doc.document_id, headers.get("ETag"), parse_http_datetime(headers.get("Last-Modified")))
                return

            if status >= 400:
                await self._handle_http_failure(doc.document_id, status, headers)
                return

            parsed = self.extractor.parse(doc.url, headers, body)
            normalized = self.normalizer.normalize(parsed["markdown"])

            if self._looks_blocked_or_low_quality(normalized):
                await self.db.mark_failure(doc.document_id, status, "Extraction quality check failed (empty/block/captcha/too short)")
                return

            content_hash = self.normalizer.hash_content(normalized)
            latest = await self.db.get_latest_version(doc.document_id)

            etag = headers.get("ETag")
            last_modified = parse_http_datetime(headers.get("Last-Modified"))

            if latest and latest["content_hash"] == content_hash:
                await self.db.mark_unchanged(
                    doc.document_id,
                    parsed["title"],
                    parsed["doc_type"],
                    parsed["detected_mime"],
                    etag,
                    last_modified,
                )
                return

            chunks = self.chunker.chunk(normalized)
            await self.db.save_new_version(
                document_id=doc.document_id,
                markdown=normalized,
                content_hash=content_hash,
                parser_metadata=parsed["parser_metadata"],
                title=parsed["title"],
                doc_type=parsed["doc_type"],
                mime=parsed["detected_mime"],
                etag=etag,
                last_modified=last_modified,
                chunks=chunks,
            )
        except asyncio.TimeoutError:
            await self.db.mark_failure(doc.document_id, 408, "Timeout while scraping")
        except aiohttp.ClientError as exc:
            await self.db.mark_failure(doc.document_id, 503, f"HTTP client error: {exc}")
        except Exception as exc:  # broad safety at task boundary
            LOGGER.exception("Unexpected processing error for %s", doc.url)
            await self.db.mark_failure(doc.document_id, None, f"Unexpected processing error: {exc}")

    async def _handle_http_failure(self, document_id: int, status: int, headers: dict[str, str]) -> None:
        if status in SOFT_ERROR_CODES:
            retry_after = parse_retry_after(headers.get("Retry-After"))
            if retry_after:
                await asyncio.sleep(retry_after)
            await self.db.mark_failure(document_id, status, f"Soft HTTP failure ({status})")
        elif status in HARD_ERROR_CODES:
            await self.db.mark_failure(document_id, status, f"Hard HTTP failure ({status})")
        else:
            await self.db.mark_failure(document_id, status, f"Unhandled HTTP failure ({status})")

    @staticmethod
    def _looks_blocked_or_low_quality(markdown: str) -> bool:
        text = markdown.lower()
        word_count = len(re.findall(r"\w+", text))
        blocked = any(token in text for token in ["access denied", "captcha", "cloudflare"])
        return not markdown.strip() or blocked or word_count < 200


def parse_http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


def parse_retry_after(value: str | None) -> int | None:
    if not value:
        return None
    if value.isdigit():
        return min(int(value), 300)
    dt = parse_http_datetime(value)
    if not dt:
        return None
    delta = int((dt - datetime.now(timezone.utc)).total_seconds())
    return max(0, min(delta, 300))


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")

    worker_id = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
    batch_size = int(os.getenv("BATCH_SIZE", "20"))
    concurrency = int(os.getenv("CONCURRENCY", "5"))

    db = Database(dsn)
    await db.connect()
    try:
        scheduler = Scheduler(db=db, batch_size=batch_size, worker_id=worker_id, concurrency=concurrency)
        await scheduler.run_forever()
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
