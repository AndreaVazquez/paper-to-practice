"""
Component 1b — PDF Downloader
Command: python manage.py download_pdfs [--source all|seed_doi|vis2020|vis2021|vis2022|vis2023|vis2024|vis2025]
                                         [--limit N] [--skip-existing]

Downloads PDFs for papers that have a pdf_url but no pdf_local_path yet.
Updates paper.pdf_local_path on success.

Download strategy (tried in order):
  1. Direct pdf_url as-is (works for arXiv, direct links from program pages)
  2. Semantic Scholar open access PDF field
  3. arXiv search by title (last resort)

Papers from VisImages are skipped — they have no PDF to download
(their figures are already extracted and stored in the VisImages dataset).

Idempotent: skips papers that already have pdf_local_path set unless
--reset is passed.
"""

import logging
import re
import time
from pathlib import Path
from typing import Optional

import httpx
from decouple import config
from django.conf import settings
from django.core.management.base import BaseCommand

from academic.models import Paper
from core.agent_log import emit

logger = logging.getLogger(__name__)

PDFS_DIR = Path(settings.MEDIA_ROOT) / "papers" / "pdfs"
CONTACT_EMAIL = config("CONTACT_EMAIL", default="researcher@example.com")

# Semantic Scholar
S2_URL = "https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=openAccessPdf,title"
S2_TITLE_URL = "https://api.semanticscholar.org/graph/v1/paper/search?query={query}&fields=openAccessPdf,title&limit=1"

# arXiv search
ARXIV_SEARCH_URL = "https://export.arxiv.org/api/query?search_query=ti:{title}&max_results=1"
ARXIV_PDF_URL = "https://arxiv.org/pdf/{arxiv_id}"

REQUEST_TIMEOUT = 15        # most legitimate servers respond in <5s
DELAY_BETWEEN_DOWNLOADS = 0.5  # inter-paper delay; arXiv gap enforced separately
ARXIV_MIN_GAP = 4.0           # minimum seconds between arXiv API calls
ARXIV_RETRY_DELAY = 65        # seconds to wait after a 429 before retrying

# Module-level timestamp so the gap is enforced across all papers in the run
_last_arxiv_call: float = 0.0


class Command(BaseCommand):
    help = "Download PDFs for papers that have a pdf_url but no local PDF yet."

    def add_arguments(self, parser):
        parser.add_argument(
            "--source",
            choices=["all", "seed_doi", "vis2019", "vis2020", "vis2021", "vis2022", "vis2023", "vis2024", "vis2025"],
            default="all",
            help="Which source group to download (default: all, skips visimages)",
        )
        parser.add_argument(
            "--limit", type=int, default=None,
            help="Stop after attempting N papers",
        )
        parser.add_argument(
            "--reset", action="store_true",
            help="Re-download even papers that already have pdf_local_path",
        )

    def handle(self, *args, **options):
        PDFS_DIR.mkdir(parents=True, exist_ok=True)

        source = options["source"]

        # visimages papers have no PDFs to download — always exclude
        qs = Paper.objects.exclude(source="visimages")

        if source != "all":
            qs = qs.filter(source=source)

        if not options["reset"]:
            qs = qs.filter(pdf_local_path="")

        papers = list(qs.order_by("id"))
        if options["limit"]:
            papers = papers[: options["limit"]]

        total = len(papers)
        emit(agent="download_pdfs", status="started",
             message=f"Attempting to download PDFs for {total} papers")

        downloaded = 0
        failed = 0

        for i, paper in enumerate(papers):
            success = self._download_paper(paper)
            if success:
                downloaded += 1
            else:
                failed += 1

            emit(agent="download_pdfs", status="running",
                 message=(
                     f"{'✓' if success else '✗'} [{i+1}/{total}] "
                     f"{paper.title[:60]}"
                 ),
                 record_id=paper.id,
                 progress=[i + 1, total])

            time.sleep(DELAY_BETWEEN_DOWNLOADS)

        emit(agent="download_pdfs", status="done",
             message=f"Done. {downloaded} downloaded, {failed} failed out of {total}.")
        self.stdout.write(
            self.style.SUCCESS(
                f"Downloaded {downloaded}/{total} PDFs. {failed} failed."
            )
        )

    # ── Per-paper download logic ───────────────────────────────────────────────

    def _download_paper(self, paper: Paper) -> bool:
        """Try each strategy in turn. Returns True if a PDF was saved."""

        # Strategy 1: direct pdf_url.
        # Only attempt if the URL looks like an actual PDF or known open-access host.
        # Skip doi.org / ieeexplore.ieee.org — they always return HTML landing pages
        # and waste a full timeout per paper.
        if paper.pdf_url:
            url = paper.pdf_url
            arxiv_id = _extract_arxiv_id(url)
            if arxiv_id:
                # Rewrite arXiv abstract/landing URL directly to PDF
                url = ARXIV_PDF_URL.format(arxiv_id=arxiv_id)
                path = self._try_download(url, paper)
                if path:
                    return self._save(paper, path)
            elif not any(skip in url for skip in
                         ("doi.org", "ieeexplore.ieee", "ieee.org")):
                # Attempt other direct URLs that aren't known HTML-only hosts
                path = self._try_download(url, paper)
                if path:
                    return self._save(paper, path)

        # Strategy 2: Semantic Scholar by DOI
        if paper.doi:
            url = self._semantic_scholar_pdf_url(paper.doi)
            if url:
                path = self._try_download(url, paper)
                if path:
                    return self._save(paper, path)

        # Strategy 2b: Semantic Scholar title search (no DOI required)
        # Covers papers from vis2020–vis2025 that have no DOI in the program JSON.
        if paper.title and not paper.title.startswith("["):
            url = self._semantic_scholar_pdf_url_by_title(paper.title)
            if url:
                path = self._try_download(url, paper)
                if path:
                    return self._save(paper, path)

        # Strategy 3: arXiv search by title (last resort)
        if paper.title and not paper.title.startswith("["):
            url = self._arxiv_pdf_url_by_title(paper.title)
            if url:
                path = self._try_download(url, paper)
                if path:
                    return self._save(paper, path)

        emit(agent="download_pdfs", status="skipped",
             message=f"No PDF found for: {paper.title[:60]}",
             record_id=paper.id, level="warning")
        return False

    def _try_download(self, url: str, paper: Paper) -> Optional[Path]:
        """
        Attempt to download a PDF from url.
        Returns the local Path if successful and content looks like a PDF, else None.

        Uses explicit httpx.Timeout to enforce per-chunk read deadline — prevents
        stalled servers (e.g. institutional repos) from hanging the process forever.
        A wall-clock deadline enforces the total download budget regardless.
        """
        if not url or not url.startswith("http"):
            return None

        filename = _safe_filename(paper)
        out_path = PDFS_DIR / filename

        # Explicit timeout object:
        #   connect=10s  — TCP + TLS handshake
        #   read=15s     — max wait between *consecutive chunks* (catches stalled servers)
        #   write=10s    — sending the request
        #   pool=5s      — acquiring a connection from the pool
        timeout = httpx.Timeout(connect=10, read=15, write=10, pool=5)

        # Hard wall-clock deadline for the entire download
        MAX_DOWNLOAD_SECS = 60
        MAX_BYTES = 50 * 1024 * 1024  # 50 MB — skip suspiciously large files

        deadline = time.monotonic() + MAX_DOWNLOAD_SECS

        try:
            with httpx.Client(
                follow_redirects=True,
                timeout=timeout,
                headers={"User-Agent": f"Mozilla/5.0 (research bot; contact: {CONTACT_EMAIL})"},
            ) as client:
                with client.stream("GET", url) as response:
                    if response.status_code != 200:
                        return None

                    content_type = response.headers.get("content-type", "")
                    # Reject obvious non-PDFs early
                    if "html" in content_type and "pdf" not in content_type:
                        return None

                    chunks = []
                    total_bytes = 0
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if time.monotonic() > deadline:
                            logger.debug("Download wall-clock deadline exceeded for %s", url)
                            return None
                        total_bytes += len(chunk)
                        if total_bytes > MAX_BYTES:
                            logger.debug("Download size cap exceeded for %s", url)
                            return None
                        chunks.append(chunk)
                    data = b"".join(chunks)

            # Verify it's actually a PDF
            if not data.startswith(b"%PDF"):
                return None

            out_path.write_bytes(data)
            logger.info("Downloaded %s → %s (%d KB)", url, filename, len(data) // 1024)
            return out_path

        except httpx.TimeoutException as exc:
            logger.debug("Timeout downloading %s: %s", url, exc)
            return None
        except Exception as exc:
            logger.debug("Download failed for %s: %s", url, exc)
            return None

    def _save(self, paper: Paper, path: Path) -> bool:
        # Store relative to MEDIA_ROOT so paths survive directory moves.
        paper.pdf_local_path = str(path.relative_to(settings.MEDIA_ROOT))
        paper.save(update_fields=["pdf_local_path"])
        return True

    # ── URL resolution strategies ──────────────────────────────────────────────

    def _semantic_scholar_pdf_url(self, doi: str) -> str | None:
        """Query Semantic Scholar for an open-access PDF URL."""
        try:
            url = S2_URL.format(doi=doi)
            with httpx.Client(follow_redirects=True, timeout=10) as client:
                resp = client.get(url)
            if resp.status_code != 200:
                return None
            data = resp.json()
            oa = data.get("openAccessPdf")
            if oa and isinstance(oa, dict):
                return oa.get("url")
            return None
        except Exception as exc:
            logger.debug("Semantic Scholar failed for %s: %s", doi, exc)
            return None

    def _semantic_scholar_pdf_url_by_title(self, title: str) -> str | None:
        """
        Search Semantic Scholar by title — works without a DOI.
        Validates the top result's title before returning the PDF URL.
        Rate limit: 100 req/5min unauthenticated — well within our per-paper cadence.
        """
        try:
            # Use first 10 words of title as search query
            query = "+".join(title.split()[:10])
            url = S2_TITLE_URL.format(query=query)
            with httpx.Client(follow_redirects=True, timeout=10) as client:
                resp = client.get(url)
            if resp.status_code != 200:
                return None
            data = resp.json()
            results = data.get("data") or []
            if not results:
                return None

            top = results[0]

            # Validate title overlap — require 2 significant words (5+ chars) in common
            result_title = (top.get("title") or "").lower()
            query_words = {w.lower() for w in re.findall(r"\b\w{5,}\b", title)}
            result_words = {w.lower() for w in re.findall(r"\b\w{5,}\b", result_title)}
            if len(query_words & result_words) < 2:
                return None

            oa = top.get("openAccessPdf")
            if oa and isinstance(oa, dict):
                return oa.get("url")
            return None
        except Exception as exc:
            logger.debug("S2 title search failed for '%s': %s", title[:60], exc)
            return None

    def _arxiv_pdf_url_by_title(self, title: str) -> str | None:
        """
        Search arXiv for a paper by title. Returns PDF URL if found.
        This is a best-effort fallback — title matching is fuzzy.

        arXiv rate limit: ~1 request per 3 seconds. Enforced by tracking the last
        call time and sleeping the exact deficit before each request.
        On 429, waits ARXIV_RETRY_DELAY seconds and retries once.
        """
        global _last_arxiv_call
        # Enforce minimum gap between arXiv API calls regardless of how long
        # Fast Semantic Scholar responses can compress arXiv calls closer together
        # than the paper-level delay allows.
        elapsed = time.monotonic() - _last_arxiv_call
        if elapsed < ARXIV_MIN_GAP:
            time.sleep(ARXIV_MIN_GAP - elapsed)

        clean_title = re.sub(r"[^\w\s]", " ", title)
        search_terms = "+".join(clean_title.split()[:8])
        url = ARXIV_SEARCH_URL.format(title=search_terms)

        for attempt in range(2):  # try once, retry once after backoff
            try:
                _last_arxiv_call = time.monotonic()
                with httpx.Client(follow_redirects=True, timeout=10) as client:
                    resp = client.get(url)

                if resp.status_code == 429:
                    if attempt == 0:
                        emit(agent="download_pdfs", status="running",
                             message=f"arXiv rate limit hit — waiting {ARXIV_RETRY_DELAY}s before retry",
                             level="warning")
                        time.sleep(ARXIV_RETRY_DELAY)
                        continue  # retry
                    return None  # second 429 — give up

                if resp.status_code != 200:
                    return None

                # Parse Atom feed — extract first entry's id and title
                id_match = re.search(r"<id>https?://arxiv\.org/abs/([\d.v]+)", resp.text)
                if not id_match:
                    return None

                # Validate the matched paper's title overlaps with our query.
                # arXiv title search is fuzzy and can return unrelated papers —
                # a 2014 paper matched for "Representing Charts as Text for Language Models"
                # is a false positive we'd otherwise chase with a 404 PDF request.
                title_match = re.search(r"<title>(.*?)</title>", resp.text, re.DOTALL)
                if title_match:
                    result_title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip().lower()
                    # Require at least 2 significant words (>4 chars) to overlap
                    query_words = {w.lower() for w in re.findall(r"\b\w{5,}\b", title)}
                    result_words = {w.lower() for w in re.findall(r"\b\w{5,}\b", result_title)}
                    overlap = query_words & result_words
                    if len(overlap) < 2:
                        logger.debug(
                            "arXiv title mismatch for '%s': got '%s' (overlap: %s)",
                            title[:60], result_title[:60], overlap
                        )
                        return None

                arxiv_id = id_match.group(1)
                return ARXIV_PDF_URL.format(arxiv_id=arxiv_id)

            except Exception as exc:
                logger.debug("arXiv search failed for '%s': %s", title, exc)
                return None

        return None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_arxiv_id(url: str) -> str | None:
    """Extract arXiv ID from an arXiv URL in any form."""
    match = re.search(r"arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,5})", url)
    return match.group(1) if match else None


def _safe_filename(paper: Paper) -> str:
    """Generate a safe local filename for a paper's PDF."""
    if paper.doi:
        safe = re.sub(r"[^\w\-]", "_", paper.doi)
        return f"{safe}.pdf"
    safe_title = re.sub(r"[^\w\s\-]", "", paper.title[:50]).strip().replace(" ", "_")
    return f"paper_{paper.id}_{safe_title}.pdf"
