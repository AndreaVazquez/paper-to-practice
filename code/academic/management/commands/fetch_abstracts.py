"""
Component 1.5 — Abstract + DOI Fetcher
Command: python manage.py fetch_abstracts [--source SOURCE] [--limit N] [--dry-run]
                                          [--fetch-dois]

Default mode — fetch missing abstracts:
  Fetches missing abstracts for papers that have a DOI but no abstract stored.
  Primarily targets VisImages and seed_doi papers which were ingested without
  abstracts, but works on any paper source.

  Fallback chain (tried in order per paper):
    1. OpenAlex        — best coverage for all IEEE VIS / INFVIS / VAST / TVCG;
                         stores abstracts as inverted index; free, no key needed.
                         Sample: 20/20 random VisImages DOIs resolved (1996–2018).
    2. Semantic Scholar — good for recent papers; patchy for older IEEE conference
                          papers but catches some OpenAlex misses.
    3. CrossRef        — IEEE rarely deposits abstracts here, but catches non-IEEE
                          papers (ACM, Wiley/EuroVis, Springer, etc.)
    4. Europe PMC      — aggregates from multiple sources; partial overlap with
                          OpenAlex but occasionally unique hits for TVCG papers.

  After running, re-run enrich_metadata to extract keywords from the newly
  populated abstracts, then re-run annotate_drift to get meaningful drift scores.

--fetch-dois mode:
  Searches for DOIs by paper title for papers where doi IS NULL.
  Uses OpenAlex title search first (strips the https://doi.org/ prefix it
  returns), then CrossRef as fallback (returns the bare DOI directly).
  Title similarity is verified via Jaccard overlap (threshold 0.80) to avoid
  false-positive matches. Writes back paper.doi when found.
  After running, re-run fetch_abstracts (without --fetch-dois) to populate
  abstracts for the newly resolved papers.

Both modes respect --source, --limit, and --dry-run.
Idempotent: skips papers that already have the target field populated.
Rate limits: ~1 req/sec (well within all free tier limits, no keys required).
"""

import logging
import re
import time
from collections import defaultdict

import httpx
from django.core.management.base import BaseCommand

from academic.models import Paper
from core.agent_log import emit

logger = logging.getLogger(__name__)

_SLEEP_BETWEEN_PAPERS = 1.1   # seconds between papers
_SLEEP_BETWEEN_SOURCES = 0.4  # seconds between fallback attempts
_TIMEOUT = 12                  # HTTP timeout per request

_UA = "paper-to-practice/1.0 (mailto:hiroshikiri1129@gmail.com)"


class Command(BaseCommand):
    help = (
        "Fetch missing abstracts via OpenAlex → Semantic Scholar → "
        "CrossRef → Europe PMC fallback chain."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--source",
            choices=["visimages", "vis2019", "vis2020", "vis2021", "vis2022", "vis2023", "vis2024", "vis2025", "seed_doi", "all"],
            default="all",
            help="Which paper source to target (default: all)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Process at most N papers — useful for testing",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Show what would be fetched without writing to DB",
        )
        parser.add_argument(
            "--fetch-dois",
            action="store_true",
            default=False,
            help=(
                "Instead of fetching abstracts, search for missing DOIs by title "
                "(OpenAlex then CrossRef). Targets papers where doi IS NULL. "
                "After running, re-run without this flag to fetch their abstracts."
            ),
        )

    def handle(self, *args, **options):
        source     = options["source"]
        dry_run    = options["dry_run"]
        fetch_dois = options["fetch_dois"]

        if fetch_dois:
            self._handle_fetch_dois(source, dry_run, options["limit"])
        else:
            self._handle_fetch_abstracts(source, dry_run, options["limit"])

    # ── Mode A: fetch missing abstracts (original behaviour) ──────────────────

    def _handle_fetch_abstracts(self, source, dry_run, limit):
        qs = (
            Paper.objects
            .filter(abstract="")
            .exclude(doi__isnull=True)
            .exclude(doi="")
        )
        if source != "all":
            qs = qs.filter(source=source)

        papers = list(qs.order_by("year", "id"))
        if limit:
            papers = papers[:limit]

        total = len(papers)
        emit(
            agent="fetch_abstracts",
            status="started",
            message=(
                f"Fetching abstracts for {total} papers "
                f"(source={source}, dry_run={dry_run})"
            ),
        )

        results: dict[str, int] = defaultdict(int)

        with httpx.Client(
            timeout=_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": _UA},
        ) as client:
            for i, paper in enumerate(papers):
                abstract, source_used = _fetch_abstract(client, paper.doi)

                if abstract and not dry_run:
                    paper.abstract = abstract
                    paper.save(update_fields=["abstract"])

                results[source_used or "not_found"] += 1

                if abstract:
                    emit(
                        agent="fetch_abstracts",
                        status="running",
                        message=(
                            f"{'DRY RUN ' if dry_run else ''}[{source_used}] "
                            f"{paper.doi} — {len(abstract)} chars — "
                            f"{paper.title[:50]}"
                        ),
                        record_id=paper.id,
                        progress=[i + 1, total],
                    )
                else:
                    emit(
                        agent="fetch_abstracts",
                        status="running",
                        message=f"No abstract found: {paper.doi} — {paper.title[:50]}",
                        record_id=paper.id,
                        progress=[i + 1, total],
                        level="warning",
                    )

                time.sleep(_SLEEP_BETWEEN_PAPERS)

        found_total = sum(v for k, v in results.items() if k != "not_found")
        summary = ", ".join(f"{k}={v}" for k, v in sorted(results.items()))

        emit(
            agent="fetch_abstracts",
            status="done",
            message=(
                f"{'DRY RUN — ' if dry_run else ''}"
                f"{found_total}/{total} abstracts fetched. {summary}"
            ),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'DRY RUN — ' if dry_run else ''}"
                f"Fetched {found_total}/{total} abstracts.\n"
                f"Breakdown: {summary}\n"
            )
            + (
                "" if dry_run else
                "\nNext steps:\n"
                "  1. python manage.py enrich_metadata  — re-extract keywords\n"
                "  2. python manage.py annotate_drift   — re-annotate with real abstracts\n"
            )
        )

    # ── Mode B: fetch missing DOIs by title search ─────────────────────────────

    def _handle_fetch_dois(self, source, dry_run, limit):
        qs = Paper.objects.filter(doi__isnull=True)
        if source != "all":
            qs = qs.filter(source=source)

        papers = list(qs.order_by("year", "id"))
        if limit:
            papers = papers[:limit]

        total = len(papers)
        emit(
            agent="fetch_abstracts",
            status="started",
            message=(
                f"Fetching DOIs by title for {total} papers "
                f"(source={source}, dry_run={dry_run})"
            ),
        )

        results: dict[str, int] = defaultdict(int)

        with httpx.Client(
            timeout=_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": _UA},
        ) as client:
            for i, paper in enumerate(papers):
                doi, source_used = _fetch_doi(client, paper.title, paper.year)

                if doi and not dry_run:
                    paper.doi = doi
                    paper.save(update_fields=["doi"])

                results[source_used or "not_found"] += 1

                if doi:
                    emit(
                        agent="fetch_abstracts",
                        status="running",
                        message=(
                            f"{'DRY RUN ' if dry_run else ''}[{source_used}] "
                            f"{doi} — {paper.title[:60]}"
                        ),
                        record_id=paper.id,
                        progress=[i + 1, total],
                    )
                else:
                    emit(
                        agent="fetch_abstracts",
                        status="running",
                        message=f"No DOI found: {paper.title[:70]}",
                        record_id=paper.id,
                        progress=[i + 1, total],
                        level="warning",
                    )

                time.sleep(_SLEEP_BETWEEN_PAPERS)

        found_total = sum(v for k, v in results.items() if k != "not_found")
        summary = ", ".join(f"{k}={v}" for k, v in sorted(results.items()))

        emit(
            agent="fetch_abstracts",
            status="done",
            message=(
                f"{'DRY RUN — ' if dry_run else ''}"
                f"{found_total}/{total} DOIs resolved. {summary}"
            ),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'DRY RUN — ' if dry_run else ''}"
                f"Resolved {found_total}/{total} DOIs.\n"
                f"Breakdown: {summary}\n"
            )
            + (
                "" if dry_run else
                "\nNext step:\n"
                "  python manage.py fetch_abstracts  — fetch abstracts for the newly resolved DOIs\n"
            )
        )


# ── Fallback chain — abstract fetching ────────────────────────────────────────

_SOURCES = None  # populated lazily below after function defs

def _fetch_abstract(client: httpx.Client, doi: str) -> tuple[str, str]:
    """
    Try each source in order.
    Returns (abstract_text, source_name) or ('', '').
    """
    for fn in (_try_openalex, _try_semantic_scholar, _try_crossref, _try_europe_pmc):
        try:
            abstract = fn(client, doi)
            if abstract:
                return abstract, fn.__name__.replace("_try_", "")
        except httpx.TimeoutException:
            logger.warning("%s timed out for DOI %s", fn.__name__, doi)
        except Exception as exc:
            logger.debug("%s failed for %s: %s", fn.__name__, doi, exc)
        time.sleep(_SLEEP_BETWEEN_SOURCES)

    return "", ""


# ── Fallback chain — DOI fetching by title ─────────────────────────────────────

def _titles_match(a: str, b: str, threshold: float = 0.80) -> bool:
    """
    Normalised Jaccard similarity on word tokens.
    Strips punctuation and lowercases before comparing.
    Returns True when overlap >= threshold (0.80 by default).
    """
    import re as _re
    def _tok(s: str) -> set[str]:
        return set(_re.sub(r"[^\w\s]", "", s.lower()).split())
    wa, wb = _tok(a), _tok(b)
    if not wa or not wb:
        return False
    return len(wa & wb) / len(wa | wb) >= threshold


def _fetch_doi_openalex(client: httpx.Client, title: str, year: int | None) -> str:
    """
    OpenAlex title search → bare DOI string.
    OpenAlex returns DOIs as full URIs ("https://doi.org/10.xxxx/..."); we strip
    the prefix so the stored value matches the rest of the corpus.
    Returns '' if no confident match found.
    """
    params: dict = {
        "search": title,
        "per-page": 1,
        "select": "doi,title,publication_year",
    }
    if year:
        params["filter"] = f"publication_year:{year}"

    r = client.get(
        "https://api.openalex.org/works",
        params=params,
        headers={"User-Agent": _UA},
    )
    if r.status_code != 200:
        return ""
    results = r.json().get("results", [])
    if not results:
        return ""

    hit = results[0]
    hit_title = hit.get("title", "") or ""
    if not _titles_match(title, hit_title):
        return ""

    raw_doi = hit.get("doi", "") or ""
    # Strip "https://doi.org/" or "http://doi.org/"
    for prefix in ("https://doi.org/", "http://doi.org/"):
        if raw_doi.startswith(prefix):
            return raw_doi[len(prefix):]
    return raw_doi  # already bare, or empty


def _fetch_doi_crossref(client: httpx.Client, title: str, year: int | None) -> str:
    """
    CrossRef title search → bare DOI string.
    CrossRef returns the DOI directly (no URI prefix needed).
    Returns '' if no confident match found.
    """
    params: dict = {
        "query.title": title,
        "rows": 1,
        "select": "DOI,title,published",
    }
    if year:
        params["filter"] = f"from-pub-date:{year},until-pub-date:{year}"

    r = client.get(
        "https://api.crossref.org/works",
        params=params,
        headers={"User-Agent": _UA},
    )
    if r.status_code != 200:
        return ""
    items = r.json().get("message", {}).get("items", [])
    if not items:
        return ""

    hit = items[0]
    # CrossRef title field is a list
    hit_titles = hit.get("title", []) or []
    hit_title = hit_titles[0] if hit_titles else ""
    if not _titles_match(title, hit_title):
        return ""

    return hit.get("DOI", "") or ""


def _fetch_doi(client: httpx.Client, title: str, year: int | None) -> tuple[str, str]:
    """
    Try OpenAlex then CrossRef to find a DOI by paper title.
    Returns (doi, source_name) or ('', '').
    """
    for fn, name in [
        (_fetch_doi_openalex, "openalex"),
        (_fetch_doi_crossref, "crossref"),
    ]:
        try:
            doi = fn(client, title, year)
            if doi:
                return doi, name
        except httpx.TimeoutException:
            logger.warning("%s (doi) timed out for title: %s", name, title[:60])
        except Exception as exc:
            logger.debug("%s (doi) failed for title %s: %s", name, title[:60], exc)
        time.sleep(_SLEEP_BETWEEN_SOURCES)

    return "", ""


def _try_openalex(client: httpx.Client, doi: str) -> str:
    """
    OpenAlex — free, no key, covers IEEE VIS back to 1996.
    Abstracts stored as inverted index; reconstructed to plain text.
    Verified 100% hit rate on random VisImages sample (1996–2018).
    """
    r = client.get(
        f"https://api.openalex.org/works/https://doi.org/{doi}",
        headers={"User-Agent": _UA},
    )
    if r.status_code != 200:
        return ""
    idx = r.json().get("abstract_inverted_index")
    return _reconstruct_inverted_index(idx) if idx else ""


def _try_semantic_scholar(client: httpx.Client, doi: str) -> str:
    """
    Semantic Scholar — good for recent papers and arXiv preprints.
    Free, no key for basic fields.
    """
    r = client.get(
        f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=abstract"
    )
    if r.status_code != 200:
        return ""
    return r.json().get("abstract") or ""


def _try_crossref(client: httpx.Client, doi: str) -> str:
    """
    CrossRef — IEEE doesn't deposit abstracts here, but ACM, Wiley,
    Springer, and Elsevier often do. Abstracts may contain JATS XML tags.
    """
    r = client.get(
        f"https://api.crossref.org/works/{doi}",
        headers={"User-Agent": _UA},
    )
    if r.status_code != 200:
        return ""
    raw = r.json().get("message", {}).get("abstract") or ""
    return _strip_jats(raw)


def _try_europe_pmc(client: httpx.Client, doi: str) -> str:
    """
    Europe PMC — aggregates from multiple publishers.
    Partial overlap with OpenAlex but occasionally catches TVCG papers
    that OpenAlex misses. Uses exact DOI field match to avoid false positives.
    """
    r = client.get(
        "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
        params={"query": f'DOI:"{doi}"', "format": "json", "resultType": "core"},
        headers={"User-Agent": _UA},
    )
    if r.status_code != 200:
        return ""
    results = r.json().get("resultList", {}).get("result", [])
    if not results:
        return ""
    # Verify the returned DOI actually matches (Europe PMC can return near-misses)
    result_doi = (results[0].get("doi") or "").lower().strip()
    if result_doi != doi.lower().strip():
        return ""
    return results[0].get("abstractText") or ""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _reconstruct_inverted_index(inverted_index: dict) -> str:
    """
    OpenAlex stores abstracts as {word: [position, ...]} inverted index.
    Reconstruct to plain text by sorting positions.
    """
    words: dict[int, str] = {}
    for word, positions in inverted_index.items():
        for pos in positions:
            words[pos] = word
    return " ".join(words[i] for i in sorted(words))


def _strip_jats(text: str) -> str:
    """Remove JATS XML tags that CrossRef sometimes wraps abstracts in."""
    return re.sub(r"<[^>]+>", "", text).strip()