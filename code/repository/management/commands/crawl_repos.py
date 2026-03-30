"""
Component 5 — Repository Crawl (Kaggle + GitHub)
Command: python manage.py crawl_repos
           [--platform kaggle|github|all]
           [--limit-per-term N]
           [--terms bar scatter ...]
           [--keyword-source taxonomy|db|both]

Kaggle (v2.0.0):
  - Searches by term without language filter (post-filters by library imports)
  - Falls back to direct HTTP API if SDK returns 0 (common with long-tail terms)
  - No language/output-type filters (post-filters by library imports instead)
  - Downloads notebook JSON to media/repos/notebooks/kaggle/

GitHub:
  - Searches Jupyter notebooks via code search API
  - Filters by visualization library imports
  - Requires GITHUB_TOKEN in .env for 5000 req/hr (60/hr unauthenticated)
  - Downloads to media/repos/notebooks/github/

Keyword strategy:
  - taxonomy: fixed terms from vis type names (most specific)
  - db:       top N keywords extracted from ingested papers (most relevant)
  - both:     union of above (default)

Idempotent: skips notebooks already in DB (by source_id).
"""

import base64
import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import httpx
from decouple import config
from django.conf import settings
from django.core.management.base import BaseCommand

from core.agent_log import emit
from core.prompts.detect_chart_types import LIBRARY_IMPORT_PATTERNS
from repository.models import RepoArtifact, RepoSource

logger = logging.getLogger(__name__)

# ── Directories ────────────────────────────────────────────────────────────────
KAGGLE_DIR      = Path(settings.MEDIA_ROOT) / "repos" / "notebooks" / "kaggle"
GITHUB_DIR      = Path(settings.MEDIA_ROOT) / "repos" / "notebooks" / "github"
OBSERVABLE_DIR  = Path(settings.MEDIA_ROOT) / "repos" / "notebooks" / "observablehq"

# ── Kaggle ─────────────────────────────────────────────────────────────────────
KAGGLE_API_BASE  = "https://www.kaggle.com/api/v1"
RESULTS_PER_TERM = 20
MIN_UPDATED_YEAR = 2020

# ── GitHub ─────────────────────────────────────────────────────────────────────
GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN    = config("GITHUB_TOKEN", default="")

# ── Observable HQ ──────────────────────────────────────────────────────────────
OBSERVABLE_API_BASE = "https://api.observablehq.com"
OBSERVABLE_ACCOUNT  = "@d3"           # the account to crawl
OBSERVABLE_UA       = "paper-to-practice/1.0 (research crawler)"
_OBSERVABLE_PAGE_SLEEP  = 0.4        # seconds between listing pages
_OBSERVABLE_FETCH_SLEEP = 0.5        # seconds between JS downloads

# ── Taxonomy search terms ──────────────────────────────────────────────────────
# Short, commonly-used titles/tags people actually use on Kaggle/GitHub.
# Kaggle search is tag+title-based, NOT full-text. Short common aliases work.
TAXONOMY_SEARCH_TERMS: list[str] = [
    "bar chart", "histogram",
    "line chart", "area chart", "streamgraph",
    "scatter plot", "bubble chart",
    "choropleth map", "geomap", "folium map",
    "network graph", "force directed graph",
    "treemap", "sunburst chart", "dendrogram",
    "heatmap", "correlation heatmap",
    "parallel coordinates", "parallel plot",
    "sankey diagram", "alluvial diagram", "chord diagram",
    "radar chart", "spider chart",
    "plotly visualization", "seaborn visualization",
    "matplotlib visualization", "bokeh visualization",
    "altair chart",
]


def _get_db_keywords(top_n: int = 25) -> list[str]:
    """
    Extract chart/technique keyword phrases from ingested papers.
    Only keeps phrases that are likely to appear as Kaggle notebook titles/tags
    (i.e. concrete technique names, not academic methodology terms).
    """
    try:
        from academic.models import Paper
        import collections, json as _json

        # Academic-only phrases that will never match on Kaggle
        SKIP_FRAGMENTS = {
            "user study", "interview", "evaluation", "crowdsource", "qualitative",
            "quantitative", "task design", "design space", "case study", "perception",
            "cognitive", "scalability", "rendering", "augmented reality", "virtual reality",
            "mixed reality", "requirements", "framework", "pipeline", "taxonomy",
            "ontology", "survey", "guidelines", "provenance", "uncertainty",
            "explainability", "fairness", "bias", "ethics", "annotation",
        }
        # Must contain at least one of these to be chart-like
        CHART_SIGNALS = {
            "chart", "plot", "graph", "map", "diagram", "matrix", "heatmap",
            "treemap", "sankey", "chord", "alluvial", "sunburst", "network",
            "scatter", "histogram", "bar", "line", "area", "parallel",
            "glyph", "flow", "stream", "radar", "spider", "bubble",
            "visualization", "visual", "layout",
        }

        counter: collections.Counter = collections.Counter()
        for paper in Paper.objects.exclude(keywords_extracted__in=["[]", "", None])[:3000]:
            try:
                for kw in _json.loads(paper.keywords_extracted):
                    kw_clean = kw.strip().lower()
                    words = kw_clean.split()
                    if not (2 <= len(words) <= 4):
                        continue
                    # Skip if contains any academic-only fragment
                    if any(skip in kw_clean for skip in SKIP_FRAGMENTS):
                        continue
                    # Keep only if it contains a chart signal word
                    if any(signal in kw_clean for signal in CHART_SIGNALS):
                        counter[kw_clean] += 1
            except Exception:
                continue

        return [kw for kw, _ in counter.most_common(top_n)]
    except Exception as exc:
        logger.warning("_get_db_keywords failed: %s", exc)
        return []


def _uses_vis_library(content: str) -> bool:
    """Return True if content imports at least one known visualization library."""
    content_lower = content.lower()
    for _lib, pattern in LIBRARY_IMPORT_PATTERNS:
        if pattern.lower() in content_lower:
            return True
    return False


def _parse_date(value) -> Optional[date]:
    if not value:
        return None
    s = str(value)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


# ══════════════════════════════════════════════════════════════════════════════

class Command(BaseCommand):
    help = "Crawl Kaggle, GitHub, and/or Observable HQ for visualization notebooks."

    def add_arguments(self, parser):
        parser.add_argument("--platform",
                            choices=["kaggle", "github", "observablehq", "all"],
                            default="all")
        parser.add_argument("--limit-per-term", type=int, default=RESULTS_PER_TERM)
        parser.add_argument("--terms", nargs="+", default=None)
        parser.add_argument("--keyword-source", choices=["taxonomy", "db", "both"],
                            default="both")

    def handle(self, *args, **options):
        KAGGLE_DIR.mkdir(parents=True, exist_ok=True)
        GITHUB_DIR.mkdir(parents=True, exist_ok=True)
        OBSERVABLE_DIR.mkdir(parents=True, exist_ok=True)

        platform = options["platform"]
        limit    = options["limit_per_term"]

        if options["terms"]:
            search_terms = options["terms"]
        else:
            search_terms = _build_search_terms(options["keyword_source"])

        if platform == "observablehq":
            start_msg = f"Crawling [observablehq] · {OBSERVABLE_ACCOUNT} account"
        else:
            start_msg = (
                f"Crawling [{platform}] · {len(search_terms)} terms · {limit}/term"
            )
        emit(agent="crawl_repos", status="started", message=start_msg)

        total = 0

        if platform in ("kaggle", "all"):
            total += self._crawl_kaggle(search_terms, limit)

        if platform in ("github", "all"):
            if not GITHUB_TOKEN:
                emit(agent="crawl_repos", status="skipped",
                     message="GitHub: GITHUB_TOKEN not set in .env — skipping",
                     level="warning")
            else:
                total += self._crawl_github(search_terms, limit)

        if platform in ("observablehq", "all"):
            total += self._crawl_observablehq()

        emit(agent="crawl_repos", status="done",
             message=f"Done. {total} new notebooks added to DB.")
        self.stdout.write(self.style.SUCCESS(f"Crawled {total} new notebooks."))

    # ── Kaggle ─────────────────────────────────────────────────────────────────

    def _crawl_kaggle(self, terms: list[str], limit: int) -> int:
        api = self._get_kaggle_api()
        total = 0
        for i, term in enumerate(terms):
            n = self._kaggle_one_term(api, term, limit)
            total += n
            emit(agent="crawl_repos", status="running",
                 message=f"Kaggle '{term}': {n} new", progress=[i + 1, len(terms)])
            time.sleep(0.8)
        return total

    def _get_kaggle_api(self):
        import os
        username = config("KAGGLE_USERNAME", default="")
        key      = config("KAGGLE_KEY",      default="")
        if username:
            os.environ["KAGGLE_USERNAME"] = username
        if key:
            os.environ["KAGGLE_KEY"] = key
        project_kaggle = Path(settings.BASE_DIR) / "kaggle.json"
        if project_kaggle.exists():
            os.environ.setdefault("KAGGLE_CONFIG_DIR", str(settings.BASE_DIR))
        # v2.0.0 renamed the class to KaggleApi; older versions used KaggleApiExtended
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            api = KaggleApi()
        except ImportError:
            from kaggle.api.kaggle_api_extended import KaggleApiExtended
            api = KaggleApiExtended()
        api.authenticate()
        return api

    def _kaggle_one_term(self, api, term: str, limit: int) -> int:
        """SDK search first, HTTP fallback if 0 results."""
        kernels = self._kaggle_sdk_search(api, term, limit)
        if not kernels:
            kernels = self._kaggle_http_search(term, limit)
        if not kernels:
            return 0

        created = 0
        for kernel in kernels:
            try:
                if self._process_kaggle_kernel(api, kernel):
                    created += 1
            except Exception as exc:
                logger.warning("Kaggle kernel processing failed: %s", exc)
            time.sleep(0.3)
        return created

    def _kaggle_sdk_search(self, api, term: str, limit: int) -> list:
        try:
            # No language filter, no output_type filter — both silently kill results
            # because almost no notebooks are explicitly tagged. Post-filter by
            # library imports instead, which is reliable.
            results = api.kernels_list(
                search=term,
                page_size=min(limit, 100),
                sort_by="scoreDescending",
            ) or []
            logger.info("Kaggle SDK '%s' → %d results", term, len(results))
            return results
        except Exception as exc:
            logger.warning("Kaggle SDK search error for '%s': %s", term, exc)
            return []

    def _kaggle_http_search(self, term: str, limit: int) -> list[dict]:
        """Direct HTTP to Kaggle v1 REST API — more reliable for long-tail terms."""
        username = config("KAGGLE_USERNAME", default="")
        key      = config("KAGGLE_KEY",      default="")
        if not (username and key):
            return []

        creds   = base64.b64encode(f"{username}:{key}".encode()).decode()
        headers = {"Authorization": f"Basic {creds}", "User-Agent": "kaggle/2.0.0"}
        params  = {
            "search": term, "pageSize": min(limit, 20),
            "sortBy": "voteCount",
        }
        try:
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                resp = client.get(f"{KAGGLE_API_BASE}/kernels/list",
                                  headers=headers, params=params)
            if resp.status_code == 429:
                emit(agent="crawl_repos", status="running",
                     message="Kaggle rate limit — waiting 60s", level="warning")
                time.sleep(60)
                with httpx.Client(timeout=15, follow_redirects=True) as client:
                    resp = client.get(f"{KAGGLE_API_BASE}/kernels/list",
                                      headers=headers, params=params)
            if resp.status_code != 200:
                return []
            data = resp.json()
            if not isinstance(data, list):
                return []
            logger.info("Kaggle HTTP '%s' → %d results", term, len(data))
            return data
        except Exception as exc:
            logger.warning("Kaggle HTTP fallback failed for '%s': %s", term, exc)
            return []

    def _process_kaggle_kernel(self, api, kernel) -> bool:
        """Normalises SDK object or HTTP dict, downloads, saves. Returns True if new."""
        if isinstance(kernel, dict):
            ref      = kernel.get("ref", "")
            title    = kernel.get("title", ref)
            votes    = kernel.get("totalVotes")
            last_run = kernel.get("lastRunTime") or kernel.get("lastRunningRunTime")
            lang     = kernel.get("language", "python")
        else:
            ref      = getattr(kernel, "ref", "") or ""
            title    = getattr(kernel, "title", ref) or ref
            # v2.0.0 SDK uses snake_case internally: total_votes, last_run_time
            votes    = (getattr(kernel, "total_votes", None) or
                        getattr(kernel, "totalVotes", None))
            last_run = (getattr(kernel, "last_run_time", None) or
                        getattr(kernel, "lastRunTime", None))
            lang     = getattr(kernel, "language", "python")

        if not ref:
            return False
        if RepoSource.objects.filter(source_id=ref).exists():
            return False

        # Skip non-Python kernels before wasting time downloading them
        if lang and lang.lower() in ("r", "rmarkdown", "sqlite", "julia"):
            return False

        last_updated = _parse_date(last_run)
        if last_updated and last_updated.year < MIN_UPDATED_YEAR:
            return False

        out_dir = KAGGLE_DIR / re.sub(r"[^\w\-]", "_", ref)
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            api.kernels_pull(kernel=ref, path=str(out_dir))
        except Exception as exc:
            logger.warning("kernels_pull failed for %s: %s", ref, exc)
            return False

        # rglob handles nested dirs (v2.0.0 sometimes nests output)
        notebook_files = list(out_dir.rglob("*.ipynb")) + list(out_dir.rglob("*.py"))
        if not notebook_files:
            import shutil; shutil.rmtree(out_dir, ignore_errors=True)
            return False
        notebook_path = notebook_files[0]

        try:
            content = notebook_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return False

        if not _uses_vis_library(content):
            import shutil; shutil.rmtree(out_dir, ignore_errors=True)
            return False

        source = RepoSource.objects.create(
            platform="kaggle",
            source_id=ref,
            url=f"https://www.kaggle.com/code/{ref}",
            title=title or ref,
            author=ref.split("/")[0] if "/" in ref else "",
            stars=votes,
            language=lang or "python",
            last_updated=last_updated,
        )
        RepoArtifact.objects.create(
            source=source,
            artifact_type="notebook" if notebook_path.suffix == ".ipynb" else "script",
            raw_content_path=str(notebook_path.relative_to(settings.MEDIA_ROOT)),
        )
        return True

    # ── GitHub ─────────────────────────────────────────────────────────────────

    def _crawl_github(self, terms: list[str], limit: int) -> int:
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        total = 0
        for i, term in enumerate(terms):
            n = self._github_one_term(term, limit, headers)
            total += n
            emit(agent="crawl_repos", status="running",
                 message=f"GitHub '{term}': {n} new", progress=[i + 1, len(terms)])
            time.sleep(6)  # GitHub secondary rate limit: ~10 search req/min
        return total

    def _github_one_term(self, term: str, limit: int, headers: dict) -> int:
        """Search GitHub code for .ipynb files mentioning the term."""
        params = {
            "q":        f'"{term}" extension:ipynb',
            "per_page": min(limit, 30),
            "sort":     "indexed",
            "order":    "desc",
        }
        try:
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                resp = client.get(f"{GITHUB_API_BASE}/search/code",
                                  headers=headers, params=params)
        except Exception as exc:
            logger.warning("GitHub search failed for '%s': %s", term, exc)
            return 0

        if resp.status_code == 403:
            # Check if it's a rate limit or auth issue
            msg = resp.json().get("message", "")
            if "rate limit" in msg.lower():
                emit(agent="crawl_repos", status="running",
                     message="GitHub rate limit — waiting 60s", level="warning")
                time.sleep(60)
            return 0
        if resp.status_code != 200:
            logger.warning("GitHub search HTTP %d for '%s'", resp.status_code, term)
            return 0

        items = resp.json().get("items", [])
        logger.info("GitHub '%s' → %d items", term, len(items))

        created = 0
        seen_repos: set[str] = set()
        for item in items[:limit]:
            try:
                repo_fn = item.get("repository", {}).get("full_name", "")
                if repo_fn in seen_repos:
                    continue
                seen_repos.add(repo_fn)
                if self._process_github_item(item, headers):
                    created += 1
            except Exception as exc:
                logger.warning("GitHub item failed: %s", exc)
            time.sleep(0.5)
        return created

    def _process_github_item(self, item: dict, headers: dict) -> bool:
        repo    = item.get("repository", {})
        path    = item.get("path", "")
        name    = item.get("name", "")
        repo_fn = repo.get("full_name", "")

        if not repo_fn or not path:
            return False

        source_id = f"github/{repo_fn}/{path}"
        if RepoSource.objects.filter(source_id=source_id).exists():
            return False

        last_updated = _parse_date(repo.get("pushed_at"))
        if last_updated and last_updated.year < MIN_UPDATED_YEAR:
            return False

        # Build raw download URL
        default_branch = repo.get("default_branch", "main")
        raw_url = (
            f"https://raw.githubusercontent.com/{repo_fn}/{default_branch}/{path}"
        )
        try:
            with httpx.Client(timeout=20, follow_redirects=True,
                              headers=headers) as client:
                resp = client.get(raw_url)
            if resp.status_code != 200:
                return False
            content = resp.text
        except Exception as exc:
            logger.debug("GitHub raw download failed: %s", exc)
            return False

        if not _uses_vis_library(content):
            return False

        # Save to disk
        safe = re.sub(r"[^\w\-/]", "_", f"{repo_fn}/{path}")
        out_path = GITHUB_DIR / safe
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            out_path.write_text(content, encoding="utf-8")
        except Exception:
            return False

        # /search/code embeds a stripped repo object — stargazers_count is absent.
        # Fetch the full repo object to get the real star count.
        stars = None
        try:
            with httpx.Client(timeout=10, follow_redirects=True, headers=headers) as cl:
                r = cl.get(f"{GITHUB_API_BASE}/repos/{repo_fn}")
            if r.status_code == 200:
                stars = r.json().get("stargazers_count")
        except Exception:
            pass  # stars stays None — not critical

        source = RepoSource.objects.create(
            platform="github",
            source_id=source_id,
            url=item.get("html_url", f"https://github.com/{repo_fn}"),
            title=f"{repo_fn} — {name}",
            author=repo_fn.split("/")[0],
            stars=stars,
            language="python",
            last_updated=last_updated,
        )
        RepoArtifact.objects.create(
            source=source,
            artifact_type="notebook",
            raw_content_path=str(out_path.relative_to(settings.MEDIA_ROOT)),
        )
        return True

    # ── Observable HQ ──────────────────────────────────────────────────────────

    def _crawl_observablehq(self) -> int:
        """
        Paginate the Observable documents API for OBSERVABLE_ACCOUNT (@d3),
        download each notebook's compiled JavaScript source, and save to disk.

        No search-term loop needed — we consume the full account listing.
        Idempotent: skips notebooks already in DB (by source_id = @d3/<slug>).
        """
        total_new = 0
        page = 1

        with httpx.Client(
            timeout=20,
            follow_redirects=True,
            headers={"User-Agent": OBSERVABLE_UA},
        ) as client:
            while True:
                try:
                    resp = client.get(
                        f"{OBSERVABLE_API_BASE}/documents/{OBSERVABLE_ACCOUNT}",
                        params={"page": page},
                    )
                except Exception as exc:
                    emit(agent="crawl_repos", status="running",
                         message=f"Observable: listing page {page} failed: {exc}",
                         level="warning")
                    break

                if resp.status_code != 200:
                    emit(agent="crawl_repos", status="running",
                         message=(
                             f"Observable: listing page {page} "
                             f"returned HTTP {resp.status_code}"
                         ),
                         level="warning")
                    break

                data = resp.json()
                results = data.get("results", [])
                if not results:
                    break

                per_page  = data.get("per_page", 30)
                total_all = data.get("total", 0)
                n_pages   = max(1, -(-total_all // per_page))  # ceiling division

                for notebook in results:
                    try:
                        if self._process_observable_notebook(client, notebook):
                            total_new += 1
                    except Exception as exc:
                        slug = notebook.get("slug", "?")
                        logger.warning("Observable: failed on %s: %s", slug, exc)
                    time.sleep(_OBSERVABLE_FETCH_SLEEP)

                emit(
                    agent="crawl_repos", status="running",
                    message=(
                        f"Observable page {page}/{n_pages}: "
                        f"{total_new} new so far"
                    ),
                    progress=[page, n_pages],
                )

                if len(results) < per_page:
                    break
                page += 1
                time.sleep(_OBSERVABLE_PAGE_SLEEP)

        return total_new

    def _process_observable_notebook(
        self, client: httpx.Client, notebook: dict
    ) -> bool:
        """
        Download compiled JS for one Observable notebook and persist to DB.
        Returns True if the notebook was new and was successfully saved.

        Observable compiled JS (api.observablehq.com/@d3/<slug>.js?v=3) is a
        plain JavaScript ES module.  It is saved as a .js file so the existing
        detect_chart_types pipeline reads it unchanged via its else-branch
        (non-.ipynb → full content as a single code block).
        """
        slug  = notebook.get("slug", "")
        title = notebook.get("title", slug)
        likes = notebook.get("likes") or 0

        if not slug:
            return False

        source_id = f"{OBSERVABLE_ACCOUNT}/{slug}"
        if RepoSource.objects.filter(source_id=source_id).exists():
            return False

        # Download compiled JS source
        js_url = f"{OBSERVABLE_API_BASE}/{OBSERVABLE_ACCOUNT}/{slug}.js?v=3"
        try:
            resp = client.get(js_url)
        except Exception as exc:
            logger.warning("Observable: JS fetch failed for %s: %s", slug, exc)
            return False

        if resp.status_code != 200:
            logger.warning(
                "Observable: JS fetch HTTP %d for %s", resp.status_code, slug
            )
            return False

        js_content = resp.text

        # Save to disk.  Slugs can contain "/" (e.g. "treemap/2") — flatten to "-".
        safe_slug = re.sub(r"[^\w\-]", "-", slug).strip("-")
        out_path = OBSERVABLE_DIR / f"{safe_slug}.js"
        try:
            out_path.write_text(js_content, encoding="utf-8")
        except Exception as exc:
            logger.warning("Observable: save failed for %s: %s", slug, exc)
            return False

        last_updated = _parse_date(
            notebook.get("publish_time") or notebook.get("update_time")
        )

        source = RepoSource.objects.create(
            platform="observablehq",
            source_id=source_id,
            url=f"https://observablehq.com/{OBSERVABLE_ACCOUNT}/{slug}",
            title=title,
            author="d3",
            stars=likes,
            language="javascript",
            last_updated=last_updated,
        )
        RepoArtifact.objects.create(
            source=source,
            artifact_type="notebook",
            raw_content_path=str(out_path.relative_to(settings.MEDIA_ROOT)),
        )
        return True


# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_search_terms(source: str) -> list[str]:
    terms: list[str] = []
    if source in ("taxonomy", "both"):
        terms.extend(TAXONOMY_SEARCH_TERMS)
    if source in ("db", "both"):
        for kw in _get_db_keywords(top_n=25):
            if kw not in terms:
                terms.append(kw)
    seen: set[str] = set()
    result: list[str] = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result