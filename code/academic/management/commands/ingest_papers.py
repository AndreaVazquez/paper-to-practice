"""
Component 1 — Paper Ingestion
Command: python manage.py ingest_papers [--source all|visimages|seed_doi|vis2019|vis2020|vis2021|vis2022|vis2023|vis2024|vis2025]

Ingests papers from three sources in priority order:
  1. VisImages JSON metadata (creates Papers + PaperFigures, skips PDF work)
  2. Seed DOI list (74 curated DOIs from a .md file)
  3. IEEE VIS program pages (2020, 2021, 2022, 2023, 2024, 2025)

Idempotent: skips records that already exist (matched by DOI or title).
"""

import csv
import json
import logging
import os
import re
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from academic.models import Paper, PaperFigure
from core.agent_log import emit

logger = logging.getLogger(__name__)

# ── Source URLs ────────────────────────────────────────────────────────────────# IEEE VIS program pages serve paper data from JSON endpoints loaded by JS.
# These are the actual data URLs — not the HTML pages.
IEEE_VIS_PAPERS_JSON = {
    "vis2025": "https://ieeevis.org/year/2025/program/papers.json",
    "vis2024": "https://ieeevis.org/year/2024/program/papers.json",
    "vis2023": "https://virtual.ieeevis.org/year/2023/papers.json",
    "vis2022": "https://virtual.ieeevis.org/year/2022/papers.json",
    "vis2021": "https://virtual.ieeevis.org/year/2021/papers.json",
    "vis2020": "https://virtual.ieeevis.org/year/2020/papers.json",
}

# VIS 2019 uses a different site structure — papers are embedded directly in
# the HTML of the papers-sessions page rather than a separate JSON endpoint.
VIS2019_HTML_URL = "https://ieeevis.org/year/2019/info/papers-sessions"

ARXIV_PDF_TEMPLATE = "https://arxiv.org/pdf/{arxiv_id}"

# ── VisImages expected paths ───────────────────────────────────────────────────
VISIMAGES_ROOT = Path(settings.MEDIA_ROOT) / "visimages"
# VisImages zip should be extracted to media/visimages/ by the user before running.
# Expected structure inside: images/ + annotations.json (or similar)

# ── Seed DOI file ──────────────────────────────────────────────────────────────
# Place your seed_dois.md in the project root before running.
SEED_DOI_FILE = Path(settings.BASE_DIR) / "seed_dois.md"


class Command(BaseCommand):
    help = "Ingest IEEE VIS papers from VisImages, seed DOI list, and program pages."

    def add_arguments(self, parser):
        parser.add_argument(
            "--source",
            choices=["all", "visimages", "seed_doi", "vis2019", "vis2020", "vis2021", "vis2022", "vis2023", "vis2024", "vis2025"],
            default="all",
            help="Which source to ingest (default: all)",
        )
        parser.add_argument(
            "--visimages-root",
            type=str,
            default=str(VISIMAGES_ROOT),
            help="Path to extracted VisImages dataset directory",
        )
        parser.add_argument(
            "--seed-doi-file",
            type=str,
            default=str(SEED_DOI_FILE),
            help="Path to seed_dois.md file",
        )

    def handle(self, *args, **options):
        source = options["source"]
        visimages_root = Path(options["visimages_root"])
        seed_doi_file = Path(options["seed_doi_file"])

        emit(agent="ingest_papers", status="started",
             message=f"Starting paper ingestion (source={source})")

        if source in ("all", "visimages"):
            self._ingest_visimages(visimages_root)

        if source in ("all", "seed_doi"):
            self._ingest_seed_dois(seed_doi_file)
            self._enrich_from_vispubdata()  # cross-reference to fill in titles/abstracts

        if source in ("all", "vis2019"):
            self._ingest_vis2019_html(VIS2019_HTML_URL)

        if source in ("all", "vis2020"):
            self._ingest_vis_program("vis2020", IEEE_VIS_PAPERS_JSON["vis2020"], 2020)

        if source in ("all", "vis2021"):
            self._ingest_vis_program("vis2021", IEEE_VIS_PAPERS_JSON["vis2021"], 2021)

        if source in ("all", "vis2022"):
            self._ingest_vis_program("vis2022", IEEE_VIS_PAPERS_JSON["vis2022"], 2022)

        if source in ("all", "vis2023"):
            self._ingest_vis_program("vis2023", IEEE_VIS_PAPERS_JSON["vis2023"], 2023)

        if source in ("all", "vis2024"):
            self._ingest_vis_program("vis2024", IEEE_VIS_PAPERS_JSON["vis2024"], 2024)

        if source in ("all", "vis2025"):
            self._ingest_vis_program("vis2025", IEEE_VIS_PAPERS_JSON["vis2025"], 2025)

        total = Paper.objects.count()
        emit(agent="ingest_papers", status="done",
             message=f"Ingestion complete. Total papers in DB: {total}")
        self.stdout.write(self.style.SUCCESS(f"Done. Total papers: {total}"))

    # ── VisImages ──────────────────────────────────────────────────────────────

    def _ingest_visimages(self, root: Path) -> None:
        emit(agent="ingest_papers", status="running",
             message=f"Looking for VisImages data at {root}")

        if not root.exists():
            emit(agent="ingest_papers", status="skipped",
                 message=f"VisImages root not found at {root}. Skipping.",
                 level="warning")
            return

        annotation_file = root / "annotation.json"
        if not annotation_file.exists():
            emit(agent="ingest_papers", status="error",
                 message="annotation.json not found in VisImages root.",
                 level="error")
            return

        # Load metadata.csv — no header row, columns: track, title, doi, url
        #
        # KEY MAPPING: paper_id in annotation.json is a 0-based index directly
        # into metadata_rows (i.e. paper_id=0 → row 0). No offset needed.
        metadata_rows = []  # list of dicts
        metadata_file = root / "metadata.csv"
        if metadata_file.exists():
            with open(metadata_file, encoding="utf-8") as fh:
                reader = csv.reader(fh)
                for row in reader:
                    if len(row) >= 3:
                        doi_val = row[2].strip()
                        url_val = row[3].strip() if len(row) >= 4 else ""
                        # Year is encoded in the DOI: e.g. 10.1109/VISUAL.1990.146359
                        # or 10.1109/TVCG.2013.121 — grab the 4-digit number after the last dot-separated word.
                        year_match = re.search(r"\.(?:19|20)(\d{2})\.", doi_val)
                        year_val = int("19" + year_match.group(1)) if year_match and int(year_match.group(1)) >= 90 else \
                                   int("20" + year_match.group(1)) if year_match else None
                        metadata_rows.append({
                            "track": row[0].strip(),
                            "title": row[1].strip(),
                            "doi":   doi_val,
                            "url":   url_val,
                            "year":  year_val,
                        })
                    else:
                        metadata_rows.append({})  # keep index alignment
            emit(agent="ingest_papers", status="running",
                 message=f"Loaded {len(metadata_rows)} rows from metadata.csv")
        else:
            emit(agent="ingest_papers", status="running",
                 message="metadata.csv not found — papers will have minimal metadata",
                 level="warning")

        def _get_metadata(paper_id: str) -> dict:
            """Return the metadata row for a paper_id (direct index — paper_id IS the row index)."""
            idx = int(paper_id)
            if 0 <= idx < len(metadata_rows):
                return metadata_rows[idx]
            return {}

        def _map_track(track_raw: str) -> str:
            t = track_raw.strip()
            if "VAST" in t:
                return "VAST"
            if "SciVis" in t or "Sci" in t:
                return "SciVis"
            if t:  # "Vis", "InfoVis", anything else
                return "InfoVis"
            return "unknown"

        with open(annotation_file, encoding="utf-8") as fh:
            data = json.load(fh)

        paper_ids = list(data.keys())
        emit(agent="ingest_papers", status="running",
             message=f"annotation.json loaded — {len(paper_ids)} papers, processing...")

        created_papers = 0
        created_figures = 0
        images_dir = root / "images"

        for i, paper_id in enumerate(paper_ids):
            images = data[paper_id]

            # Resolve metadata directly by row index — no post-hoc title matching needed
            meta = _get_metadata(paper_id)
            title  = meta.get("title") or f"[VisImages paper {paper_id}]"
            doi    = meta.get("doi") or None
            track  = _map_track(meta.get("track", ""))
            year   = meta.get("year")  # extracted from DOI string e.g. VISUAL.1990 → 1990

            # Upsert: prefer DOI match; fall back to title+source for doi-less rows
            if doi:
                paper, created = Paper.objects.get_or_create(
                    doi=doi,
                    defaults={
                        "source": "visimages", "title": title,
                        "track": track, "year": year,
                    },
                )
                # If the paper already existed (from seed_doi ingest), update track/year
                update_fields = []
                if not created and paper.track == "unknown" and track != "unknown":
                    paper.track = track
                    update_fields.append("track")
                if not created and paper.year is None and year is not None:
                    paper.year = year
                    update_fields.append("year")
                if update_fields:
                    paper.save(update_fields=update_fields)
            else:
                paper, created = Paper.objects.get_or_create(
                    source="visimages",
                    title=title,
                    defaults={"doi": None, "track": track, "year": year},
                )

            if created:
                created_papers += 1

            # Process each image record for this paper
            for image_record in images:
                fig_created = self._process_visimages_image(
                    paper, paper_id, image_record, images_dir
                )
                if fig_created:
                    created_figures += 1

            if i % 100 == 0:
                emit(agent="ingest_papers", status="running",
                     message=f"VisImages: {i}/{len(paper_ids)} papers processed "
                             f"({created_papers} new, {created_figures} figures)",
                     progress=[i, len(paper_ids)])

        emit(agent="ingest_papers", status="running",
             message=f"VisImages done: {created_papers} new papers, {created_figures} figures")



    def _process_visimages_image(
        self, paper, paper_id: str, image_record: dict, images_dir: Path
    ) -> bool:
        """
        Process one image record from annotation.json.
        Returns True if a new PaperFigure was created.

        Image record shape:
          { "image_id": 42, "file_name": "fig3.png",
            "nums_of_visualizations": {"bar_chart": 1, "line_chart": 2} }

        vis_type logic:
          - Empty nums_of_visualizations → is_visualization=None (LLM classifier picks it up)
          - All mapped types are None (e.g. only "table") → is_visualization=False
          - Otherwise → dominant type (highest count), mapped through VISIMAGES_TYPE_MAP
        """
        from core.taxonomy import VISIMAGES_TYPE_MAP

        image_id = image_record.get("image_id", 0)
        file_name = image_record.get("file_name", "")
        nums_of_vis = image_record.get("nums_of_visualizations", {})

        # Image path: images/{paper_id}/{file_name}
        # Actual file layout: images/{paper_id}/{image_id}.png
        # file_name in annotation.json (e.g. "1018_00.png") is NOT the disk filename.
        img_path = images_dir / str(paper_id) / f"{image_id}.png"
        if not img_path.exists():
            # Fallback: try the annotated file_name just in case
            fallback = images_dir / str(paper_id) / file_name
            if fallback.exists():
                img_path = fallback
            else:
                return False

        if not nums_of_vis:
            # No annotation — leave for LLM classifier
            is_vis = None
            vis_type = ""
        else:
            # Filter out None-mapped types (tables, etc.)
            mapped = {
                VISIMAGES_TYPE_MAP.get(t): count
                for t, count in nums_of_vis.items()
                if VISIMAGES_TYPE_MAP.get(t) is not None
            }
            if not mapped:
                # Only non-vis types present (e.g. just "table")
                is_vis = False
                vis_type = ""
            else:
                is_vis = True
                # Take the type with the highest instance count
                vis_type = max(mapped, key=mapped.get)

        fig, created = PaperFigure.objects.get_or_create(
            paper=paper,
            figure_index=image_id,
            defaults={
                "image_local_path": str(img_path),
                "is_visualization": is_vis,
                "vis_type": vis_type,
                "vis_type_confidence": 1.0 if is_vis else None,
                "annotation_source": "visimages_json",
            },
        )
        return created

    # ── Seed DOIs ──────────────────────────────────────────────────────────────

    def _ingest_seed_dois(self, seed_file: Path) -> None:
        emit(agent="ingest_papers", status="running",
             message=f"Loading seed DOIs from {seed_file}")

        if not seed_file.exists():
            emit(agent="ingest_papers", status="skipped",
                 message=f"Seed DOI file not found at {seed_file}. Skipping.",
                 level="warning")
            return

        text = seed_file.read_text(encoding="utf-8")
        # Extract DOIs: match standard DOI pattern 10.xxxx/xxxx
        dois = re.findall(r"10\.\d{4,}/\S+", text)
        # Clean trailing punctuation
        dois = [d.rstrip(".,;)\"'") for d in dois]
        dois = list(dict.fromkeys(dois))  # deduplicate, preserve order

        emit(agent="ingest_papers", status="running",
             message=f"Found {len(dois)} DOIs in seed file")

        created = 0
        for i, doi in enumerate(dois):
            # Check if already ingested
            if Paper.objects.filter(doi=doi).exists():
                continue

            # Create a stub record; PDF download happens in extract_figures
            paper = Paper.objects.create(
                source="seed_doi",
                doi=doi,
                title=f"[Pending metadata] {doi}",
                pdf_url=f"https://doi.org/{doi}",
            )
            created += 1

            emit(agent="ingest_papers", status="running",
                 message=f"Seed DOI {i+1}/{len(dois)}: {doi}",
                 progress=[i + 1, len(dois)])

        emit(agent="ingest_papers", status="running",
             message=f"Seed DOIs: {created} new stubs created")

    def _enrich_from_vispubdata(self) -> None:
        """
        Enrich '[Pending metadata]' stubs by querying the CrossRef API per DOI.

        VisPubData no longer hosts a public CSV — its canonical source is now a
        Google Spreadsheet. CrossRef is the upstream source VisPubData itself uses
        and is the most reliable option: free, no auth, covers 2023–2025 papers.

        API: https://api.crossref.org/works/{doi}
        Polite pool: send email via User-Agent header.
        Rate limit: ~50 req/s in polite pool — we sleep 0.1s between calls.
        """
        from decouple import config as _config

        email = _config("CONTACT_EMAIL", default="user@example.com")
        headers = {
            "User-Agent": f"PaperToPractice/1.0 (mailto:{email})",
        }

        stubs = list(Paper.objects.filter(title__startswith="[Pending metadata]"))
        if not stubs:
            emit(agent="ingest_papers", status="running",
                 message="No pending stubs to enrich — all seed DOIs already have metadata")
            return

        emit(agent="ingest_papers", status="running",
             message=f"Enriching {len(stubs)} seed DOI stubs via CrossRef API")

        enriched = 0
        failed = 0

        with httpx.Client(follow_redirects=True, timeout=15, headers=headers) as client:
            for i, paper in enumerate(stubs):
                if not paper.doi:
                    continue

                url = f"https://api.crossref.org/works/{paper.doi}"
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                    item = resp.json().get("message", {})
                except Exception as exc:
                    failed += 1
                    emit(agent="ingest_papers", status="running",
                         message=f"CrossRef miss for {paper.doi}: {exc}",
                         level="warning")
                    time.sleep(0.2)
                    continue

                # Title
                title_parts = item.get("title") or []
                title = title_parts[0].strip() if title_parts else ""
                if not title:
                    failed += 1
                    continue

                # Abstract — CrossRef wraps it in JATS XML tags sometimes
                abstract_raw = item.get("abstract") or ""
                abstract = re.sub(r"<[^>]+>", "", abstract_raw).strip()

                # Year — prefer published-print, fall back to published-online
                year = None
                for date_field in ("published-print", "published-online", "created"):
                    date_parts = (item.get(date_field) or {}).get("date-parts", [[]])
                    if date_parts and date_parts[0]:
                        try:
                            year = int(date_parts[0][0])
                            break
                        except (ValueError, TypeError):
                            pass

                # Authors
                authors = []
                for a in item.get("author") or []:
                    given = a.get("given", "").strip()
                    family = a.get("family", "").strip()
                    name = f"{given} {family}".strip() if given else family
                    if name:
                        authors.append(name)

                # Container (journal/conference) — used to infer track
                container = (item.get("container-title") or [""])[0].lower()
                track = "unknown"
                if "information visualization" in container or "infovis" in container:
                    track = "InfoVis"
                elif "vast" in container or "visual analytics" in container:
                    track = "VAST"
                elif "scientific visualization" in container or "scivis" in container:
                    track = "SciVis"
                # Most IEEE VIS TVCG papers don't carry track in CrossRef — leave as unknown;
                # the vis2020/2021/2022/2023/2024/2025 ingestion will have already set track for those.

                paper.title = title
                paper.abstract = abstract
                if year:
                    paper.year = year
                paper.track = track
                paper.set_authors(authors)
                paper.save(update_fields=["title", "abstract", "year", "track", "authors"])
                enriched += 1

                if (i + 1) % 10 == 0:
                    emit(agent="ingest_papers", status="running",
                         message=f"CrossRef: {i+1}/{len(stubs)} stubs processed "
                                 f"({enriched} enriched, {failed} failed)",
                         progress=[i + 1, len(stubs)])

                time.sleep(0.1)  # polite pool — ~10 req/s

        emit(agent="ingest_papers", status="running",
             message=f"CrossRef enrichment done: {enriched} enriched, {failed} failed/missing")

    # ── IEEE VIS 2019 (HTML scrape) ───────────────────────────────────────────

    def _ingest_vis2019_html(self, url: str) -> None:
        """
        Ingest IEEE VIS 2019 papers by scraping the papers-sessions HTML page.

        The 2019 site renders paper data server-side into a single <article>
        element rather than loading from a JSON endpoint.  Each paper occupies
        one <p> tag with this structure:

          <p>
            <strong>[V/I/S] Title (venue)</strong> [optional award em]
            <br>Authors: Name1, Name2, ...
            <br><a href="https://vimeo.com/...">Video Preview</a> | ... |
                <a href="https://doi.org/10.1109/...">DOI</a>
          </p>

        Track prefix:
          [V] → VAST   [I] → InfoVis   [S] → SciVis   (none) → InfoVis

        Venue suffix in title:  (J) SI journal · (T) prior TVCG · (C) conference
        These are stripped from the stored title.

        No abstracts are stored — fetch_abstracts handles those via DOI lookup.
        """
        from bs4 import BeautifulSoup

        emit(agent="ingest_papers", status="running",
             message="Fetching IEEE VIS 2019 papers page (HTML scrape)")

        try:
            raw = self._fetch_with_retry(url)
        except Exception as exc:
            emit(agent="ingest_papers", status="error",
                 message=f"Failed to fetch VIS 2019 page: {exc}", level="error")
            return

        soup = BeautifulSoup(raw, "html.parser")
        article = soup.find("article", class_="content")
        if not article:
            emit(agent="ingest_papers", status="error",
                 message="VIS 2019: could not find <article class='content'>",
                 level="error")
            return

        # Track prefix → our track value
        _TRACK_MAP = {"V": "VAST", "I": "InfoVis", "S": "SciVis"}
        # Venue suffixes to strip from titles
        _VENUE_RE = re.compile(r"\s*\([JTC]\)\s*$")
        # Award suffix in em tags — ignored, just stripped automatically

        created = 0
        skipped = 0
        total_candidates = 0

        for p in article.find_all("p"):
            # Must have a DOI link to be a paper entry
            doi_tag = p.find("a", href=re.compile(r"doi\.org/"))
            if not doi_tag:
                continue

            total_candidates += 1

            # ── DOI ────────────────────────────────────────────────────────────
            doi_href = doi_tag.get("href", "")
            m = re.search(r"doi\.org/(.+)", doi_href)
            if not m:
                continue
            doi = m.group(1).rstrip("/")

            # ── Title + track ──────────────────────────────────────────────────
            strong = p.find("strong")
            if not strong:
                continue
            raw_title = strong.get_text(strip=True)

            # Extract track prefix: "[V] Title" → track=VAST, title="Title"
            track = "InfoVis"  # default when no prefix present
            prefix_m = re.match(r"^\[([VIS])\]\s+", raw_title)
            if prefix_m:
                track = _TRACK_MAP.get(prefix_m.group(1), "InfoVis")
                raw_title = raw_title[prefix_m.end():]

            # Strip venue suffix: "Title (J)" → "Title"
            title = _VENUE_RE.sub("", raw_title).strip()
            if not title:
                continue

            # ── Authors ────────────────────────────────────────────────────────
            full_text = p.get_text("\n")
            authors: list[str] = []
            for line in full_text.splitlines():
                line = line.strip()
                if line.startswith("Authors:"):
                    author_str = line[len("Authors:"):].strip()
                    authors = [a.strip() for a in author_str.split(",") if a.strip()]
                    break

            # ── Idempotency ────────────────────────────────────────────────────
            if Paper.objects.filter(doi=doi).exists():
                skipped += 1
                continue
            if Paper.objects.filter(title=title, year=2019).exists():
                skipped += 1
                continue

            # ── Create ─────────────────────────────────────────────────────────
            paper = Paper.objects.create(
                source="vis2019",
                doi=doi,
                title=title,
                year=2019,
                track=track,
                abstract="",        # populated later by fetch_abstracts
                pdf_url=f"https://doi.org/{doi}",
            )
            paper.set_authors(authors)
            paper.save(update_fields=["authors"])
            created += 1

        emit(agent="ingest_papers", status="running",
             message=(
                 f"IEEE VIS 2019: {total_candidates} paper entries found, "
                 f"{created} created, {skipped} already existed"
             ))

    # ── IEEE VIS program pages ─────────────────────────────────────────────────

    def _ingest_vis_program(self, source_key: str, json_url: str, year: int) -> None:
        """
        Ingest papers from an IEEE VIS program JSON endpoint.
        The program pages render papers via JavaScript from papers.json —
        the HTML itself contains only an empty placeholder div.
        """
        emit(agent="ingest_papers", status="running",
             message=f"Fetching IEEE VIS {year} papers JSON")

        try:
            raw = self._fetch_with_retry(json_url)
        except Exception as exc:
            emit(agent="ingest_papers", status="error",
                 message=f"Failed to fetch {json_url}: {exc}", level="error")
            return

        try:
            import json as _json
            papers_data = _json.loads(raw)
        except Exception as exc:
            emit(agent="ingest_papers", status="error",
                 message=f"Could not parse papers JSON for {year}: {exc}", level="error")
            return

        if not isinstance(papers_data, list):
            emit(agent="ingest_papers", status="error",
                 message=f"Unexpected JSON structure for {year}: {type(papers_data)}", level="error")
            return

        emit(agent="ingest_papers", status="running",
             message=f"IEEE VIS {year}: {len(papers_data)} papers found in JSON")

        created = 0
        for i, item in enumerate(papers_data):
            title = (item.get("title") or "").strip()
            if not title:
                continue

            doi = (item.get("doi") or "").strip() or None

            # 2020 and 2021 carry no dedicated doi field; the DOI is embedded in
            # external_paper_link as https://doi.org/… or https://dx.doi.org/…
            if not doi:
                ext_link = (item.get("external_paper_link") or "").strip()
                m = re.search(r"(?:dx\.)?doi\.org/(.+)", ext_link)
                if m:
                    doi = m.group(1).rstrip("/")

            # Skip if already exists
            if doi and Paper.objects.filter(doi=doi).exists():
                continue
            if not doi and Paper.objects.filter(title=title, year=year).exists():
                continue

            # Authors: list of {name, email} dicts or plain strings
            raw_authors = item.get("authors") or []
            if raw_authors and isinstance(raw_authors[0], dict):
                authors = [a.get("name", "").strip() for a in raw_authors if a.get("name")]
            else:
                authors = [str(a).strip() for a in raw_authors if a]

            # Abstract
            abstract = (item.get("abstract") or "").strip()

            # Track — infer from event_title
            event_title = (item.get("event_title") or item.get("session_title") or "").lower()
            if "infovis" in event_title or "information visualization" in event_title:
                track = "InfoVis"
            elif "vast" in event_title or "visual analytics" in event_title:
                track = "VAST"
            elif "scivis" in event_title or "scientific visualization" in event_title:
                track = "SciVis"
            else:
                track = "unknown"

            # PDF URL: prefer accessible_pdf, then pdf_url, then preprint_link
            pdf_url = (
                item.get("accessible_pdf") or
                item.get("pdf_url") or
                item.get("preprint_link") or
                item.get("external_paper_link") or
                ""
            )

            paper = Paper.objects.create(
                source=source_key,
                doi=doi,
                title=title,
                year=year,
                track=track,
                abstract=abstract,
                pdf_url=pdf_url or "",
            )
            paper.set_authors(authors)
            paper.save(update_fields=["authors"])
            created += 1

            if i % 50 == 0 and i > 0:
                emit(agent="ingest_papers", status="running",
                     message=f"IEEE VIS {year}: {i}/{len(papers_data)} processed",
                     progress=[i, len(papers_data)])

        emit(agent="ingest_papers", status="running",
             message=f"IEEE VIS {year}: {created} new papers created")

    def _fetch_with_retry(self, url: str, retries: int = 3) -> str:
        for attempt in range(1, retries + 1):
            # Disable SSL verification on retry attempts — handles sites with
            # misconfigured certificates (e.g. hostname mismatch on vispubdata.org)
            verify = attempt == 1
            try:
                with httpx.Client(follow_redirects=True, timeout=30, verify=verify) as client:
                    response = client.get(url)
                    response.raise_for_status()
                    return response.text
            except Exception as exc:
                if attempt == retries:
                    raise
                logger.warning("Fetch attempt %d failed for %s: %s", attempt, url, exc)
                time.sleep(2 ** attempt)
        raise RuntimeError(f"All fetch attempts failed for {url}")
