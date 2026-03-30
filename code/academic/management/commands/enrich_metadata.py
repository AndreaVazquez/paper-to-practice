"""
Component 4 — Paper Metadata Enrichment
Command: python manage.py enrich_metadata [--limit N]

For each Paper that has an abstract but no extracted keywords,
sends title + abstract to the TEXT role model to generate:
  - 5-10 keyword phrases
  - 2-3 free-form topic labels

Idempotent: skips papers that already have keywords.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.core.management.base import BaseCommand

from academic.models import Paper
from core.agent_log import emit
from core.config import get_role_concurrency
from core.llm_client import call_llm
from core.prompts.enrich_metadata import ENRICH_SYSTEM_PROMPT, enrich_metadata_prompt

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Enrich paper metadata with LLM-extracted keywords and topics."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=None,
                            help="Process at most N papers")

    def handle(self, *args, **options):
        # Papers with abstracts but no extracted keywords
        qs = Paper.objects.exclude(abstract="").filter(
            keywords_extracted__in=["[]", "", None]
        )
        papers = list(qs)
        if options["limit"]:
            papers = papers[: options["limit"]]

        total = len(papers)
        emit(agent="enrich_metadata", status="started",
             message=f"Enriching {total} papers with keywords/topics")

        enriched = 0
        with ThreadPoolExecutor(max_workers=get_role_concurrency("TEXT")) as executor:
            future_to_paper = {
                executor.submit(self._enrich_one, paper, i, total): paper
                for i, paper in enumerate(papers)
            }
            for future in as_completed(future_to_paper):
                paper = future_to_paper[future]
                try:
                    future.result()
                    enriched += 1
                except Exception as exc:
                    logger.error("enrich_metadata: paper %d failed: %s", paper.id, exc)
                    emit(agent="enrich_metadata", status="error",
                         message=f"Paper {paper.id} failed: {exc}",
                         record_id=paper.id, level="error")

        emit(agent="enrich_metadata", status="done",
             message=f"Done. {enriched}/{total} papers enriched.")
        self.stdout.write(self.style.SUCCESS(f"Enriched {enriched}/{total} papers."))

    def _enrich_one(self, paper: Paper, index: int, total: int) -> None:
        prompt = enrich_metadata_prompt(paper.title, paper.abstract)
        result = call_llm(
            role="TEXT",
            prompt=prompt,
            response_format="json",
            system_prompt=ENRICH_SYSTEM_PROMPT,
        )

        if isinstance(result, dict):
            keywords = result.get("keywords", [])
            topics = result.get("topics", [])
        else:
            keywords = []
            topics = []
            logger.warning("enrich_metadata: unexpected response type for paper %d", paper.id)

        paper.keywords_extracted = json.dumps(keywords)
        paper.topics_extracted = json.dumps(topics)
        paper.save(update_fields=["keywords_extracted", "topics_extracted"])

        emit(agent="enrich_metadata", status="running",
             message=f"Paper {paper.id}: {len(keywords)} keywords, {len(topics)} topics",
             record_id=paper.id, progress=[index + 1, total])
