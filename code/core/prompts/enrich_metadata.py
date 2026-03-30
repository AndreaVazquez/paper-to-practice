"""
Prompts for paper metadata enrichment (Component 4).
Used by: academic/management/commands/enrich_metadata.py
Role: TEXT

No imports from any API library — pure strings/templates only.
"""


ENRICH_SYSTEM_PROMPT = (
    "You are a visualization research analyst. "
    "Extract structured metadata from academic paper abstracts. "
    "Respond only with the JSON object requested. No extra text."
)


def enrich_metadata_prompt(title: str, abstract: str) -> str:
    """
    Generate a prompt to extract keywords and topic labels from a paper.

    Args:
        title:    Paper title.
        abstract: Paper abstract text.
    """
    return f"""Extract metadata from this IEEE VIS paper.

Title: {title}

Abstract:
{abstract}

Return a JSON object with exactly two fields:
1. "keywords": an array of 5-10 keyword phrases describing the visualization
   techniques, methods, and analytic tasks in this paper. Use noun phrases,
   not full sentences. Be specific (e.g. "parallel coordinates brushing" not
   just "interaction").
2. "topics": an array of 2-3 high-level topic labels for this paper. Generate
   these freely — do not use a fixed vocabulary. Examples of good topic labels:
   "spatiotemporal visualization", "network topology analysis",
   "uncertainty visualization". Be concise.

Respond with valid JSON only:
{{"keywords": ["...", "..."], "topics": ["...", "..."]}}"""
