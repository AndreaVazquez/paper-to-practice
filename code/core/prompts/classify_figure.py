"""
Prompts for figure classification (Component 3).
Used by: academic/management/commands/classify_figures.py
Role: IMAGE

No imports from any API library — pure strings/templates only.
"""


# ── Step A: Relevance filter ───────────────────────────────────────────────────

RELEVANCE_SYSTEM_PROMPT = (
    "You are a visualization researcher. "
    "Answer concisely and exactly as instructed."
)

RELEVANCE_PROMPT = """Is this image a data visualization chart or graph?
Examples of YES: bar chart, scatter plot, line chart, network diagram, map,
parallel coordinates, heatmap, treemap, chord diagram, sankey diagram.
Examples of NO: system architecture diagram, workflow box, author photo,
UI screenshot, equation, table of text, algorithm pseudocode.

Answer YES or NO only on the first line.
On the second line, give a confidence score between 0.0 and 1.0.

Example output:
YES
0.95"""


# ── Step B: Type classification ────────────────────────────────────────────────

def type_classification_prompt(taxonomy_list: list[str]) -> str:
    """
    Generate a prompt that asks the model to classify a visualization image
    using the VisImages taxonomy.

    Args:
        taxonomy_list: Flat list of all vis type strings from core.taxonomy.VIS_TYPES
    """
    taxonomy_str = "\n".join(f"  - {t}" for t in taxonomy_list)
    return f"""Classify this data visualization image using exactly one type from the list below.

Taxonomy:
{taxonomy_str}

Choose the single best match. If uncertain between two types, pick the more specific one.

Respond with valid JSON only, no explanation, no markdown fences:
{{"type": "<type from list>", "confidence": <0.0-1.0>, "notes": "<one sentence reason>"}}"""


TYPE_CLASSIFICATION_SYSTEM_PROMPT = (
    "You are an expert in data visualization taxonomy. "
    "Respond only with the JSON object requested. No extra text."
)
