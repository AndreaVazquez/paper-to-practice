"""
Prompts for narrative generation (Component 9).
Used by: tracing/views.py (generate endpoint)
Role: REASONING

No imports from any API library — pure strings/templates only.
"""


GENERATE_NARRATIVE_SYSTEM_PROMPT = (
    "You are a data visualization educator writing for practitioners. "
    "Write clearly, avoid academic jargon, and be concrete. "
    "Target length: 150-250 words."
)


def generate_narrative_prompt(
    vis_type: str,
    total_papers: int,
    year_range: tuple[int, int],
    tracks: dict[str, int],
    top_keywords: list[str],
    abstract_sample: list[dict],
    total_traces: int,
    encoding_drift_counts: dict[str, int],
    interaction_drift_counts: dict[str, int],
    task_drift_counts: dict[str, int],
    drift_notes_sample: list[dict],
) -> str:
    """
    Build a prompt for generating a "design anchor" narrative about a
    visualization type, grounded in aggregate corpus context rather than
    a single arbitrarily-selected paper.

    Args:
        vis_type:                 Visualization type label (e.g. "Scatter").
        total_papers:             Number of unique IEEE VIS papers in the corpus
                                  that contain at least one figure of this type.
        year_range:               (earliest_year, latest_year) across those papers.
        tracks:                   {"InfoVis": n, "VAST": n, "SciVis": n, ...}
        top_keywords:             Up to 10 most-frequent keywords extracted across
                                  all papers for this type.
        abstract_sample:          Up to 3 dicts with keys "title", "year", "track",
                                  "abstract" — spread across the year range to give
                                  the model representative academic text.
        total_traces:             Number of confirmed paper→repository trace pairs.
        encoding_drift_counts:    {"none": n, "minor": n, "major": n}
        interaction_drift_counts: {"none": n, "minor": n, "major": n}
        task_drift_counts:        {"none": n, "minor": n, "major": n}
        drift_notes_sample:       Up to 3 sample drift note dicts for concreteness.
    """
    # ── Corpus summary ─────────────────────────────────────────────────────────
    track_str = ", ".join(
        f"{t} ({n})" for t, n in sorted(tracks.items(), key=lambda x: -x[1])
        if t != "unknown" and n > 0
    ) or "unknown"
    kw_str = ", ".join(top_keywords[:10]) if top_keywords else "none extracted"
    yr_start, yr_end = year_range

    # ── Abstract sample ────────────────────────────────────────────────────────
    abstracts_block = ""
    for i, ab in enumerate(abstract_sample[:3], 1):
        title   = ab.get("title", "")
        year    = ab.get("year", "")
        track   = ab.get("track", "")
        text    = ab.get("abstract", "").strip()
        if text:
            abstracts_block += (
                f"\nSample paper {i}: \"{title}\" "
                f"(IEEE VIS {year}, {track})\n{text}\n"
            )
    abstracts_section = (
        abstracts_block
        if abstracts_block
        else "\n(No abstracts available - reason from the keywords and drift evidence.)\n"
    )

    # ── Drift summary ──────────────────────────────────────────────────────────
    drift_summary = (
        f"Encoding drift:    none={encoding_drift_counts.get('none', 0)}, "
        f"minor={encoding_drift_counts.get('minor', 0)}, "
        f"major={encoding_drift_counts.get('major', 0)}\n"
        f"Interaction drift: none={interaction_drift_counts.get('none', 0)}, "
        f"minor={interaction_drift_counts.get('minor', 0)}, "
        f"major={interaction_drift_counts.get('major', 0)}\n"
        f"Task drift:        none={task_drift_counts.get('none', 0)}, "
        f"minor={task_drift_counts.get('minor', 0)}, "
        f"major={task_drift_counts.get('major', 0)}"
    )

    notes_text = ""
    for i, note in enumerate(drift_notes_sample[:3], 1):
        enc   = note.get("encoding_notes", "")
        inter = note.get("interaction_notes", "")
        task  = note.get("task_notes", "")
        notes_text += f"\nExample {i}:\n"
        if enc:
            notes_text += f"  Encoding: {enc}\n"
        if inter:
            notes_text += f"  Interaction: {inter}\n"
        if task:
            notes_text += f"  Task: {task}\n"

    return f"""Write a "design anchor" narrative for the visualization type: {vis_type}

## ACADEMIC CORPUS OVERVIEW
This type appears in {total_papers} IEEE VIS papers spanning {yr_start}–{yr_end}.
Tracks represented: {track_str}
Recurring research themes (extracted keywords): {kw_str}

## SAMPLE ABSTRACTS FROM THE CORPUS
These are drawn from different years to represent the range of academic work:{abstracts_section}

## EVIDENCE FROM {total_traces} REAL-WORLD IMPLEMENTATIONS
Drift summary across all matched repository notebooks:
{drift_summary}
{notes_text}

## INSTRUCTIONS
Write a design anchor narrative in four parts. Use plain, practitioner-friendly language:

1. **What it was designed for** — Based on the academic corpus, what analytic tasks
   or insight needs does this visualization technique address? What makes it valuable
   in a research context?

2. **How it is typically used in practice** — Based on the repository evidence,
   how do practitioners actually use this chart type? What contexts is it deployed in?

3. **What is commonly lost in adoption** — What design elements, interactions,
   or analytic depth are frequently missing in practice? Be specific.

4. **Guidance for practitioners** — One or two concrete recommendations for
   someone implementing this visualization type who wants to preserve its
   original analytical value.

Write as continuous prose, 150-250 words total. No headers, no bullet points."""
