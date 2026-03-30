"""
Prompts for multimodal drift annotation (Component 8 — revised).
Used by: tracing/management/commands/annotate_drift.py
Role: TRACE_ANNOTATE (Gemini, multimodal)

The model receives the actual figure image alongside the notebook code.
It first verifies whether the image genuinely shows the claimed vis_type,
then — only if valid — annotates drift across three dimensions.

No imports from any API library — pure strings/templates only.
"""


ANNOTATE_DRIFT_SYSTEM_PROMPT = (
    "You are a data visualization researcher specialising in design provenance. "
    "You will be shown a figure image from an academic paper and code from a "
    "repository notebook. "
    "Your task has two steps:\n\n"
    "STEP 1 — Verify the figure image. "
    "Decide whether the image genuinely shows the visualization type stated in "
    "the prompt. If it does not (e.g. it is a different chart type, a pipeline "
    "diagram, a screenshot, a photograph, or any other non-matching visual), "
    "set \"valid\": false and provide a short invalid_reason. "
    "Leave all drift fields as \"none\" with empty notes.\n\n"
    "STEP 2 — If valid is true, assess design drift across three dimensions:\n"
    "  encoding_drift   — visual marks, channels, axes, colour, layout\n"
    "  interaction_drift — brushing, linking, filtering, zoom, tooltips\n"
    "  task_drift       — analytic purpose (exploratory vs static reporting, etc.)\n"
    "For each dimension choose: none / minor / major.\n\n"
    "Respond with valid JSON only. No extra text, no markdown fences."
)


def annotate_drift_prompt(
    vis_type: str,
    paper_title: str,
    paper_abstract: str,
    paper_year: int,
    paper_track: str,
    paper_keywords: list[str],
    detected_libraries: list[str],
    detected_chart_types: list[str],
    notebook_code_excerpt: str,
    platform: str = "kaggle",
) -> str:
    """
    Build the user-turn prompt for multimodal drift annotation.

    The figure image is passed separately as image_path to call_llm() —
    this function builds only the text portion of the prompt.

    Args:
        vis_type:               Visualization type claimed for the figure.
        paper_title:            Academic paper title.
        paper_abstract:         Paper abstract.
        paper_year:             Publication year.
        paper_track:            IEEE VIS track (InfoVis, VAST, SciVis, unknown).
        paper_keywords:         Keywords extracted by the TEXT agent.
        detected_libraries:     Libraries found in the notebook.
        detected_chart_types:   Chart types detected in the notebook.
        notebook_code_excerpt:  First ~200 lines of notebook code.
        platform:               kaggle | github | observablehq.

    Expected JSON response shape:
        {
          "valid": true | false,
          "invalid_reason": "<why the image does not show vis_type, or empty>",
          "encoding":          "none|minor|major",
          "interaction":       "none|minor|major",
          "task":              "none|minor|major",
          "encoding_notes":    "<one sentence>",
          "interaction_notes": "<one sentence>",
          "task_notes":        "<one sentence>"
        }
    """
    _PLATFORM_LABELS = {
        "kaggle":       "Kaggle Python notebook",
        "github":       "GitHub Python notebook",
        "observablehq": "Observable HQ JavaScript notebook (D3.js)",
    }
    platform_label = _PLATFORM_LABELS.get(platform, f"{platform} notebook")
    code_lang = "javascript" if platform == "observablehq" else "python"
    keywords_str = ", ".join(paper_keywords) if paper_keywords else "none extracted"
    libraries_str = ", ".join(detected_libraries) if detected_libraries else "unknown"
    repo_types_str = ", ".join(detected_chart_types) if detected_chart_types else "unknown"

    return f"""The attached image is a figure from an academic paper.
The paper claims this figure shows a: {vis_type}

## STEP 1 — VERIFY THE IMAGE
Look at the attached image carefully. Does it genuinely show a {vis_type} visualization?
Set "valid": true only if the image clearly is a {vis_type}.
Set "valid": false if it is a different chart type, a system diagram, a screenshot,
a photograph, an equation, a table, or any other non-{vis_type} visual.
If false, explain briefly in "invalid_reason" what the image actually shows.

## STEP 2 — DRIFT ANNOTATION (only if valid=true)

### ACADEMIC SIDE (IEEE VIS {paper_year}, {paper_track} track)
Title: {paper_title}
Visualization type: {vis_type}
Keywords: {keywords_str}

Abstract:
{paper_abstract}

### REPOSITORY SIDE ({platform_label})
Libraries used: {libraries_str}
Chart types detected: {repo_types_str}

Code excerpt (first ~200 lines, language: {code_lang}):
```{code_lang}
{notebook_code_excerpt[:3000]}
```

Assess drift across three dimensions. For each: none / minor / major.

- encoding_drift: Are visual marks, channels, axes, colour scales, or layout
  simplified or altered vs what the academic design requires?

- interaction_drift: Does the repository lack interactivity the academic design
  relies on? (brushing, filtering, linked views, tooltips, zoom, etc.)

- task_drift: Has the analytic purpose shifted?
  (e.g. exploratory analysis → static reporting; comparison → distribution)

Respond with valid JSON only:
{{
  "valid": true,
  "invalid_reason": "",
  "encoding":          "none|minor|major",
  "interaction":       "none|minor|major",
  "task":              "none|minor|major",
  "encoding_notes":    "<one sentence explaining encoding drift>",
  "interaction_notes": "<one sentence explaining interaction drift>",
  "task_notes":        "<one sentence explaining task drift>"
}}"""