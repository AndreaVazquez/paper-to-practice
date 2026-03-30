"""
Prompts for the two-step Text-to-SQL → Vega-Altair chart pipeline.

Step 1 (SQL_SYSTEM / sql_prompt):
  NL chart description → SELECT SQL against the full schema.

Step 2 (ALTAIR_SYSTEM / altair_prompt):
  Actual query result rows + original NL description → Altair Python code
  that produces a chart object named `chart`. The code is exec()'d
  server-side and rendered to PNG via vl-convert.

No imports from any API library — pure strings/templates only.
"""

# ── Full schema exposed to the model ──────────────────────────────────────────

SCHEMA = """
TABLE paper_figures
  id                   INTEGER  PRIMARY KEY
  paper_id             INTEGER  FK → papers.id
  figure_index         INTEGER  position in PDF
  is_visualization     INTEGER  1=yes 0=no NULL=unclassified
  vis_type             TEXT     e.g. "Bar", "Scatter", "Heatmap", "Node-Link", "Parallel Coordinates", ...
  vis_type_confidence  REAL     0.0–1.0 (1.0 = VisImages ground truth, ~0.91 = LLM classified)
  annotation_source    TEXT     visimages_json | llm_classified | manual

TABLE papers
  id                   INTEGER  PRIMARY KEY
  source               TEXT     visimages | vis2019 | vis2020 | vis2021 | vis2022 | vis2023 | vis2024 | vis2025 | seed_doi
  doi                  TEXT     DOI string (may be NULL)
  title                TEXT     full paper title
  authors              TEXT     JSON array of author name strings
  year                 INTEGER  publication year (1995–2026)
  track                TEXT     InfoVis | VAST | SciVis | unknown
  abstract             TEXT     paper abstract
  keywords_extracted   TEXT     JSON array of keywords extracted by LLM
  topics_extracted     TEXT     JSON array of high-level topics extracted by LLM

TABLE drift_annotations
  id                   INTEGER  PRIMARY KEY
  trace_id             INTEGER  FK → traces.id
  encoding_drift       TEXT     none | minor | major
  interaction_drift    TEXT     none | minor | major
  task_drift           TEXT     none | minor | major
  encoding_notes       TEXT     free-text explanation of encoding drift
  interaction_notes    TEXT     free-text explanation of interaction drift
  task_notes           TEXT     free-text explanation of task drift
  annotated_by         TEXT     llm | manual
  annotated_at         TEXT     ISO timestamp

TABLE traces
  id                   INTEGER  PRIMARY KEY
  figure_id            INTEGER  FK → paper_figures.id
  artifact_id          INTEGER  FK → repo_artifacts.id
  match_method         TEXT     chart_type_match | keyword_match
  match_confidence     REAL     0.0–1.0 (>=0.8 = auto-verified)
  verified             INTEGER  1=yes 0=no

TABLE repo_artifacts
  id                   INTEGER  PRIMARY KEY
  source_id            INTEGER  FK → repo_sources.id
  artifact_type        TEXT     notebook | script | markdown
  detected_chart_types TEXT     JSON array e.g. '["Bar","Scatter"]'
  detected_libraries   TEXT     JSON array e.g. '["matplotlib","seaborn","plotly"]'
  detection_method     TEXT     code_analysis | llm_code_analysis | weak_pattern

TABLE repo_sources
  id                   INTEGER  PRIMARY KEY
  platform             TEXT     kaggle | github
  source_id            TEXT     kaggle kernel ref or github full_name
  url                  TEXT     public URL
  title                TEXT     notebook/repo title
  author               TEXT     uploader username
  stars                INTEGER  kaggle votes or github stars
  language             TEXT     programming language
  last_updated         TEXT     ISO date
"""

# ── Step 1 — SQL generation ────────────────────────────────────────────────────

SQL_SYSTEM = """\
You are a SQLite query writer for a visualization research database.
Your job: translate a natural-language chart request into a single SELECT query.

Rules:
- Return ONLY the SQL statement, no markdown fences, no explanation.
- Only SELECT is allowed. No INSERT, UPDATE, DELETE, DROP, PRAGMA, ATTACH.
- Only use the tables and columns listed in the schema. No other tables exist.
- Always end the query with LIMIT 500.
- Avoid selecting raw surrogate id columns (id, paper_id, figure_id, trace_id,
  artifact_id, source_id) unless explicitly needed for a join — they have no
  meaning on a chart axis.
- For trend/distribution/frequency charts, SELECT the grouping dimension
  (e.g. year, vis_type, platform, track, encoding_drift) and an aggregate.
- Free-text columns (title, abstract, keywords_extracted, topics_extracted,
  encoding_notes, interaction_notes, task_notes, url, author) MAY be selected
  when the request explicitly asks to list, search, or sample them.
- When the request mentions time / year / trend, GROUP BY papers.year ORDER BY papers.year.
- Prefer 2–3 column results. Use GROUP BY + COUNT(*) or aggregates for
  distributions and frequencies.
- For vis_type context, filter to that vis_type unless the request explicitly
  asks for a cross-type comparison.
"""


def sql_prompt(nl_request: str, vis_type: str) -> str:
    return f"""\
Schema:
{SCHEMA}

Visualization type context: {vis_type}

Chart request:
{nl_request}

Write a single SELECT query that retrieves the data needed to draw this chart.
Return only the SQL."""


# ── Step 2 — Altair code generation ───────────────────────────────────────────

ALTAIR_SYSTEM = """\
You are a data visualisation engineer writing Vega-Altair Python code.
Given a query result (column names + rows) and a chart description,
write Python code that builds an Altair chart and assigns it to a variable
named exactly `chart`.

Rules:
- Import only: altair as alt, pandas as pd. No other imports.
- Build a DataFrame from the provided data — do NOT call any external APIs or files.
- Assign the final chart to `chart`. Do not call .show() or .save().
- Use a light background theme: set chart background to "#f8fafc",
  font color "#475569", gridline color "#e2e8f0".
- Choose the most appropriate mark for the data:
    category vs count        → mark_bar()
    year / time vs value     → mark_bar() or mark_line() with mark_point()
    two numeric columns      → mark_point() (scatter)
    many categories, ranking → mark_bar(orient horizontal)
    distribution             → mark_bar()
- Label axes using the column names. Add a descriptive title.
- Set width=520, height=300 (or height=30*len(rows) for horizontal bars, max 400).
- Use a tasteful color scheme: blues (#0e6090), purples (#6d28a8), greens (#156a3e).
- Return ONLY the Python code block, no markdown fences, no explanation.
- If data is empty, assign: chart = alt.Chart(pd.DataFrame()).mark_point()
"""


def altair_prompt(nl_request: str, columns: list[str], rows: list[list]) -> str:
    # Inline the data directly as Python literals for the model to use
    header = ", ".join(columns)
    sample = rows[:100]
    row_lines = [f"  {list(row)}" for row in sample]
    if len(rows) > 100:
        row_lines.append(f"  # ... {len(rows) - 100} more rows truncated")
    rows_repr = "[\n" + ",\n".join(row_lines) + "\n]"

    return f"""\
Original chart request:
{nl_request}

Column names: {header}

Data rows ({len(rows)} total):
{rows_repr}

Write Altair Python code that visualises this data.
Assign the final chart to `chart`.
Return only the Python code, no fences."""