"""
Prompts and patterns for repository chart type detection (Component 6).
Used by: repository/management/commands/detect_chart_types.py
Role IMAGE:         Method B — output image classification fallback
Role DETECT_CHARTS: Method A+ — LLM code analysis when pattern matching fails

No imports from any API library — pure strings/templates only.
"""

from core.prompts.classify_figure import (  # noqa: F401  (re-export)
    RELEVANCE_PROMPT,
    RELEVANCE_SYSTEM_PROMPT,
    TYPE_CLASSIFICATION_SYSTEM_PROMPT,
    type_classification_prompt,
)

# ── Method A — Code pattern matching ──────────────────────────────────────────
# THREE tiers:
#   STRONG_PATTERNS  — highly specific, low false-positive risk → always used
#   WEAK_PATTERNS    — broad calls that appear in many chart types → only used
#                      when STRONG_PATTERNS returns nothing
#   LIBRARY_IMPORT_PATTERNS — detect which libraries are present
#
# Format: (library_hint, code_substring, vis_type)
# vis_type strings MUST be valid taxonomy values from core.taxonomy.VIS_TYPES.

STRONG_PATTERNS: list[tuple[str, str, str]] = [
    # ── seaborn ───────────────────────────────────────────────────────────────
    ("seaborn", "sns.barplot(",           "Bar"),
    ("seaborn", "sns.countplot(",         "Bar"),
    ("seaborn", "sns.histplot(",          "Histogram"),
    ("seaborn", "sns.kdeplot(",           "Area"),
    ("seaborn", "sns.scatterplot(",       "Scatter"),
    ("seaborn", "sns.regplot(",           "Scatter"),
    ("seaborn", "sns.lineplot(",          "Line"),
    ("seaborn", "sns.heatmap(",           "Heatmap"),
    ("seaborn", "sns.clustermap(",        "Heatmap"),
    ("seaborn", "sns.violinplot(",        "Glyph"),
    ("seaborn", "sns.boxplot(",           "Glyph"),
    ("seaborn", "sns.pairplot(",          "Small Multiples"),
    ("seaborn", "sns.jointplot(",         "Scatter"),

    # ── matplotlib — specific ─────────────────────────────────────────────────
    ("matplotlib", "plt.bar(",            "Bar"),
    ("matplotlib", "plt.barh(",           "Bar"),
    ("matplotlib", "ax.bar(",             "Bar"),
    ("matplotlib", "ax.barh(",            "Bar"),
    ("matplotlib", "plt.scatter(",        "Scatter"),
    ("matplotlib", "ax.scatter(",         "Scatter"),
    ("matplotlib", "plt.hist(",           "Histogram"),
    ("matplotlib", "ax.hist(",            "Histogram"),
    ("matplotlib", "plt.pie(",            "Other"),
    ("matplotlib", "ax.pie(",             "Other"),
    ("matplotlib", "plt.stackplot(",      "Stacked Area"),
    ("matplotlib", "plt.fill_between(",   "Area"),
    # imshow only counts as Heatmap when colorbar is also present
    # (plain imshow is usually a photo/array display, not a data viz)

    # ── plotly express ────────────────────────────────────────────────────────
    ("plotly", "px.bar(",                 "Bar"),
    ("plotly", "px.histogram(",           "Histogram"),
    ("plotly", "px.scatter(",             "Scatter"),
    ("plotly", "px.scatter_3d(",          "Scatter"),
    ("plotly", "px.line(",                "Line"),
    ("plotly", "px.area(",                "Area"),
    ("plotly", "px.choropleth(",          "Choropleth"),
    ("plotly", "px.choropleth_mapbox(",   "Choropleth"),
    ("plotly", "px.scatter_mapbox(",      "Dot Map"),
    ("plotly", "px.scatter_geo(",         "Dot Map"),
    ("plotly", "px.parallel_coordinates(","Parallel Coordinates"),
    ("plotly", "px.parallel_categories(", "Parallel Coordinates"),
    ("plotly", "px.sunburst(",            "Sunburst"),
    ("plotly", "px.treemap(",             "Treemap"),
    ("plotly", "px.icicle(",              "Icicle"),
    ("plotly", "px.funnel(",              "Sankey"),
    ("plotly", "px.density_heatmap(",     "Heatmap"),
    ("plotly", "px.imshow(",              "Heatmap"),  # px.imshow IS data viz
    ("plotly", "px.strip(",               "Glyph"),
    ("plotly", "px.violin(",              "Glyph"),
    ("plotly", "px.box(",                 "Glyph"),

    # ── plotly graph objects ───────────────────────────────────────────────────
    ("plotly", "go.Bar(",                 "Bar"),
    ("plotly", "go.Histogram(",           "Histogram"),
    ("plotly", "go.Scatter(",             "Scatter"),
    ("plotly", "go.Scatter3d(",           "Scatter"),
    ("plotly", "go.Scattermapbox(",       "Dot Map"),
    ("plotly", "go.Scattergeo(",          "Dot Map"),
    ("plotly", "go.Choropleth(",          "Choropleth"),
    ("plotly", "go.Heatmap(",             "Heatmap"),
    ("plotly", "go.Sankey(",              "Sankey"),
    ("plotly", "go.ParallelCoordinates(", "Parallel Coordinates"),
    ("plotly", "go.ParallelCategories(",  "Parallel Coordinates"),
    ("plotly", "go.Treemap(",             "Treemap"),
    ("plotly", "go.Sunburst(",            "Sunburst"),
    ("plotly", "go.Funnel(",              "Sankey"),
    ("plotly", "go.Box(",                 "Glyph"),
    ("plotly", "go.Violin(",              "Glyph"),

    # ── bokeh ─────────────────────────────────────────────────────────────────
    ("bokeh", ".vbar(",                   "Bar"),
    ("bokeh", ".hbar(",                   "Bar"),
    ("bokeh", ".vbar_stack(",             "Stacked Bar"),
    ("bokeh", ".hbar_stack(",             "Stacked Bar"),
    ("bokeh", ".scatter(",                "Scatter"),
    ("bokeh", ".circle(",                 "Scatter"),
    ("bokeh", ".line(",                   "Line"),
    ("bokeh", ".multi_line(",             "Line"),
    ("bokeh", "quad(",                    "Histogram"),
    ("bokeh", ".image(",                  "Heatmap"),
    ("bokeh", ".image_rgba(",             "Heatmap"),
    ("bokeh", "from bokeh.models import Sankey", "Sankey"),

    # ── altair ────────────────────────────────────────────────────────────────
    ("altair", "mark_bar(",               "Bar"),
    ("altair", "mark_point(",             "Scatter"),
    ("altair", "mark_circle(",            "Scatter"),
    ("altair", "mark_line(",              "Line"),
    ("altair", "mark_area(",              "Area"),
    ("altair", "mark_rect(",              "Heatmap"),
    ("altair", "mark_boxplot(",           "Glyph"),
    ("altair", "mark_trail(",             "Line"),

    # ── pandas .plot() API ────────────────────────────────────────────────────
    # These handle the most common Kaggle pattern: df.plot.bar(), df.hist() etc.
    ("pandas", ".plot.bar(",              "Bar"),
    ("pandas", ".plot.barh(",             "Bar"),
    ("pandas", "kind='bar'",             "Bar"),
    ("pandas", 'kind="bar"',             "Bar"),
    ("pandas", ".plot.hist(",             "Histogram"),
    ("pandas", ".hist(",                  "Histogram"),
    ("pandas", "kind='hist'",            "Histogram"),
    ("pandas", 'kind="hist"',            "Histogram"),
    ("pandas", ".plot.scatter(",          "Scatter"),
    ("pandas", "kind='scatter'",         "Scatter"),
    ("pandas", 'kind="scatter"',         "Scatter"),
    ("pandas", ".plot.line(",             "Line"),
    ("pandas", "kind='line'",            "Line"),
    ("pandas", 'kind="line"',            "Line"),
    ("pandas", ".plot.area(",             "Area"),
    ("pandas", "kind='area'",            "Area"),
    ("pandas", 'kind="area"',            "Area"),
    ("pandas", ".plot.pie(",              "Other"),
    ("pandas", "kind='pie'",             "Other"),
    ("pandas", 'kind="pie"',             "Other"),
    ("pandas", ".plot.box(",              "Glyph"),
    ("pandas", "kind='box'",             "Glyph"),
    ("pandas", 'kind="box"',             "Glyph"),
    ("pandas", ".plot.kde(",              "Area"),
    ("pandas", "kind='kde'",             "Area"),
    ("pandas", ".value_counts().plot(",   "Bar"),

    # ── networkx ──────────────────────────────────────────────────────────────
    ("networkx", "nx.draw(",              "Node-Link"),
    ("networkx", "nx.draw_networkx(",     "Node-Link"),
    ("networkx", "nx.draw_spring(",       "Node-Link"),
    ("networkx", "nx.draw_circular(",     "Node-Link"),
    ("networkx", "nx.draw_kamada_kawai(", "Node-Link"),
    ("networkx", "nx.draw_spectral(",     "Node-Link"),

    # ── sklearn / confusion matrix ────────────────────────────────────────────
    ("sklearn", "ConfusionMatrixDisplay(","Confusion Matrix"),
    ("sklearn", "confusion_matrix_display","Confusion Matrix"),
    ("sklearn", "plot_confusion_matrix(", "Confusion Matrix"),

    # ── wordcloud ─────────────────────────────────────────────────────────────
    ("wordcloud", "WordCloud(",           "Other"),
    ("wordcloud", "wordcloud.generate(",  "Other"),

    # ── D3 (JavaScript notebooks — Observable HQ and GitHub JS files) ──────────
    ("d3", "d3.chord(",                   "Chord Diagram"),
    ("d3", "d3.ribbon(",                  "Chord Diagram"),
    ("d3", "d3.hierarchy(",               "Dendrogram"),
    ("d3", "d3.tree(",                    "Dendrogram"),
    ("d3", "d3.cluster(",                 "Dendrogram"),
    ("d3", "d3.treemap(",                 "Treemap"),
    ("d3", "d3.pack(",                    "Treemap"),
    ("d3", "d3.partition(",               "Sunburst"),
    ("d3", "d3.forceSimulation(",         "Node-Link"),
    ("d3", "d3.forceLink(",               "Node-Link"),
    ("d3", "d3.sankey(",                  "Sankey"),
    ("d3", "d3.geoPath(",                 "Choropleth"),
    ("d3", "d3.geoProjection(",           "Choropleth"),
    ("d3", "d3.stack(",                   "Stacked Area"),
    ("d3", "d3.scaleBand(",               "Bar"),
    ("d3", "d3.line(",                    "Line"),
    ("d3", "d3.area(",                    "Area"),
    ("d3", "d3.contours(",                "Heatmap"),
    ("d3", "d3.hexbin(",                  "Heatmap"),
    ("d3", "d3.brush(",                   "Scatter"),
    ("d3", "d3.brushX(",                  "Scatter"),
    ("d3", "d3.symbol(",                  "Glyph"),
    ("d3", "d3.pie(",                     "Other"),
    ("d3", "d3.arc(",                     "Other"),
]

# Weak patterns — only used when STRONG_PATTERNS finds nothing.
# These are valid patterns but appear in too many contexts to be trusted alone.
WEAK_PATTERNS: list[tuple[str, str, str]] = [
    ("matplotlib", "plt.plot(",           "Line"),
    ("matplotlib", "ax.plot(",            "Line"),
    ("matplotlib", "plt.imshow(",         "Heatmap"),  # only if plt.colorbar( also present
    ("pandas",     ".plot(",              "Line"),     # df.plot() with no kind= → defaults to line
]

# Alias-safe library detection: if none of the standard aliases are found
# but the library IS imported under another name, we still detect it.
LIBRARY_IMPORT_PATTERNS: list[tuple[str, str]] = [
    ("matplotlib", "import matplotlib"),
    ("matplotlib", "from matplotlib"),
    ("seaborn",    "import seaborn"),
    ("seaborn",    "from seaborn"),
    ("plotly",     "import plotly"),
    ("plotly",     "from plotly"),
    ("bokeh",      "import bokeh"),
    ("bokeh",      "from bokeh"),
    ("altair",     "import altair"),
    ("altair",     "from altair"),
    ("pandas",     "import pandas"),
    ("pandas",     "from pandas"),
    ("networkx",   "import networkx"),
    ("networkx",   "from networkx"),
    ("sklearn",    "from sklearn"),
    ("sklearn",    "import sklearn"),
    ("wordcloud",  "from wordcloud"),
    ("wordcloud",  "import wordcloud"),
    ("d3",         '"d3"'),
    ("d3",         "'d3'"),
    ("d3",         "require('d3')"),
    ("d3",         'require("d3")'),
    ("d3",         "d3."),             # Observable compiled JS: d3. appears in function bodies
]

# ── Method A+ system prompt (DETECT_CHARTS role / kimi-k2) ────────────────────

DETECT_CHARTS_SYSTEM_PROMPT = """\
You are an expert at reading data science code and identifying what types of \
data visualizations are being created. You understand Python plotting libraries \
(matplotlib, seaborn, plotly, bokeh, altair, pandas .plot(), networkx), \
JavaScript D3.js, and R ggplot2.

Respond ONLY with a valid JSON object. No explanation, no markdown fences.\
"""


def detect_charts_prompt(code_excerpt: str, vis_types: list[str]) -> str:
    """
    Method A+ prompt — sent to DETECT_CHARTS role when simple pattern matching
    finds nothing. Asks the model to read the code and identify chart types.

    Args:
        code_excerpt: Raw code from the notebook (first ~150 lines of code cells).
        vis_types:    The controlled vocabulary list from core.taxonomy.VIS_TYPES.

    Returns:
        Prompt string. Expected response: JSON with a "chart_types" key.
    """
    types_str = ", ".join(f'"{t}"' for t in vis_types)
    return f"""\
Examine the following code from a Jupyter notebook and identify what data \
visualizations it produces.

VALID CHART TYPES (use ONLY these exact strings):
{types_str}

Return a JSON object with this exact structure:
{{
  "chart_types": ["Type1", "Type2"],
  "confidence": "high" | "medium" | "low",
  "reasoning": "one sentence explanation"
}}

Rules:
- chart_types must be a list of strings from the VALID CHART TYPES above.
- Return an empty list [] if no data visualization is produced.
- Do NOT invent new type names. Pick the closest match from the list.
- "plt.plot()" used only as a reference/trend line does NOT count as a Line chart.
- "plt.imshow()" displaying a photo or array does NOT count as a Heatmap.
- Only include a type if you are confident the notebook genuinely creates that chart.

CODE:
```python
{code_excerpt[:4000]}
```"""