"""
VisImages taxonomy — Deng et al. (2022).
12 top-level categories, 30 subtypes.
This is the controlled vocabulary used throughout the system for vis_type fields.
"""

import re

# Top-level categories (used for display and grouping)
TOP_LEVEL_CATEGORIES = [
    "Bar",
    "Line",
    "Scatter",
    "Area",
    "Map",
    "Network/Graph",
    "Tree/Hierarchy",
    "Matrix/Heatmap",
    "Parallel Coordinates",
    "Glyph",
    "Flow",
    "Other/Composite",
]

# Full taxonomy with subtypes
# Format: { "category": ["subtype1", "subtype2", ...] }
TAXONOMY = {
    "Bar": [
        "Bar",
        "Grouped Bar",
        "Stacked Bar",
        "Histogram",
    ],
    "Line": [
        "Line",
        "Multi-Line",
        "Stream Graph",
    ],
    "Scatter": [
        "Scatter",
        "Bubble",
        "Dot Plot",
    ],
    "Area": [
        "Area",
        "Stacked Area",
    ],
    "Map": [
        "Choropleth",
        "Dot Map",
        "Flow Map",
        "Cartogram",
    ],
    "Network/Graph": [
        "Node-Link",
        "Adjacency Matrix",
        "Arc Diagram",
        "Chord Diagram",
    ],
    "Tree/Hierarchy": [
        "Treemap",
        "Sunburst",
        "Dendrogram",
        "Icicle",
    ],
    "Matrix/Heatmap": [
        "Heatmap",
        "Confusion Matrix",
    ],
    "Parallel Coordinates": [
        "Parallel Coordinates",
    ],
    "Glyph": [
        "Glyph",
        "Radar/Spider",
        "Star Plot",
    ],
    "Flow": [
        "Sankey",
        "Alluvial",
    ],
    "Other/Composite": [
        "Small Multiples",
        "Composite/Dashboard",
        "Other",
    ],
}

# Flat list of all subtypes — used when prompting models to classify a figure
VIS_TYPES: list[str] = []
for _category, _subtypes in TAXONOMY.items():
    VIS_TYPES.extend(_subtypes)

# Mapping subtype → parent category — used for parent-category matching in trace building
SUBTYPE_TO_CATEGORY: dict[str, str] = {}
for _category, _subtypes in TAXONOMY.items():
    for _subtype in _subtypes:
        SUBTYPE_TO_CATEGORY[_subtype] = _category


def vis_type_to_slug(vis_type: str) -> str:
    """Convert a vis_type label to the canonical narrative URL slug."""
    return re.sub(r"[^a-z0-9]+", "-", vis_type.lower()).strip("-")


SLUG_TO_VIS_TYPE: dict[str, str] = {vis_type_to_slug(v): v for v in VIS_TYPES}


# Mapping from VisImages annotation.json type strings → our taxonomy subtypes.
# None means "not a visualization" — these records get is_visualization=False.
VISIMAGES_TYPE_MAP: dict[str, str | None] = {
    "bar_chart":                  "Bar",
    "scatterplot":                "Scatter",
    "line_chart":                 "Line",
    "area_chart":                 "Area",
    "heatmap":                    "Heatmap",
    "matrix":                     "Heatmap",
    "map":                        "Choropleth",
    "graph":                      "Node-Link",
    "tree":                       "Dendrogram",        # category=Tree/Hierarchy; Dendrogram is closest generic
    "treemap":                    "Treemap",
    "sunburst_icicle":            "Sunburst",
    "parallel_coordinate":        "Parallel Coordinates",
    "sankey_diagram":             "Sankey",
    "flow_diagram":               "Sankey",           # category=Flow; Sankey is the dominant subtype
    "chord_diagram":              "Chord Diagram",
    "hierarchical_edge_bundling": "Node-Link",
    "small_multiple":             "Small Multiples",
    "glyph_based":                "Glyph",
    "box_plot":                   "Glyph",
    "error_bar":                  "Glyph",
    "polar_plot":                 "Radar/Spider",
    "donut_chart":                "Other",            # category=Other/Composite; subtype=Other
    "pie_chart":                  "Other",
    "proportional_area_chart":    "Other",
    "sector_chart":               "Other",
    "stripe_graph":               "Other",
    "unit_visualization":         "Other",
    "word_cloud":                 "Other",
    "storyline":                  "Other",
    "table":                      None,               # not a visualization — filter out
}


def get_category(vis_type: str) -> str | None:
    """Return the parent category for a vis_type string, or None if not found."""
    return SUBTYPE_TO_CATEGORY.get(vis_type)


def resolve_vis_type(value: str) -> str | None:
    """Resolve either an exact vis_type label or its slug to the canonical label."""
    candidate = str(value or "").strip()
    if not candidate:
        return None
    if candidate in SUBTYPE_TO_CATEGORY:
        return candidate
    return SLUG_TO_VIS_TYPE.get(vis_type_to_slug(candidate))


def types_in_same_category(type_a: str, type_b: str) -> bool:
    """Return True if two vis_type strings share the same parent category."""
    cat_a = get_category(type_a)
    cat_b = get_category(type_b)
    return cat_a is not None and cat_a == cat_b
