"""
Text-to-SQL → Vega-Altair chart pipeline for custom narrative charts.

Public function: generate_chart_from_nl(nl_prompt, vis_type) → image block dict

Three-step pipeline:
  1. QUERY model: NL + full schema → SELECT SQL
  2. Validate SQL (SELECT-only, allowed tables, no dangerous tokens)
  3. Execute against the live SQLite DB → rows (capped at 500)
  4. QUERY model: rows + original NL → Altair Python code
  5. exec() the code in a sandboxed namespace → capture `chart` object
  6. Render to PNG via vl-convert → base64 encode → image block

Returns an `image` block (not a `chart` block) so the publisher and
author template render it as a static <img> rather than a Plotly div.
"""

from __future__ import annotations

import base64
import logging
import re
import sqlite3
import uuid

from django.conf import settings

from core.llm_client import call_llm
from core.prompts.chart_query import (
    SQL_SYSTEM,
    ALTAIR_SYSTEM,
    sql_prompt,
    altair_prompt,
)

logger = logging.getLogger(__name__)

# ── Safety allowlist ───────────────────────────────────────────────────────────

ALLOWED_TABLES = {
    "paper_figures",
    "papers",
    "drift_annotations",
    "traces",
    "repo_artifacts",
    "repo_sources",
    "narratives",
    # SQLite table-valued functions used in JSON queries
    "json_each",
    "json_tree",
}

_BLOCKED_TOKENS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|ATTACH|DETACH|PRAGMA|"
    r"VACUUM|REINDEX|ANALYZE|REPLACE|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

_MULTI_STATEMENT = re.compile(r";.+", re.DOTALL)

MAX_ROWS = 500


# ── Validation ─────────────────────────────────────────────────────────────────

class SQLValidationError(ValueError):
    pass


def _validate_sql(sql: str) -> str:
    sql = sql.strip()
    if sql.startswith("```"):
        lines = sql.splitlines()
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        sql = "\n".join(inner).strip()

    sql_stripped = sql.rstrip(";").strip()
    if _MULTI_STATEMENT.search(sql_stripped):
        raise SQLValidationError("Multiple statements are not allowed")

    if _BLOCKED_TOKENS.search(sql_stripped):
        raise SQLValidationError("Query contains a disallowed SQL keyword")

    upper = sql_stripped.upper().lstrip()
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        raise SQLValidationError("Only SELECT queries are allowed")

    referenced = set(re.findall(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        sql_stripped,
        re.IGNORECASE,
    ))
    disallowed = referenced - ALLOWED_TABLES
    if disallowed:
        raise SQLValidationError(
            f"Query references disallowed table(s): {disallowed}"
        )

    if not re.search(r"\bLIMIT\b", sql_stripped, re.IGNORECASE):
        sql_stripped += f" LIMIT {MAX_ROWS}"

    return sql_stripped


# ── Execution ──────────────────────────────────────────────────────────────────

def _execute_sql(sql: str) -> tuple[list[str], list[list]]:
    db_path = str(settings.DATABASES["default"]["NAME"])
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        cursor = conn.execute(sql)
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = [list(row) for row in cursor.fetchmany(MAX_ROWS)]
        return columns, rows
    finally:
        conn.close()


# ── Altair render ──────────────────────────────────────────────────────────────

def _render_altair_to_png_b64(code: str) -> str:
    """
    Execute Altair Python code, capture the `chart` variable,
    render to PNG via vl-convert, return base64 data URI.
    """
    import altair as alt
    import pandas as pd
    import vl_convert as vlc

    # Strip markdown fences the model may have added despite instructions
    code = code.strip()
    if code.startswith("```"):
        lines = code.splitlines()
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        code = "\n".join(inner).strip()

    namespace: dict = {"alt": alt, "pd": pd}
    exec(code, namespace)  # noqa: S102

    chart = namespace.get("chart")
    if chart is None:
        raise RuntimeError("Altair code did not assign a variable named `chart`")

    png_bytes = vlc.vegalite_to_png(chart.to_json(), scale=2)
    b64 = base64.b64encode(png_bytes).decode("utf-8")
    return f"data:image/png;base64,{b64}"


# ── Public entry point ─────────────────────────────────────────────────────────

def generate_chart_from_nl(nl_request: str, vis_type: str) -> dict:
    """
    Full pipeline: NL → SQL → execute → Altair code → PNG → image block.

    Returns an image block dict:
      { "uuid": "...", "type": "image", "prompt": "...", "image_b64": "data:image/png;base64,..." }

    On any failure returns a block with empty image_b64 and an _error field.
    """
    block_uuid = str(uuid.uuid4())

    def error_block(msg: str) -> dict:
        return {
            "uuid": block_uuid,
            "type": "image",
            "prompt": nl_request,
            "image_b64": "",
            "_error": msg,
        }

    # ── Step 1: NL → SQL ──────────────────────────────────────────────────────
    try:
        raw_sql = str(call_llm(
            role="QUERY",
            prompt=sql_prompt(nl_request, vis_type),
            system_prompt=SQL_SYSTEM,
            response_format="text",
        ))
    except Exception as exc:
        logger.error("chart_query: SQL generation failed: %s", exc)
        return error_block(f"SQL generation failed: {exc}")

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    try:
        safe_sql = _validate_sql(raw_sql)
    except SQLValidationError as exc:
        logger.warning("chart_query: SQL validation failed: %s\nSQL was: %s", exc, raw_sql)
        return error_block(f"SQL validation failed: {exc}")

    logger.debug("chart_query: executing SQL: %s", safe_sql)

    # ── Step 3: Execute ───────────────────────────────────────────────────────
    try:
        columns, rows = _execute_sql(safe_sql)
    except sqlite3.Error as exc:
        logger.error("chart_query: SQL execution failed: %s\nSQL was: %s", exc, safe_sql)
        return error_block(f"SQL execution failed: {exc}")

    if not rows:
        logger.info("chart_query: query returned no rows for prompt: %s", nl_request)
        return error_block("Query returned no rows — try rephrasing the chart request")

    # ── Step 4: Rows → Altair Python code ────────────────────────────────────
    try:
        altair_code = str(call_llm(
            role="QUERY",
            prompt=altair_prompt(nl_request, columns, rows),
            system_prompt=ALTAIR_SYSTEM,
            response_format="text",
        ))
    except Exception as exc:
        logger.error("chart_query: Altair code generation failed: %s", exc)
        return error_block(f"Altair code generation failed: {exc}")

    # ── Step 5: exec → PNG → base64 ───────────────────────────────────────────
    try:
        image_b64 = _render_altair_to_png_b64(altair_code)
    except Exception as exc:
        logger.error("chart_query: Altair render failed: %s\nCode was:\n%s", exc, altair_code)
        return error_block(f"Altair render failed: {exc}")

    return {
        "uuid": block_uuid,
        "type": "image",
        "prompt": nl_request,
        "image_b64": image_b64,
        "_sql": safe_sql,
        "_row_count": len(rows),
    }