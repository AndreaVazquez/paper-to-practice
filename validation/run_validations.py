#!/usr/bin/env python3
"""
===============================================================================
FROM PAPER TO PRACTICE — Statistical Validation Analysis
===============================================================================
Run AFTER completing the annotation (HTML tool or spreadsheets).

Usage (with HTML tool JSON export):
    python run_validations.py \
        --db db.sqlite3 \
        --annotations p2p_annotations_2026-03-26.json \
        --output validation_results.txt

Usage (with Excel spreadsheets):
    python run_validations.py \
        --db db.sqlite3 \
        --drift-sheet 01_drift_annotation_sheet.xlsx \
        --chart-sheet 02_chart_detection_sheet.xlsx \
        --output validation_results.txt

Produces: validation_results.txt with all statistics ready for the paper.
===============================================================================
"""

import sqlite3
import json
import argparse
import numpy as np
import pandas as pd
from scipy import stats


FAILURE_NOTE_PATTERNS = {
    "wrong_paper_figure": [
        "wrong paper/figure",
        "wrong paper",
        "wrong figure",
        "wrong picture",
    ],
    "mismatch": [
        "mismatch",
        "mistmatch",
        "mismacht",
        "mismacht",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# INPUT LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_from_json(path):
    """Load annotations from HTML tool JSON export."""
    with open(path) as f:
        data = json.load(f)
    annotator = data.get('annotator_id', 'unknown')
    drift_df = pd.DataFrame(data.get('drift', []))
    chart_df = pd.DataFrame(data.get('chart', []))
    return annotator, drift_df, chart_df


def load_from_excel(drift_path=None, chart_path=None):
    """Load annotations from Excel spreadsheets."""
    drift_df = None
    chart_df = None
    if drift_path:
        drift_df = pd.read_excel(drift_path, sheet_name="ANOTACIÓN_DRIFT")
        # Rename columns to match JSON format
        col_map = {
            'HUMAN_encoding': 'human_encoding',
            'HUMAN_interaction': 'human_interaction',
            'HUMAN_task': 'human_task',
            'HUMAN_encoding_notes': 'human_encoding_notes',
            'HUMAN_interaction_notes': 'human_interaction_notes',
            'HUMAN_task_notes': 'human_task_notes',
            'LLM_encoding': 'llm_encoding',
            'LLM_interaction': 'llm_interaction',
            'LLM_task': 'llm_task',
        }
        drift_df.rename(columns=col_map, inplace=True)
    if chart_path:
        chart_df = pd.read_excel(chart_path, sheet_name="CHART_DETECTION")
        col_map = {
            'HUMAN_verdict': 'human_verdict',
            'HUMAN_chart_types': 'human_charts',
            'PIPELINE_chart_types': 'pipeline_charts',
        }
        chart_df.rename(columns=col_map, inplace=True)
    return drift_df, chart_df


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def wilson_ci(p, n, z=1.96):
    """Wilson score interval for a proportion."""
    if n == 0:
        return 0, 0
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    spread = z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denom
    return max(0, centre - spread), min(1, centre + spread)


def _clean_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _classify_failure_notes(notes_text):
    for label, patterns in FAILURE_NOTE_PATTERNS.items():
        if any(pattern in notes_text for pattern in patterns):
            return label
    return ""


def build_annotation_status_df(df, prefix="human"):
    """
    Classify each trace as:
      - classified: at least one drift label present
      - failure_like: all labels blank and notes indicate annotation failure
      - blank_other: all labels blank but without explicit failure notes
    """
    cols = [
        f"{prefix}_encoding", f"{prefix}_interaction", f"{prefix}_task",
        f"{prefix}_encoding_notes", f"{prefix}_interaction_notes", f"{prefix}_task_notes",
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for {prefix}: {missing}")

    rows = []
    for _, row in df.iterrows():
        values = [_clean_text(row[f"{prefix}_{dim}"]) for dim in ("encoding", "interaction", "task")]
        notes = [_clean_text(row[f"{prefix}_{dim}_notes"]) for dim in ("encoding", "interaction", "task")]
        all_blank = all(v == "" for v in values)
        notes_text = " | ".join(n for n in notes if n)
        failure_type = _classify_failure_notes(notes_text) if all_blank else ""

        if all_blank and failure_type:
            status = "failure_like"
        elif all_blank:
            status = "blank_other"
        else:
            status = "classified"

        rows.append({
            "trace_id": row["trace_id"],
            "status": status,
            "failure_type": failure_type,
            "notes_text": notes_text,
        })

    return pd.DataFrame(rows)


def run_failure_summary(name, df, prefix="human"):
    """Summarize excluded / failure-like traces for one annotator."""
    status_df = build_annotation_status_df(df, prefix=prefix)
    n = len(status_df)
    n_classified = (status_df["status"] == "classified").sum()
    n_failure = (status_df["status"] == "failure_like").sum()
    n_blank_other = (status_df["status"] == "blank_other").sum()

    results = []
    results.append("\n" + "=" * 72)
    results.append(f"ANNOTATION COVERAGE / EXCLUDED TRACES ({name})")
    results.append("=" * 72)
    results.append(f"  Total traces:      {n}")
    results.append(f"  Classified:        {n_classified}")
    results.append(f"  Failure-like:      {n_failure}")
    results.append(f"  Blank other:       {n_blank_other}")

    if n > 0:
        results.append(f"  Classification coverage: {n_classified/n*100:.1f}%")
        results.append(f"  Explicit failures:       {n_failure/n*100:.1f}%")

    if n_failure > 0:
        results.append("\n  Failure types:")
        for ft, count in status_df.loc[status_df["status"] == "failure_like", "failure_type"].value_counts().items():
            label = ft.replace("_", " ")
            results.append(f"    {label:<20} {count:>4}")

    return "\n".join(results)


def run_failure_pairwise(name_x, name_y, df_x, df_y, prefix_x="human", prefix_y="human"):
    """Compare excluded / failure-like traces between two annotators."""
    from sklearn.metrics import cohen_kappa_score

    sx = build_annotation_status_df(df_x, prefix=prefix_x).rename(columns={
        "status": "status_x",
        "failure_type": "failure_type_x",
        "notes_text": "notes_text_x",
    })
    sy = build_annotation_status_df(df_y, prefix=prefix_y).rename(columns={
        "status": "status_y",
        "failure_type": "failure_type_y",
        "notes_text": "notes_text_y",
    })

    merged = sx.merge(sy, on="trace_id", how="inner")
    n = len(merged)
    both_failure = ((merged["status_x"] == "failure_like") & (merged["status_y"] == "failure_like")).sum()
    x_only_failure = ((merged["status_x"] == "failure_like") & (merged["status_y"] != "failure_like")).sum()
    y_only_failure = ((merged["status_x"] != "failure_like") & (merged["status_y"] == "failure_like")).sum()

    results = []
    results.append("\n" + "=" * 72)
    results.append(f"ANNOTATION FAILURES / EXCLUDED TRACES ({name_x} vs {name_y})")
    results.append("=" * 72)
    results.append(f"  Shared traces:         {n}")
    results.append(f"  {name_x} failure-like: {(merged['status_x'] == 'failure_like').sum()}")
    results.append(f"  {name_y} failure-like: {(merged['status_y'] == 'failure_like').sum()}")
    results.append(f"  Both failure-like:     {both_failure}")
    results.append(f"  {name_x} only:         {x_only_failure}")
    results.append(f"  {name_y} only:         {y_only_failure}")

    failure_union = both_failure + x_only_failure + y_only_failure
    if failure_union > 0:
        jaccard = both_failure / failure_union
        results.append(f"  Failure-trace Jaccard: {jaccard:.3f}")

    bx = (merged["status_x"] == "failure_like").astype(int).tolist()
    by = (merged["status_y"] == "failure_like").astype(int).tolist()
    if n >= 10 and len(set(bx + by)) > 1:
        results.append(f"  Binary κ on failure detection: {cohen_kappa_score(bx, by):.3f}")

    common = merged[(merged["status_x"] == "failure_like") & (merged["status_y"] == "failure_like")].copy()
    if len(common) > 0:
        subtype_match = (common["failure_type_x"] == common["failure_type_y"]).sum()
        results.append(f"  Failure subtype agreement: {subtype_match}/{len(common)} = {subtype_match/len(common)*100:.1f}%")
        results.append("\n  Failure subtype breakdown (common failures):")
        for ft, count in common["failure_type_x"].value_counts().items():
            label = ft.replace("_", " ")
            results.append(f"    {label:<20} {count:>4}")

    unresolved = merged[(merged["status_x"] == "blank_other") | (merged["status_y"] == "blank_other")]
    if len(unresolved) > 0:
        results.append(f"\n  Warning: {len(unresolved)} shared traces still have blank-without-reason annotations.")

    return "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION 1: Inter-Annotator Agreement (Human vs LLM)
# ─────────────────────────────────────────────────────────────────────────────

def run_ira_validation(drift_df):
    """Validation 1: Inter-annotator reliability."""
    from sklearn.metrics import cohen_kappa_score, confusion_matrix, classification_report

    results = []
    results.append("=" * 72)
    results.append("VALIDATION 1: INTER-ANNOTATOR AGREEMENT (Human vs LLM)")
    results.append("=" * 72)

    label_map = {"none": 0, "minor": 1, "major": 2}
    labels_order = ["none", "minor", "major"]
    
    dimensions = [("encoding", "human_encoding", "llm_encoding"),
                  ("interaction", "human_interaction", "llm_interaction"),
                  ("task", "human_task", "llm_task")]

    all_h, all_l = [], []

    for dim_name, h_col, l_col in dimensions:
        if h_col not in drift_df.columns or l_col not in drift_df.columns:
            results.append(f"\n--- {dim_name.upper()} DRIFT: columns missing ---")
            continue

        mask = drift_df[h_col].notna() & (drift_df[h_col] != '')
        human = drift_df.loc[mask, h_col].astype(str).str.strip().str.lower().tolist()
        llm = drift_df.loc[mask, l_col].astype(str).str.strip().str.lower().tolist()

        valid = [(h, l) for h, l in zip(human, llm)
                 if h in label_map and l in label_map]
        
        results.append(f"\n--- {dim_name.upper()} DRIFT ---")
        results.append(f"  N annotated: {len(valid)}")

        if len(valid) < 10:
            results.append(f"  INSUFFICIENT DATA. Need ≥10 valid pairs.")
            continue

        hv, lv = zip(*valid)
        all_h.extend(hv)
        all_l.extend(lv)

        h_num = [label_map[x] for x in hv]
        l_num = [label_map[x] for x in lv]

        k_uw = cohen_kappa_score(h_num, l_num)
        k_w = cohen_kappa_score(h_num, l_num, weights="linear")

        interp = ("almost perfect" if k_w >= 0.8 else "substantial" if k_w >= 0.6
                  else "moderate" if k_w >= 0.4 else "fair" if k_w >= 0.2 else "slight")

        results.append(f"  Cohen's κ (unweighted): {k_uw:.3f}")
        results.append(f"  Cohen's κ (linear weighted): {k_w:.3f}")
        results.append(f"  Interpretation (Landis & Koch): {interp}")

        cm = confusion_matrix(list(hv), list(lv), labels=labels_order)
        results.append(f"\n  Confusion Matrix (rows=Human, cols=LLM):")
        results.append(f"  {'':>8} {'none':>8} {'minor':>8} {'major':>8}")
        for i, label in enumerate(labels_order):
            results.append(f"  {label:>8} {cm[i][0]:>8} {cm[i][1]:>8} {cm[i][2]:>8}")

        report = classification_report(list(hv), list(lv), labels=labels_order,
                                       target_names=labels_order, zero_division=0)
        results.append(f"\n  Classification Report:\n{report}")

    # Aggregate
    if len(all_h) >= 30:
        results.append("\n--- AGGREGATE ---")
        h_num = [label_map[x] for x in all_h]
        l_num = [label_map[x] for x in all_l]
        results.append(f"  Aggregate κ (unweighted): {cohen_kappa_score(h_num, l_num):.3f}")
        results.append(f"  Aggregate κ (linear weighted): {cohen_kappa_score(h_num, l_num, weights='linear'):.3f}")
        results.append(f"  Total pairs: {len(all_h)}")

    return "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION 2: Confidence Intervals for Drift Metrics
# ─────────────────────────────────────────────────────────────────────────────

def run_ci_validation(db_path):
    """Validation 2: Confidence intervals for headline metrics."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT da.encoding_drift, da.interaction_drift, da.task_drift,
               pf.vis_type, p.track, rs.platform
        FROM drift_annotations da
        JOIN traces t ON da.trace_id = t.id
        JOIN paper_figures pf ON t.figure_id = pf.id
        JOIN papers p ON pf.paper_id = p.id
        JOIN repo_artifacts ra ON t.artifact_id = ra.id
        JOIN repo_sources rs ON ra.source_id = rs.id
        WHERE t.annotation_status = 'annotated'
    """, conn)
    conn.close()

    n_total = len(df)
    results = []
    results.append("\n" + "=" * 72)
    results.append("VALIDATION 2: CONFIDENCE INTERVALS FOR DRIFT METRICS")
    results.append("=" * 72)
    results.append(f"\nTotal annotated traces: {n_total}")

    results.append(f"\n--- HEADLINE METRICS (Wilson 95% CI) ---")
    for dim in ["encoding_drift", "interaction_drift", "task_drift"]:
        n_major = (df[dim] == "major").sum()
        n_minor = (df[dim] == "minor").sum()
        n_none = (df[dim] == "none").sum()
        p_major = n_major / n_total
        ci_lo, ci_hi = wilson_ci(p_major, n_total)
        dim_name = dim.replace("_drift", "").upper()
        results.append(f"\n  {dim_name} DRIFT:")
        results.append(f"    major: {n_major}/{n_total} = {p_major*100:.1f}% "
                       f"[{ci_lo*100:.1f}% – {ci_hi*100:.1f}%]")
        results.append(f"    minor: {n_minor}/{n_total} = {n_minor/n_total*100:.1f}%")
        results.append(f"    none:  {n_none}/{n_total} = {n_none/n_total*100:.1f}%")
        z_stat = (p_major - 0.5) / np.sqrt(0.5 * 0.5 / n_total)
        p_val = 1 - stats.norm.cdf(z_stat)
        results.append(f"    H0: p(major) ≤ 0.5, z = {z_stat:.2f}, p = {p_val:.2e}")

    # Interaction Cliff formal
    results.append(f"\n--- THE INTERACTION CLIFF ---")
    n_int = (df["interaction_drift"] == "major").sum()
    p_int = n_int / n_total
    ci_lo, ci_hi = wilson_ci(p_int, n_total)
    results.append(f"  {n_int}/{n_total} = {p_int*100:.1f}% [{ci_lo*100:.1f}% – {ci_hi*100:.1f}%]")

    # Bootstrap
    rng = np.random.default_rng(42)
    is_major = (df["interaction_drift"] == "major").values.astype(int)
    boot = [is_major[rng.choice(n_total, n_total, replace=True)].mean() for _ in range(10000)]
    results.append(f"  Bootstrap 95% CI: [{np.percentile(boot,2.5)*100:.1f}% – {np.percentile(boot,97.5)*100:.1f}%]")

    # By vis_type
    results.append(f"\n--- BY VIS_TYPE ---")
    results.append(f"  {'vis_type':<25} {'n':>4} {'major':>6} {'%':>7} {'95% CI':>18}")
    results.append(f"  {'-'*65}")
    for vt in sorted(df['vis_type'].unique()):
        sub = df[df['vis_type'] == vt]
        n_vt = len(sub)
        n_maj = (sub['interaction_drift'] == 'major').sum()
        p = n_maj / n_vt if n_vt > 0 else 0
        lo, hi = wilson_ci(p, n_vt)
        results.append(f"  {vt:<25} {n_vt:>4} {n_maj:>6} {p*100:>6.1f}% [{lo*100:.1f}% – {hi*100:.1f}%]")

    return "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION 3: Chi-squared Tests
# ─────────────────────────────────────────────────────────────────────────────

def run_chi2_validation(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT da.encoding_drift, da.interaction_drift, da.task_drift,
               pf.vis_type, p.track, rs.platform
        FROM drift_annotations da
        JOIN traces t ON da.trace_id = t.id
        JOIN paper_figures pf ON t.figure_id = pf.id
        JOIN papers p ON pf.paper_id = p.id
        JOIN repo_artifacts ra ON t.artifact_id = ra.id
        JOIN repo_sources rs ON ra.source_id = rs.id
        WHERE t.annotation_status = 'annotated'
    """, conn)
    conn.close()

    results = []
    results.append("\n" + "=" * 72)
    results.append("VALIDATION 3: CHI-SQUARED TESTS")
    results.append("=" * 72)

    # 3a: Independence between dimensions
    results.append(f"\n--- 3a: INDEPENDENCE BETWEEN DRIFT DIMENSIONS ---")
    for d1, d2 in [("encoding_drift","interaction_drift"),
                   ("encoding_drift","task_drift"),
                   ("interaction_drift","task_drift")]:
        ct = pd.crosstab(df[d1], df[d2])
        chi2, p, dof, _ = stats.chi2_contingency(ct)
        n = ct.values.sum()
        k = min(ct.shape) - 1
        v = np.sqrt(chi2 / (n * k)) if k > 0 else 0
        d1n = d1.replace("_drift","")
        d2n = d2.replace("_drift","")
        results.append(f"\n  {d1n} × {d2n}:")
        results.append(f"    {ct.to_string()}")
        results.append(f"    χ²={chi2:.2f}, df={dof}, p={p:.4e}, Cramér's V={v:.3f}")
        results.append(f"    → {'SIGNIFICANT' if p < 0.05 else 'Not significant'}")

    # 3b: By vis_type (group small)
    results.append(f"\n--- 3b: DRIFT BY VIS_TYPE ---")
    tc = df['vis_type'].value_counts()
    small = tc[tc < 10].index.tolist()
    dfg = df.copy()
    dfg.loc[dfg['vis_type'].isin(small), 'vis_type'] = '_Small'
    for dim in ["encoding_drift","interaction_drift","task_drift"]:
        ct = pd.crosstab(dfg['vis_type'], dfg[dim])
        chi2, p, dof, _ = stats.chi2_contingency(ct)
        n = ct.values.sum(); k = min(ct.shape)-1
        v = np.sqrt(chi2/(n*k)) if k>0 else 0
        results.append(f"  {dim.replace('_drift','').upper()}: χ²={chi2:.2f}, df={dof}, p={p:.4e}, V={v:.3f}")

    # 3c: InfoVis vs VAST
    results.append(f"\n--- 3c: InfoVis vs VAST ---")
    dft = df[df['track'].isin(['InfoVis','VAST'])]
    if len(dft) > 10:
        ct = pd.crosstab(dft['track'], dft['interaction_drift']=='major')
        ct.columns = ['not_major','major']
        if ct.shape == (2,2):
            odds, pf = stats.fisher_exact(ct.values)
            results.append(f"    {ct.to_string()}")
            results.append(f"    Fisher: OR={odds:.2f}, p={pf:.4f}")
        iv = dft[dft['track']=='InfoVis']; va = dft[dft['track']=='VAST']
        n1,x1 = len(iv),(iv['interaction_drift']=='major').sum()
        n2,x2 = len(va),(va['interaction_drift']=='major').sum()
        results.append(f"    InfoVis: {x1}/{n1}={x1/n1*100:.1f}%  VAST: {x2}/{n2}={x2/n2*100:.1f}%")

    # 3d: Platform
    results.append(f"\n--- 3d: PLATFORM ---")
    for dim in ["encoding_drift","interaction_drift","task_drift"]:
        ct = pd.crosstab(df['platform'], df[dim])
        chi2,p,dof,_ = stats.chi2_contingency(ct)
        results.append(f"  {dim.replace('_drift','').upper()}: χ²={chi2:.2f}, df={dof}, p={p:.4f}")

    return "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION 4: Chart Detection Pipeline Accuracy
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline_validation(chart_df):
    results = []
    results.append("\n" + "=" * 72)
    results.append("VALIDATION 4: CHART DETECTION PIPELINE ACCURACY")
    results.append("=" * 72)

    mask = chart_df['human_verdict'].notna() & (chart_df['human_verdict'] != '')
    df = chart_df[mask].copy()
    df['human_verdict'] = df['human_verdict'].str.strip().str.lower()
    n = len(df)
    results.append(f"\nAssessed artifacts: {n}")

    if n == 0:
        results.append("  NO ANNOTATIONS. Complete annotation first.")
        return "\n".join(results)

    for v, count in df['human_verdict'].value_counts().items():
        lo, hi = wilson_ci(count/n, n)
        results.append(f"  {v:<15} {count:>4} ({count/n*100:.1f}%) [{lo*100:.1f}%–{hi*100:.1f}%]")

    assess = df[df['human_verdict'] != 'cannot_assess']
    na = len(assess)
    if na > 0:
        ne = (assess['human_verdict']=='exact_match').sum()
        np_ = (assess['human_verdict']=='partial').sum()
        nw = (assess['human_verdict']=='wrong').sum()
        strict = ne/na
        lenient = (ne+np_)/na
        s_lo,s_hi = wilson_ci(strict,na)
        l_lo,l_hi = wilson_ci(lenient,na)
        results.append(f"\n  Strict accuracy:  {strict*100:.1f}% [{s_lo*100:.1f}%–{s_hi*100:.1f}%]")
        results.append(f"  Lenient accuracy: {lenient*100:.1f}% [{l_lo*100:.1f}%–{l_hi*100:.1f}%]")
        results.append(f"  Wrong:            {nw}/{na} = {nw/na*100:.1f}%")

    if 'detection_method' in df.columns:
        results.append(f"\n  By detection method:")
        for m in df['detection_method'].dropna().unique():
            sub = df[(df['detection_method']==m) & (df['human_verdict']!='cannot_assess')]
            if len(sub)>0:
                ok = sub['human_verdict'].isin(['exact_match','partial']).sum()
                results.append(f"    {m:<20} {ok}/{len(sub)} = {ok/len(sub)*100:.1f}%")

    return "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# SENSITIVITY + LATEX
# ─────────────────────────────────────────────────────────────────────────────

def run_sensitivity(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT da.interaction_drift, pf.vis_type
        FROM drift_annotations da
        JOIN traces t ON da.trace_id = t.id
        JOIN paper_figures pf ON t.figure_id = pf.id
        WHERE t.annotation_status = 'annotated'
    """, conn)
    conn.close()

    results = []
    results.append("\n" + "=" * 72)
    results.append("SENSITIVITY ANALYSIS")
    results.append("=" * 72)

    tc = df['vis_type'].value_counts()
    small = tc[tc < 10].index.tolist()
    results.append(f"\n  Removed (n<10): {small}")

    dfr = df[~df['vis_type'].isin(small)]
    n = len(dfr)
    nm = (dfr['interaction_drift']=='major').sum()
    p = nm/n
    lo,hi = wilson_ci(p,n)
    results.append(f"  Remaining: {n} traces")
    results.append(f"  Major interaction: {nm}/{n} = {p*100:.1f}% [{lo*100:.1f}%–{hi*100:.1f}%]")
    results.append(f"  Cliff {'HOLDS' if p > 0.80 else 'weakened'}")

    return "\n".join(results)


def generate_latex(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT da.encoding_drift, da.interaction_drift, da.task_drift
        FROM drift_annotations da
        JOIN traces t ON da.trace_id = t.id
        WHERE t.annotation_status = 'annotated'
    """, conn)
    conn.close()
    n = len(df)

    results = ["\n" + "="*72, "LATEX TABLE", "="*72]
    lines = [r"\begin{table}[t]",
             r"\caption{Drift severity with 95\% Wilson confidence intervals ($n=" + str(n) + r"$).}",
             r"\label{tab:drift_ci}", r"\centering\small",
             r"\begin{tabular}{lccc}", r"\toprule",
             r"\textbf{Dimension} & \textbf{Major} & \textbf{Minor} & \textbf{None} \\",
             r"\midrule"]

    for dim, label in [("encoding_drift","Encoding"),("interaction_drift","Interaction"),("task_drift","Task")]:
        parts = []
        for level in ["major","minor","none"]:
            c = (df[dim]==level).sum()
            p = c/n
            lo,hi = wilson_ci(p,n)
            parts.append(f"{p*100:.1f}\\% [{lo*100:.1f}--{hi*100:.1f}]")
        lines.append(f"{label} & {' & '.join(parts)} \\\\")

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}"]
    results.append("\n".join(lines))
    return "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_ira_pairwise(name_x, name_y, df_x, df_y, dims_x, dims_y):
    """
    Compute IRA between any two annotators (human-A vs human-B, or human vs LLM).
    dims_x/dims_y: tuples of (encoding_col, interaction_col, task_col) in each df.
    Returns formatted string.
    """
    from sklearn.metrics import cohen_kappa_score, confusion_matrix, classification_report
    label_map = {"none": 0, "minor": 1, "major": 2}
    labels_order = ["none", "minor", "major"]

    results = []
    results.append(f"\n  ╔══ {name_x} vs {name_y} ══╗")

    dim_names = ["encoding", "interaction", "task"]
    all_x, all_y = [], []

    for i, dim in enumerate(dim_names):
        cx, cy = dims_x[i], dims_y[i]
        if cx not in df_x.columns or cy not in df_y.columns:
            results.append(f"    {dim}: columns missing")
            continue

        # Rename to avoid collision on merge
        left = df_x[['trace_id', cx]].rename(columns={cx: 'val_x'})
        right = df_y[['trace_id', cy]].rename(columns={cy: 'val_y'})
        merged = left.merge(right, on='trace_id', how='inner')
        merged = merged.dropna(subset=['val_x', 'val_y'])
        merged = merged[(merged['val_x'] != '') & (merged['val_y'] != '')]
        merged['val_x'] = merged['val_x'].astype(str).str.strip().str.lower()
        merged['val_y'] = merged['val_y'].astype(str).str.strip().str.lower()
        merged = merged[merged['val_x'].isin(label_map) & merged['val_y'].isin(label_map)]

        n = len(merged)
        results.append(f"\n    {dim.upper()} (n={n}):")
        if n < 10:
            results.append(f"      Insufficient data (need ≥10)")
            continue

        xv = [label_map[v] for v in merged['val_x']]
        yv = [label_map[v] for v in merged['val_y']]
        all_x.extend(merged['val_x'].tolist())
        all_y.extend(merged['val_y'].tolist())

        k_uw = cohen_kappa_score(xv, yv)
        k_w = cohen_kappa_score(xv, yv, weights="linear")
        interp = ("almost perfect" if k_w >= 0.8 else "substantial" if k_w >= 0.6
                  else "moderate" if k_w >= 0.4 else "fair" if k_w >= 0.2 else "slight")

        results.append(f"      κ (unweighted) = {k_uw:.3f}")
        results.append(f"      κ (weighted)   = {k_w:.3f}  [{interp}]")

        cm = confusion_matrix(merged['val_x'].tolist(), merged['val_y'].tolist(), labels=labels_order)
        results.append(f"      Confusion (rows={name_x}, cols={name_y}):")
        results.append(f"      {'':>8} {'none':>6} {'minor':>6} {'major':>6}")
        for j, lab in enumerate(labels_order):
            results.append(f"      {lab:>8} {cm[j][0]:>6} {cm[j][1]:>6} {cm[j][2]:>6}")

    # Aggregate
    if len(all_x) >= 30:
        xn = [label_map[v] for v in all_x]
        yn = [label_map[v] for v in all_y]
        results.append(f"\n    AGGREGATE (n={len(all_x)}):")
        results.append(f"      κ (unweighted) = {cohen_kappa_score(xn, yn):.3f}")
        results.append(f"      κ (weighted)   = {cohen_kappa_score(xn, yn, weights='linear'):.3f}")

    return "\n".join(results)


def main():
    p = argparse.ArgumentParser(description="""
    Statistical validation for From Paper to Practice.
    
    Supports three modes:
      1) Two annotators:  --annotator-a fileA.json --annotator-b fileB.json
      2) Single annotator: --annotations file.json
      3) Excel sheets:    --drift-sheet file.xlsx --chart-sheet file.xlsx
    """)
    p.add_argument("--db", required=True)
    p.add_argument("--annotator-a", default=None, help="JSON from annotator A")
    p.add_argument("--annotator-b", default=None, help="JSON from annotator B")
    p.add_argument("--annotations", default=None, help="Single JSON (backward compat)")
    p.add_argument("--drift-sheet", default=None, help="XLSX drift annotations")
    p.add_argument("--chart-sheet", default=None, help="XLSX chart annotations")
    p.add_argument("--chart-validation", default=None, help="JSON from chart_trace_validation tool")
    p.add_argument("--output", default="validation_results.txt")
    args = p.parse_args()

    out = ["FROM PAPER TO PRACTICE — Statistical Validation Report",
           f"Database: {args.db}", ""]

    # ── Always run from DB (no human annotations needed) ──
    out.append(run_ci_validation(args.db))
    out.append(run_chi2_validation(args.db))
    out.append(run_sensitivity(args.db))
    out.append(generate_latex(args.db))

    # ── Two-annotator mode (primary) ──
    if args.annotator_a and args.annotator_b:
        name_a, drift_a, _ = load_from_json(args.annotator_a)
        name_b, drift_b, _ = load_from_json(args.annotator_b)

        out.append("\n" + "=" * 72)
        out.append("VALIDATION 1: INTER-ANNOTATOR AGREEMENT")
        out.append("=" * 72)
        out.append(f"  Annotator A: {name_a} ({args.annotator_a})")
        out.append(f"  Annotator B: {name_b} ({args.annotator_b})")

        # 1) Human A vs Human B  — THE KEY IRA METRIC
        out.append(run_ira_pairwise(
            name_a, name_b, drift_a, drift_b,
            ('human_encoding', 'human_interaction', 'human_task'),
            ('human_encoding', 'human_interaction', 'human_task'),
        ))

        # 2) Human A vs LLM
        out.append(run_ira_pairwise(
            name_a, "LLM", drift_a, drift_a,
            ('human_encoding', 'human_interaction', 'human_task'),
            ('llm_encoding', 'llm_interaction', 'llm_task'),
        ))

        # 3) Human B vs LLM
        out.append(run_ira_pairwise(
            name_b, "LLM", drift_b, drift_b,
            ('human_encoding', 'human_interaction', 'human_task'),
            ('llm_encoding', 'llm_interaction', 'llm_task'),
        ))

        out.append(run_failure_summary(name_a, drift_a, prefix="human"))
        out.append(run_failure_summary(name_b, drift_b, prefix="human"))
        out.append(run_failure_pairwise(name_a, name_b, drift_a, drift_b, prefix_x="human", prefix_y="human"))

    # ── Single-annotator mode (backward compatible) ──
    elif args.annotations:
        _, drift_df, _ = load_from_json(args.annotations)
        if drift_df is not None and len(drift_df) > 0:
            out.append("\n" + "=" * 72)
            out.append("VALIDATION 1: HUMAN vs LLM AGREEMENT (single annotator)")
            out.append("=" * 72)
            out.append(run_ira_pairwise(
                "Human", "LLM", drift_df, drift_df,
                ('human_encoding', 'human_interaction', 'human_task'),
                ('llm_encoding', 'llm_interaction', 'llm_task'),
            ))
            out.append(run_failure_summary("Human", drift_df, prefix="human"))

    elif args.drift_sheet:
        drift_df = pd.read_excel(args.drift_sheet, sheet_name="ANOTACIÓN_DRIFT")
        rn = {'HUMAN_encoding':'human_encoding','HUMAN_interaction':'human_interaction',
              'HUMAN_task':'human_task','LLM_encoding':'llm_encoding',
              'LLM_interaction':'llm_interaction','LLM_task':'llm_task'}
        drift_df.rename(columns=rn, inplace=True)
        out.append(run_ira_validation(drift_df))
        out.append(run_failure_summary("Human", drift_df, prefix="human"))
    else:
        out.append("\n[SKIPPED] Validation 1: no drift annotation files provided")

    # ── Chart detection + trace quality (from separate tool) ──
    if args.chart_validation:
        with open(args.chart_validation) as f:
            cv_data = json.load(f)

        # V4: Chart detection accuracy
        chart_list = cv_data.get('chart_detection', [])
        if chart_list:
            chart_df = pd.DataFrame(chart_list)
            chart_df.rename(columns={'human_verdict': 'human_verdict', 'human_notes': 'human_notes'}, inplace=True)
            out.append(run_pipeline_validation(chart_df))
        else:
            out.append("\n[SKIPPED] Validation 4: no chart detection data in JSON")
    else:
        out.append("\n[SKIPPED] Validations 4/5: --chart-validation not provided")

    report = "\n".join(out)
    with open(args.output, "w") as f:
        f.write(report)

    print(report)
    print(f"\n{'='*72}")
    print(f"Report saved to: {args.output}")


if __name__ == "__main__":
    main()
