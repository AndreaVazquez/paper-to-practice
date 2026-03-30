Here’s a concise, clean README tailored to your repo and aligned with the paper:

---

# PaperToPractice

**From Paper to Practice: Tracing How Academic Visualization Designs Evolve in Public Repositories**
IEEE VIS 2026 (Full Paper Submission)

---

## Overview

**PaperToPractice** is a modular AI-agent ecosystem that establishes traceability between academic visualization research (IEEE VIS papers) and real-world implementations in public repositories (Kaggle, GitHub, Observable).

The system builds a **progressive curation pipeline** that:

1. Extracts and classifies figures from research papers
2. Detects visualization types in repository code
3. Matches them via chart-type affinity
4. Verifies matches using multimodal LLM analysis
5. Annotates **Design Drift** (encoding, interaction, task)
6. Generates **provenance-aware educational narratives (OERs)**

---

## Key Features

* **Large-scale corpus processing**

  * 3,774 VIS papers
  * 27,942 classified figures
  * 1,419 repository notebooks

* **Trace construction**

  * 1,270 verified paper ↔ practice links
  * 68.6% false-positive rejection via multimodal validation

* **Design Drift analysis**

  * Encoding, Interaction, Task dimensions
  * Empirical characterization of the *Interaction Cliff*

* **Multi-agent architecture**

  * Specialized agents for image, text, detection, reasoning, and querying

* **OER generation**

  * Human-in-the-loop narrative authoring
  * JSON-LD metadata for machine-readable provenance

---

## Repository Structure

```
.
├── trace_inspector/      # UI for exploring traces and narratives
├── tracing/              # Core pipeline logic (trace construction, drift)
├── ui/                   # Frontend components (dashboard, authoring)
├── docs/                 # Technical pipeline documentation (01–12)
├── validation/           # Evaluation scripts and annotation datasets
│   ├── p2p_annotations_*.json
│   ├── p2p_chart_trace_validation.json
│   └── run_validations.py
├── manage.py             # Django entry point
├── requirements.txt      # Dependencies
├── db.sqlite3            # Local database (corpus + traces)
└── .env                  # Environment variables
```

---

## Pipeline Stages

The system follows a **progressive curation funnel**:

1. **Ingest Papers** → extract figures from PDFs
2. **Classify Figures** → taxonomy-based chart types
3. **Fetch Repositories** → crawl Kaggle/GitHub/Observable
4. **Detect Charts** → deterministic + LLM fallback
5. **Build Traces** → chart-type affinity matching
6. **Annotate Drift** → multimodal verification + labeling
7. **Narrative Authoring** → generate OERs with human review
8. **Publish** → self-contained HTML + JSON-LD metadata

Each stage is **modular and re-runnable**.

---

## Installation

```bash
git clone <repo-url>
cd papertopractice

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

---

## Setup

Create a `.env` file with API keys:

```
GROQ_API_KEY=...
GOOGLE_API_KEY=...
OPENAI_API_KEY=...
```

Run migrations:

```bash
python manage.py migrate
```

---

## Usage

### Run pipeline stages (example)

```bash
python manage.py ingest_papers
python manage.py extract_figures
python manage.py classify_figures
python manage.py crawl_repos
python manage.py detect_charts
python manage.py build_traces
python manage.py annotate_drift
```

### Launch UI

```bash
python manage.py runserver
```

Then open:

```
http://127.0.0.1:8000/
```

---

## Validation

Run evaluation scripts:

```bash
python validation/run_validations.py
```

Includes:

* Drift annotation agreement (Cohen’s κ)
* Chart detection accuracy (strict + lenient)

---

## Core Concepts

* **Chart-Type Affinity**
  Matching based on shared visualization taxonomy

* **Design Drift**

  * Encoding: visual structure changes
  * Interaction: loss of interactivity
  * Task: shift in analytical purpose

* **Interaction Cliff**
  Interaction is either fully preserved or completely lost

* **Progressive Curation**
  Each pipeline stage filters errors from the previous one

---

## Output

* SQLite database of traces and annotations
* Interactive dashboard (trace exploration)
* OER narratives (HTML + JSON-LD)
* Validation datasets

---

## License

CC BY 4.0 (for generated OER content)
Code license: TBD

---

## Notes

* The system is designed as **research infrastructure**, not a one-off pipeline
* Each module can be independently improved and re-run
* LLMs are used cautiously within a **verification-first architecture**
