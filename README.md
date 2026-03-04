# Medical Audit

Healthcare document audit system for Colombian hospitals. A Streamlit application that automates the verification, organisation, and reporting of medical billing invoices sourced from the SIHOS hospital information system.

## Overview

The system ingests weekly billing exports from SIHOS, organises the corresponding PDF document folders, runs automated checks (file naming, mandatory document types, OCR, CUFE codes), and provides an auditor interface to review and annotate findings.

## Requirements

- Python 3.11+
- [`ocrmypdf`](https://ocrmypdf.readthedocs.io/) (requires Tesseract and Ghostscript on the system)
- Playwright Chromium (for SIHOS invoice downloads)

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2. Install Python dependencies
pip install -e .

# 3. Install Playwright browser
playwright install chromium

# 4. Configure environment
cp .env.example .env
# Edit .env with your paths and hospital key

# 5. Add credentials
mkdir -p config/keys/SANTA_LUCIA
# Place drive.json (Google service account) and sihos.json here
# sihos.json format: {"user": "...", "password": "..."}
```

## Running

```bash
streamlit run app.py
```

## Configuration

### `.env` variables

| Variable | Description |
|---|---|
| `ACTIVE_HOSPITAL` | Hospital key matching an entry in `config/hospitals.py` (e.g. `SANTA_LUCIA`) |
| `AUDIT_WEEK` | Audit period string (e.g. `22-28`), appended to `BASE_PATH` |
| `ROOT_PATH` | Root shared folder; `audit.db` is stored here |
| `BASE_PATH` | Parent of weekly audit folders |
| `LOG_LEVEL` | Python logging level (default: `INFO`) |

### Supported hospitals

- `SANTA_LUCIA`
- `RAMON_MARIA_ARANA`

## Folder structure (per audit week)

```
BASE_PATH/
└── AUDIT_WEEK/
    ├── DRIVE/          # Raw folders downloaded from Google Drive
    ├── STAGE/          # Working staging area (pipeline runs here)
    ├── AUDIT/          # Final organised archive
    ├── DOCS/           # Text list files (missing_folders.txt, skip_soat.txt, etc.)
    ├── MISSING_FOLDERS/
    ├── MISSING_FILES/
    ├── {WEEK}_SIHOS.xlsx     # Input: SIHOS billing export
    └── {WEEK}_AUDITORIA.xlsx # Output: audit report
```

## Application tabs

- **Pipeline** — Select and run audit pipeline stages (ingestion, download, normalisation, verification)
- **Audit** — Browse invoices with findings, update finding statuses, export report
- **Documents** — View and edit `.txt` input/output files under `DOCS/`
- **Settings** — Read-only view of active configuration

## Document naming convention

Files follow the pattern: `{PREFIX}_{NIT}_{INVOICE_ID_PREFIX}{number}.pdf`

Example for Santa Lucia: `FEV_890701078_HSL12345.pdf`

Prefixes per document type (FACTURA, FIRMA, HISTORIA, etc.) are defined in `config/hospitals.py` under `DOCUMENT_STANDARDS`.
