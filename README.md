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

# 4. Run the app — no .env required
streamlit run app.py
```

The database is created automatically at `~/.medical-audit/audit.db` on first run.
All hospital configuration is managed from the **Settings** page in the UI.

## Optional environment variable

| Variable | Description |
|---|---|
| `LOG_LEVEL` | Python logging level (default: `INFO`) |

## First-time hospital setup

Go to **Settings → Gestión de hospitales → Agregar / editar hospital** and fill in:

- Display name, NIT, invoice identifier prefix
- SIHOS base URL and invoice doc code
- DOCUMENT_STANDARDS JSON (maps document type labels to filename prefixes)
- SIHOS user and password
- Drive credentials path (path to a `drive.json` Google service account file)
- Base path (root directory for this hospital's audit data, e.g. `C:/Auditorias/SANTA_LUCIA`)

Admin/contract pair mappings are managed under **Settings → Mapeos**. Unknown pairs
found during ingestion are auto-registered with NULL canonical values; fill them in
to include those invoices in the audit.

## Folder structure (per audit period)

```
{hospital.base_path}/
└── {period}/
    ├── DRIVE/               # Raw folders downloaded from Google Drive
    ├── STAGE/               # Working staging area (pipeline runs here)
    ├── AUDIT/               # Final organised archive
    ├── {period}_SIHOS.xlsx  # Input: SIHOS billing export
```

## Application tabs

- **Pipeline** — Select and run audit pipeline stages (ingestion, download, normalisation, OCR, CUFE verification, document presence checks)
- **Audit** — Browse invoices with findings, batch-update statuses and types, export report
- **Settings** — Manage hospitals, admin/contract mappings, and filename prefix fixes

## Document naming convention

Files follow the pattern: `{INVOICE_ID_PREFIX}{INVOICE_NUMBER}_{NIT}_{INVOICE_ID_PREFIX}{INVOICE_NUMBER}.pdf`

Example for Santa Lucia: `FEV_890701078_HSL12345.pdf`

Prefixes per document type (FACTURA, FIRMA, HISTORIA, etc.) are configured per hospital
under `DOCUMENT_STANDARDS` in the Settings page.

## Development

```bash
pip install -e ".[dev]"
ruff check .
mypy core/ db/
pytest
pre-commit install
```
