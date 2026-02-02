# Veracode Reporting API – Full Fetcher (HTTPie + HMAC)

![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Output](https://img.shields.io/badge/output-JSONL%20%7C%20JSON%20%7C%20CSV%20%7C%20XLSX-green)
![Status](https://img.shields.io/badge/status-production--ready-brightgreen)

Production-ready CLI to export **all findings** from the Veracode Reporting REST API across any date range, with resilient retries, verification/auditing, and scalable outputs.

---

## ✨ Features

- **Full export** across any date range → auto-splits into ≤ **180-day windows** (API “6-month rule”)
- **Exhaustive pagination**
  - Follows HAL `next` and **enforces your `--size`**
  - Falls back to page metadata and length heuristics
- **Resilient retries** (5xx / 429 / network) with exponential backoff + jitter
- **Verification** (`--verify`)
  - Pages **seen vs reported**, totals **collected vs expected**
  - Writes per-window **audit JSON**
- **Stamping** (default)
  - Adds `source_report_id`, `window_start`, `window_end` to each row
- **Outputs**
  - **JSONL** (lossless, one row per line)
  - **JSON** (array)
  - **CSV** → **single file** (streamed; unlimited rows)
  - **XLSX** → **single workbook** (multi-sheet if needed)
- **Skip outputs** with `--no-csv` / `--no-xlsx`
- **Professional console icons** (`--icons`)

---

## 🔧 Prerequisites

- **Python 3.9+**
- **HTTPie** + Veracode HMAC plugin  
  ```bash
  pip install httpie veracode-api-signing

  For Excel export (optional):
  pip install pandas openpyxl xlsxwriter

  If you don’t need Excel, use --no-xlsx.

  Or, recommended, pip install -r requirements.txt

  🔐 Authentication
  export VERACODE_API_KEY_ID=YOUR_KEY_ID
  export VERACODE_API_KEY_SECRET=YOUR_KEY_SECRET

  Optional (macOS trust store):
  export REQUESTS_CA_BUNDLE=$(python -m certifi)

  🚀 Quick Start
  python3 VERACODE_REPORT_FETCH.py \
  --from 2023-01-01 --to 2025-09-15 \
  --report-type FINDINGS \
  --size 200 \
  --out ./out \
  --icons --verify

  Outputs:
	•	report_all_YYYYMMDD_HHMMSS.jsonl – line-delimited JSON
	•	report_all_YYYYMMDD_HHMMSS.json – JSON array
	•	report_all_YYYYMMDD_HHMMSS.csv – CSV (single file, unlimited rows)
	•	report_all_YYYYMMDD_HHMMSS.xlsx – Excel (one workbook, multiple sheets if needed)
	•	audit/audit_<report_id>.json – per-window audit files (when --verify is used)

  ⚙️ CLI Options
  --from YYYY-MM-DD       Start date (inclusive; 00:00:00 per window)
  --to YYYY-MM-DD         End date (inclusive; 23:59:59 per window)
  --report-type FINDINGS  Report type (default FINDINGS)
  --size INT              Page size for GET (default 1000)
  --out PATH              Output directory (default ./out)
  --filters FILE|<(JSON)  JSON merged into POST body (e.g., status, severity, application_name)
  --sleep FLOAT           Delay after POST before polling (default 0.5s)
  --poll-interval FLOAT   Seconds between polls (default 2.0)
  --poll-timeout INT      Max seconds to wait for COMPLETED (default 600)
  --icons                 Show console icons
  --no-stamp              Do not add source_report_id/window_start/window_end
  --verify                Verify pages/totals; write audit JSON
  --strict                With --verify, exit on mismatch/dupes
  --id-field FIELD        Unique key for duplicate check (e.g., finding_id)
  --no-xlsx               Skip Excel output
  --no-csv                Skip CSV output

	🎛️ Using Filters

	You can narrow down the findings returned by the API by passing a JSON file with --filters.
	These filters are merged into the POST body when creating the report.

	Examples

	Open-only findings
	{
 	 "status": "open"
	}

	Closed findings for a specific application
	{
	 "status": "closed",
 	 "application_name": "Demo Web App"
	}

	High severity only
	{
  	 "severity": ["5 - Very High", "4 - High"]
	}

	Running with filters
	python3 VERACODE_REPORT_FETCH.py \
  	--from 2024-01-01 --to 2025-09-17 \
  	--report-type FINDINGS \
 	--size 200 \
  	--filters filters.json \
  	--out ./out --icons --verify

	💡 Tip: If you omit "status" from your filters, the API will return all findings (open + closed + mitigated).

	🔍 Examples
 		python3 VERACODE_REPORT_FETCH.py \
  		--from 2022-01-01 --to 2025-09-17 \
  		--report-type FINDINGS --size 200 \
 		 --out ./out --icons --verify

	Skip Excel, keep CSV:
  		python3 VERACODE_REPORT_FETCH.py \
		--from 2022-01-01 --to 2025-09-17 \
  		--report-type FINDINGS --size 200 \
  		--out ./out --icons --verify --no-xlsx

  	JSON/JSONL only (no CSV, no XLSX):
  		python3 VERACODE_REPORT_FETCH.py \
  		--from 2022-01-01 --to 2025-09-17 \
  		--report-type FINDINGS --size 200 \
  		--out ./out --icons --verify --no-xlsx --no-csv

  	Gentler polling & longer timeout (busy tenants):
  		python3 VERACODE_REPORT_FETCH.py \
  		--from 2022-01-01 --to 2025-09-17 \
  		--report-type FINDINGS --size 200 \
  		--poll-interval 3.0 --poll-timeout 1800 \
  		--out ./out --icons --verify

  		🧾 Verification & Audit

		With --verify, per window you’ll see:
			🧾 running verification …
     		✅ pages: seen=7 reported=7 => OK
			✅ totals: collected=3002 expected=3002 => OK

   		Audit JSON (./out/audit/audit_<report_id>.json) includes:
		•	Page indexes seen and API total_pages
		•	API-reported total_elements vs collected
		•	Duplicate count (if --id-field is set)

		🔁 Resilient Retries
		•	Retries up to 7 attempts on 5xx / 429 / network errors
		•	Exponential backoff + jitter
		•	Honors Retry-After header on 429
		•	Retries partial JSON decode errors
		•	Fails fast on 401 Unauthorized

		Tuning tips:
		•	Large datasets: --size 200..500, --poll-interval 3..5, --poll-timeout 1800..3600

⸻

		📄 Output Details
		•	JSONL – Source of truth; easiest to pipe/stream
		•	JSON – Pretty-printed array
		•	CSV – One file, flattened; lists encoded as JSON strings in cells
		•	XLSX – One workbook; adds sheets when row cap reached (~1,048,576 per sheet)


	📸 Sample Console Output
	🗂️ === Window 2023-12-22 → 2024-06-18 ===
  	📄 report id: cae52e31-69e6-4994-be8f-20e146c96c71
  	🔄 status: PROCESSING
  	✅ status: COMPLETED
    📦 page 0: 928 items  ➡️  window_total=0, grand_total=3754
    📦 page 1: 283 items  ➡️  window_total=928, grand_total=4682
    📦 page 2: 1036 items ➡️  window_total=1211, grand_total=4965
    ...
    🧾 running verification …
      ✅ pages: seen=7 reported=7 => OK
  	📊 window complete: 3002 items  (grand_total=6756)
	Outputs:
 	 JSONL : out/report_all_20250918_213455.jsonl
 	 JSON  : out/report_all_20250918_213455.json
 	 CSV   : out/report_all_20250918_213455.csv
 	 XLSX  : out/report_all_20250918_213455.xlsx
	📊 Grand total items: 10126




💡 With this script, you can reliably export Veracode Reporting API data at scale, verify completeness, and get outputs in analyst-friendly formats.
Not an official VERACODE tool.
https://docs.veracode.com/r/Reporting_REST_API
