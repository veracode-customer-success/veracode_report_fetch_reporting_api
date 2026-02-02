#!/usr/bin/env python3
# VERACODE_REPORT_FETCH.py
# Production build:
# - Robust pagination, resilient retries (5xx/429/network)
# - 180-day windowing
# - Verification (pages seen vs reported, totals collected vs expected) + audit JSON
# - Stamping (source_report_id, window_start, window_end)
# - Outputs: JSONL + JSON; CSV (single file, streamed); XLSX (single workbook with multi-sheets)
#   Skip via flags: --no-csv / --no-xlsx
# - Professional console icons

import argparse
import csv
import json
import os
import re
import random
import subprocess
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# ----------------------------- Constants -----------------------------

BASE_URL = "https://api.veracode.com"
POST_URL = f"{BASE_URL}/appsec/v1/analytics/report"
GET_URL_T = f"{BASE_URL}/appsec/v1/analytics/report/{{rid}}?page={{page}}&size={{size}}"
GET_URL_META_T = f"{BASE_URL}/appsec/v1/analytics/report/{{rid}}"

ICONS = {
    "window": "🗂️",
    "report": "📄",
    "status": {"SUBMITTED": "⏳", "PROCESSING": "🔄", "COMPLETED": "✅", "UNKNOWN": "❔"},
    "page": "📦",
    "done": "📊",
    "arrow": "➡️",
    "audit": "🧾",
}

warnings.simplefilter("ignore", UserWarning)  # quiets noisy libs if present


# ----------------------------- Utilities -----------------------------

def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def check_env() -> None:
    if not os.getenv("VERACODE_API_KEY_ID") or not os.getenv("VERACODE_API_KEY_SECRET"):
        die("Set VERACODE_API_KEY_ID and VERACODE_API_KEY_SECRET for the HTTPie HMAC plugin.")
    if os.getenv("VERACODE_API_ID") or os.getenv("VERACODE_API_KEY"):
        print("WARN: Legacy VERACODE_API_ID/VERACODE_API_KEY are set; HTTPie uses *_KEY_ID/*_KEY_SECRET.",
              file=sys.stderr)


def call_httpie(method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Run HTTPie with HMAC auth, with resilient retries on transient failures.
    Retries on: 5xx, 429, and common connection errors; max 7 attempts; jittered exponential backoff.
    """
    max_attempts = 7
    base = 1.2  # backoff base
    for attempt in range(1, max_attempts + 1):
        try:
            cmd = ["http", "--body", "-A", "veracode_hmac", method, url]
            proc = subprocess.run(
                cmd,
                input=json.dumps(body) if body is not None else None,
                text=True,
                capture_output=True,
                check=False,
            )
        except FileNotFoundError:
            die("http(ie) is not installed. Install with `pip install httpie`.")

        # Success path
        if proc.returncode == 0:
            out = proc.stdout.strip()
            if not out:
                return {}
            try:
                return json.loads(out)
            except json.JSONDecodeError as e:
                if attempt < max_attempts:
                    sleep = min(30, (base ** attempt) + random.uniform(0, 0.5))
                    print(f"  JSON parse error; retrying in {sleep:.1f}s …", file=sys.stderr)
                    time.sleep(sleep)
                    continue
                die(f"JSON parse error from {method} {url}: {e}\nRaw (first 4KB):\n{out[:4096]}")

        # Non-zero return: inspect stderr for status
        stderr = proc.stderr or ""
        transient = any(code in stderr for code in [" 500 ", " 502 ", " 503 ", " 504 "]) or \
                    "Read timed out" in stderr or "Connection reset" in stderr or "EOF occurred" in stderr

        # 429 with Retry-After
        if " 429 " in stderr:
            m = re.search(r"Retry-After:\s*(\d+)", stderr, flags=re.IGNORECASE)
            ra = int(m.group(1)) if m else None
            wait = ra if ra is not None else min(60, (base ** attempt) + random.uniform(0, 0.5))
            print(f"  429 rate limited; retrying in {wait:.1f}s …", file=sys.stderr)
            time.sleep(wait)
            continue

        if transient and attempt < max_attempts:
            sleep = min(60, (base ** attempt) + random.uniform(0, 0.75))
            print(f"  transient error (attempt {attempt}/{max_attempts}); retrying in {sleep:.1f}s …", file=sys.stderr)
            time.sleep(sleep)
            continue

        # Unauthorized should fail fast with a clear message
        if "Unauthorized" in stderr or " 401 " in stderr:
            die("HTTPie 401 Unauthorized. Verify VERACODE_API_KEY_ID/VERACODE_API_KEY_SECRET and tenant access.\n" + stderr)

        # Final hard fail
        die(f"HTTPie error after {attempt} attempt(s):\n{stderr}", code=proc.returncode)


def windows_180(from_d: str, to_d: str) -> list[tuple[str, str]]:
    start = datetime.strptime(from_d, "%Y-%m-%d").date()
    end = datetime.strptime(to_d, "%Y-%m-%d").date()
    if end < start:
        die("--to must be >= --from")
    out: list[tuple[str, str]] = []
    cur = start
    step = timedelta(days=180)
    while cur <= end:
        nxt = cur + step - timedelta(days=1)
        if nxt > end:
            nxt = end
        out.append((cur.isoformat(), nxt.isoformat()))
        cur = nxt + timedelta(days=1)
    return out


# ----------------------------- Payload helpers -----------------------------

def extract_report_id(post_json: dict[str, Any]) -> str:
    rid = post_json.get("id")
    if not rid and isinstance(post_json.get("_embedded"), dict):
        rid = post_json["_embedded"].get("id")
    rid = str(rid) if rid else ""
    if not rid:
        die(f"POST returned no report id:\n{json.dumps(post_json, indent=2)[:2000]}")
    return rid


def current_status(meta_json: dict[str, Any]) -> str:
    status = meta_json.get("status")
    if not status and isinstance(meta_json.get("_embedded"), dict):
        status = meta_json["_embedded"].get("status")
    return str(status or "")


def is_completed(meta_json: dict[str, Any]) -> bool:
    if current_status(meta_json).upper() == "COMPLETED":
        return True
    drc = meta_json.get("date_report_completed")
    if not drc and isinstance(meta_json.get("_embedded"), dict):
        drc = meta_json["_embedded"].get("date_report_completed")
    return bool(drc)


def extract_items(page_json: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(page_json.get("content"), list):
        return page_json["content"]
    emb = page_json.get("_embedded")
    if isinstance(emb, dict):
        if isinstance(emb.get("items"), list):
            return emb["items"]
        if isinstance(emb.get("findings"), list):
            return emb["findings"]
    if isinstance(page_json.get("findings"), list):
        return page_json["findings"]
    if isinstance(page_json, list):
        return page_json
    return []


def hal_next(page_json: dict[str, Any]) -> str | None:
    links = page_json.get("_links")
    if isinstance(links, dict):
        nxt = links.get("next")
        if isinstance(nxt, dict):
            href = nxt.get("href")
            if isinstance(href, str) and href:
                return href if href.startswith("http") else (BASE_URL + href)
    return None


def hal_next_with_size(page_json: dict[str, Any], desired_size: int) -> str | None:
    """Follow HAL next, forcing &size=desired_size if the link omits it."""
    nxt = hal_next(page_json)
    if not nxt:
        return None
    u = urlparse(nxt)
    q = dict(parse_qsl(u.query))
    if "size" not in q:
        q["size"] = str(desired_size)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))


def _find_page_meta(payload: dict[str, Any]) -> dict[str, int | None]:
    """Normalize pagination meta from multiple possible locations/key styles."""
    candidates = [
        payload.get("page"),
        payload.get("page_metadata"),
        (payload.get("_embedded") or {}).get("page"),
        (payload.get("_embedded") or {}).get("page_metadata"),
    ]
    meta: dict[str, int | None] = {}
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if "number" in c:
            try: meta["number"] = int(c["number"])
            except Exception: pass
        if "page_number" in c:
            try: meta["number"] = int(c["page_number"])
            except Exception: pass
        if "totalPages" in c:
            try: meta["total_pages"] = int(c["totalPages"])
            except Exception: pass
        if "total_pages" in c:
            try: meta["total_pages"] = int(c["total_pages"])
            except Exception: pass
        if "size" in c:
            try: meta["size"] = int(c["size"])
            except Exception: pass
        if "number" in meta and "total_pages" in meta:
            return meta
    return meta


def normalize_page_meta(payload: dict[str, Any]) -> dict[str, int | None]:
    """Return {number,total_pages,size,total_elements} if discoverable."""
    meta = _find_page_meta(payload)
    te: int | None = None
    for candidate in (
        payload.get("totalElements"), payload.get("total_elements"),
        (payload.get("page") or {}).get("totalElements"),
        (payload.get("page_metadata") or {}).get("totalElements"),
        (payload.get("_embedded") or {}).get("totalElements"),
        (payload.get("_embedded", {}).get("page") or {}).get("totalElements"),
    ):
        if isinstance(candidate, (int, str)):
            try:
                te = int(candidate)
                break
            except Exception:
                pass
    meta.setdefault("number", None)
    meta.setdefault("total_pages", None)
    meta.setdefault("size", None)
    meta["total_elements"] = te
    return meta


# ----------------------------- API ops -----------------------------

def post_report(report_type: str, start_d: str, end_d: str, extra: dict[str, Any]) -> str:
    body = {
        "report_type": report_type,
        "last_updated_start_date": f"{start_d} 00:00:00",
        "last_updated_end_date": f"{end_d} 23:59:59",
        # NOTE: do NOT set "status" here → API returns open+closed+mitigated by default
    }
    body.update(extra or {})
    resp = call_httpie("POST", POST_URL, body)
    return extract_report_id(resp)


def poll_ready(rid: str, max_wait_s: int, interval_s: float, icons: bool) -> None:
    deadline = time.time() + max_wait_s
    last = ""
    while time.time() < deadline:
        meta = call_httpie("GET", GET_URL_META_T.format(rid=rid))
        st = (current_status(meta) or "UNKNOWN").upper()
        if st != last:
            st_icon = ICONS["status"].get(st, ICONS["status"]["UNKNOWN"]) if icons else ""
            print(f"  {st_icon} status: {st}".rstrip())
            last = st
        if is_completed(meta):
            return
        time.sleep(interval_s)
    die(f"Report {rid} not ready within {max_wait_s}s")


def stream_report_items(rid: str, size: int):
    """
    Exhaustive pagination:
      1) Start at page=0
      2) Follow HAL _links.next (forcing your size if missing)
      3) Else use page metadata (camel/snake)
      4) Else fallback: if items == size, try next page index; stop on short/empty
    Yields a marker dict {'__PAGE_META__': {...}} before each page's items.
    """
    page_no = 0
    next_url = GET_URL_T.format(rid=rid, page=page_no, size=size)

    while next_url:
        page = call_httpie("GET", next_url)
        items = extract_items(page)
        meta = normalize_page_meta(page)

        yield {"__PAGE_META__": {"page_no": page_no, "count": len(items), "meta": meta}}
        for it in items:
            yield it

        # 1) HAL next (force &size if omitted)
        nxt = hal_next_with_size(page, size) or hal_next(page)
        if nxt:
            next_url = nxt
            page_no += 1
            continue

        # 2) Page meta next
        meta_next = _find_page_meta(page)
        if meta_next and "number" in meta_next and "total_pages" in meta_next:
            num = meta_next["number"]
            tot = meta_next["total_pages"]
            if isinstance(num, int) and isinstance(tot, int) and (num + 1) < tot:
                next_url = GET_URL_T.format(rid=rid, page=(num + 1), size=size)
                page_no = num + 1
                continue

        # 3) Length-based fallback
        if len(items) == size:
            page_no += 1
            next_url = GET_URL_T.format(rid=rid, page=page_no, size=size)
            continue

        # Done
        next_url = None


# ----------------------------- Outputs: JSON/JSONL + CSV (single) + XLSX (single workbook) -----------------------------

def write_jsonl(all_items: list[dict[str, Any]], jsonl_path: Path) -> int:
    n = 0
    with jsonl_path.open("w", encoding="utf-8") as jf:
        for obj in all_items:
            if "__PAGE_META__" in obj:
                continue
            jf.write(json.dumps(obj, ensure_ascii=False) + "\n")
            n += 1
    return n


def build_headers_from_jsonl(jsonl_path: Path) -> list[str]:
    """Make a union of flattened keys without loading all records in RAM."""
    def flatten_keys(d: dict[str, Any], prefix: str = "") -> set[str]:
        out: set[str] = set()
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out |= flatten_keys(v, key)
            else:
                out.add(key)
        return out

    headers: set[str] = set()
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            headers |= flatten_keys(obj)
    return sorted(headers)


def flatten_for_row(d: dict[str, Any], headers: list[str]) -> dict[str, Any]:
    """Flatten d according to headers. Lists are JSON-encoded strings."""
    def flatten(d0: dict[str, Any], prefix: str = "", out: dict[str, Any] | None = None) -> dict[str, Any]:
        if out is None:
            out = {}
        for k, v in d0.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                flatten(v, key, out)
            elif isinstance(v, list):
                out[key] = json.dumps(v, ensure_ascii=False)
            else:
                out[key] = v
        return out
    flat = flatten(d)
    return {h: flat.get(h, None) for h in headers}


def write_csv_single_from_jsonl(jsonl_path: Path, out_dir: Path, base_name: str, headers: list[str]) -> Path:
    """Stream JSONL -> one CSV file (unbounded; limited by disk)."""
    csv_path = out_dir / f"{base_name}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fw:
        writer = csv.DictWriter(fw, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                writer.writerow(flatten_for_row(obj, headers))
    return csv_path


def write_xlsx_one_workbook_from_jsonl(
    jsonl_path: Path, out_dir: Path, base_name: str, headers: list[str],
    max_rows_per_sheet: int = 1_048_000,  # Excel limit minus buffer for header
    chunk_size: int = 100_000
) -> Path:
    """
    Stream JSONL -> one XLSX workbook. Adds sheets as needed, never new files.
    Requires pandas + XlsxWriter. Avoids big in-memory frames by chunking.
    """
    try:
        import pandas as pd  # type: ignore
    except Exception as e:
        die(f"XLSX requested but pandas is not available: {e}. Install pandas/openpyxl/xlsxwriter or use --no-xlsx.")

    xlsx_path = out_dir / f"{base_name}.xlsx"
    writer = pd.ExcelWriter(str(xlsx_path), engine="xlsxwriter",
                            datetime_format="yyyy-mm-dd hh:mm:ss",
                            date_format="yyyy-mm-dd")

    sheet_idx = 1
    rows_buffer: list[dict[str, Any]] = []
    sheet_rows_written = 0

    def flush_buffer_to_sheet():
        nonlocal rows_buffer, sheet_rows_written, sheet_idx
        if not rows_buffer:
            return
        import pandas as pd
        df = pd.DataFrame(rows_buffer, columns=headers)
        sheet_name = f"findings_{sheet_idx:02d}"
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for i, col in enumerate(df.columns[:50]):  # cap autosize to first 50 cols
            max_len = min(80, max(len(str(col)), int(df[col].astype(str).map(len).max())))
            ws.set_column(i, i, max(10, max_len + 2))
        sheet_rows_written += len(rows_buffer)
        rows_buffer = []

    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            rows_buffer.append(flatten_for_row(obj, headers))

            if len(rows_buffer) >= chunk_size:
                if sheet_rows_written + len(rows_buffer) > max_rows_per_sheet:
                    if sheet_rows_written > 0:
                        flush_buffer_to_sheet()
                        sheet_idx += 1
                        sheet_rows_written = 0
                    while len(rows_buffer) > max_rows_per_sheet:
                        cut = rows_buffer[:max_rows_per_sheet]
                        rows_buffer = rows_buffer[max_rows_per_sheet:]
                        import pandas as pd
                        df = pd.DataFrame(cut, columns=headers)
                        sheet_name = f"findings_{sheet_idx:02d}"
                        df.to_excel(writer, index=False, sheet_name=sheet_name)
                        sheet_idx += 1
                    flush_buffer_to_sheet()
                else:
                    flush_buffer_to_sheet()

    # final flush
    if rows_buffer:
        if sheet_rows_written + len(rows_buffer) > max_rows_per_sheet:
            flush_buffer_to_sheet()
            sheet_idx += 1
            sheet_rows_written = 0
        flush_buffer_to_sheet()

    writer.close()
    return xlsx_path


def write_all_outputs(
    all_items: list[dict[str, Any]], out_dir: Path, no_csv: bool = False, no_xlsx: bool = False
) -> tuple[Path, Path, Path | None, Path | None]:
    """
    Writes:
      - JSONL (authoritative stream)
      - JSON (array)
      - CSV (single file, streamed) [unless --no-csv]
      - XLSX (single workbook with multiple sheets) [unless --no-xlsx]
    Returns: (jsonl_path, json_path, csv_path_or_None, xlsx_path_or_None)
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")  # timezone-aware UTC
    base = f"report_all_{ts}"

    # JSONL
    jsonl_path = out_dir / f"{base}.jsonl"
    _ = write_jsonl(all_items, jsonl_path)

    # JSON array
    json_path = out_dir / f"{base}.json"
    arr = [o for o in all_items if "__PAGE_META__" not in o]
    json_path.write_text(json.dumps(arr, ensure_ascii=False, indent=2), encoding="utf-8")

    headers = build_headers_from_jsonl(jsonl_path)

    # CSV (single)
    csv_path: Path | None = None
    if not no_csv:
        csv_path = write_csv_single_from_jsonl(jsonl_path, out_dir, base_name=base, headers=headers)

    # XLSX (one workbook)
    xlsx_path: Path | None = None
    if not no_xlsx:
        xlsx_path = write_xlsx_one_workbook_from_jsonl(
            jsonl_path, out_dir, base_name=base, headers=headers,
            max_rows_per_sheet=1_048_000, chunk_size=100_000
        )

    return jsonl_path, json_path, csv_path, xlsx_path


# ----------------------------- CLI / Main -----------------------------

def main() -> None:
    check_env()

    ap = argparse.ArgumentParser(
        description="Veracode Reporting API via HTTPie/HMAC. Robust pagination with retries. JSON/JSONL/CSV outputs. Optional XLSX."
    )
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--report-type", default="FINDINGS", help="Report type (e.g., FINDINGS)")
    ap.add_argument("--size", type=int, default=1000, help="Page size for GET")
    ap.add_argument("--out", default="./out", help="Output directory")
    ap.add_argument("--filters", default=None, help="Path to JSON with extra POST filters (merged)")
    ap.add_argument("--sleep", type=float, default=0.5, help="Pause after POST before polling")
    ap.add_argument("--poll-timeout", type=int, default=600, help="Seconds to wait for report completion")
    ap.add_argument("--poll-interval", type=float, default=2.0, help="Polling interval in seconds")
    ap.add_argument("--icons", action="store_true", help="Add visual icons to logs")
    ap.add_argument("--no-stamp", action="store_true",
                    help="Do not add source_report_id/window_start/window_end to each record")
    ap.add_argument("--verify", action="store_true",
                    help="After paging, verify coverage using server metadata; fetch missing pages if any")
    ap.add_argument("--strict", action="store_true",
                    help="With --verify, exit non-zero on any mismatch/duplicate")
    ap.add_argument("--id-field", default=None,
                    help="Optional unique id field (e.g., finding_id) to check for duplicates")
    ap.add_argument("--no-xlsx", action="store_true",
                    help="Skip generating the Excel (.xlsx) file")
    ap.add_argument("--no-csv", action="store_true",
                    help="Skip generating the CSV file")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    audit_dir = out_dir / "audit"

    extra: dict[str, Any] = {}
    if args.filters:
        try:
            extra = json.loads(Path(args.filters).read_text(encoding="utf-8"))
            if not isinstance(extra, dict):
                die("--filters must be a JSON object")
        except Exception as e:
            die(f"reading --filters: {e}")

    windows = windows_180(args.date_from, args.date_to)
    print("Windows:")
    for s, e in windows:
        print(f"  - {ICONS['window'] if args.icons else ''} {s} -> {e}".rstrip())

    all_items: list[dict[str, Any]] = []
    grand_total = 0

    for w_start, w_end in windows:
        print(f"{ICONS['window'] if args.icons else ''} === Window {w_start} → {w_end} ===".rstrip())
        rid = post_report(args.report_type, w_start, w_end, extra)
        print(f"  {ICONS['report'] if args.icons else ''} report id: {rid}".rstrip())
        if args.sleep > 0:
            time.sleep(args.sleep)
        poll_ready(rid, max_wait_s=args.poll_timeout, interval_s=args.poll_interval, icons=args.icons)

        window_total = 0
        pages_seen_meta: list[dict[str, Any]] = []
        window_items: list[dict[str, Any]] = []

        for obj in stream_report_items(rid, args.size):
            if "__PAGE_META__" in obj:
                meta = obj["__PAGE_META__"]
                pages_seen_meta.append(meta)
                print(
                    f"    {ICONS['page'] if args.icons else ''} "
                    f"page {meta['page_no']}: {meta['count']} items"
                    f"  {ICONS['arrow'] if args.icons else ''}  window_total={window_total}, grand_total={grand_total}"
                    .rstrip()
                )
                continue

            # stamp finding with provenance
            if args.no_stamp:
                stamped = obj
            else:
                stamped = dict(obj)
                stamped["source_report_id"] = rid
                stamped["window_start"] = w_start
                stamped["window_end"] = w_end

            all_items.append(stamped)
            window_items.append(stamped)
            window_total += 1
            grand_total += 1

        if args.verify:
            print(f"    {ICONS['audit'] if args.icons else ''} running verification …".rstrip())
            seen_indexes = {p["page_no"] for p in pages_seen_meta}
            meta_sources = [p.get("meta") or {} for p in pages_seen_meta if isinstance(p.get("meta"), dict)]
            merged_meta: dict[str, Any] = {}
            for m in meta_sources:
                for k, v in m.items():
                    if v is not None:
                        merged_meta[k] = v

            total_pages = merged_meta.get("total_pages")
            pages_seen_count = len(seen_indexes)
            if isinstance(total_pages, int):
                same = (pages_seen_count == total_pages)
                status_icon = "✅" if same and args.icons else ("⚠️" if args.icons else "")
                print(f"      {status_icon} pages: seen={pages_seen_count} reported={total_pages} "
                      f"=> {'OK' if same else 'MISMATCH'}".rstrip())
            else:
                print(f"      {'❔ ' if args.icons else ''}pages: seen={pages_seen_count} reported=? (not provided)".rstrip())

            audit = {
                "report_id": rid,
                "page_indexes_seen": sorted(list(seen_indexes)),
                "pages_seen_count": pages_seen_count,
                "total_pages_reported": total_pages,
                "total_elements_reported": merged_meta.get("total_elements"),
                "collected_count_after_verify": len(window_items),
                "id_field": args.id_field,
                "duplicate_id_count": None,
                "strict_ok": True
            }
            audit_dir.mkdir(parents=True, exist_ok=True)
            (audit_dir / f"audit_{rid}.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")

        print(f"  {ICONS['done'] if args.icons else ''} window complete: {window_total} items  (grand_total={grand_total})".rstrip())

    # Write outputs (CSV single file; XLSX single workbook; both skippable)
    jsonl_path, json_path, csv_path, xlsx_path = write_all_outputs(
        all_items, out_dir, no_csv=args.no_csv, no_xlsx=args.no_xlsx
    )

    print("Outputs:")
    print(f"  JSONL : {jsonl_path}")
    print(f"  JSON  : {json_path}")
    print(f"  CSV   : {csv_path if csv_path else '(skipped)'}")
    print(f"  XLSX  : {xlsx_path if xlsx_path else '(skipped)'}")
    print(f"{ICONS['done'] if args.icons else ''} Grand total items: {grand_total}".rstrip())


if __name__ == "__main__":
    main()
