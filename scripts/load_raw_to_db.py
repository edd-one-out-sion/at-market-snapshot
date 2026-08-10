# data/raw/*.json → Supabase 적재기
# - aT v2: trd_clcln_ymd(정산일)를 sale_date로 그대로 사용
# - realtime은 잠정, origin은 확정이며 origin끼리 줄어들 때만 승인 관문 적용

from __future__ import annotations

import glob
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(ROOT, "data", "raw")
VALIDATION_DIR = os.path.join(ROOT, "data", "validation")
BATCH = 2000
KST = timezone(timedelta(hours=9))
AT_SOURCES = {"at-realtime", "at-origin"}
LOAD_SUMMARY_PATH = os.path.join(RAW_DIR, "_load_summary.json")


def _unquote(value):
    return value.strip().strip('"').strip("'")


def env():
    conf = {}
    env_path = os.environ.get("NONGSANMUL_ENV_FILE", os.path.join(ROOT, ".env"))
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as env_file:
            for line in env_file:
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.split("=", 1)
                    conf[key.strip()] = _unquote(value)
    for key in (
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
        "LOADER_DRY_RUN",
        "PLANNED_ITEM_ZERO",
        "PLANNED_ORIGIN_REPLACE",
    ):
        if os.environ.get(key):
            conf[key] = _unquote(os.environ[key])
    return conf


CONF = env()
BASE = None
HEADERS = None


def configure():
    """네트워크 호출 직전에만 접속 설정을 확정해 단위 테스트 import를 허용한다."""
    global BASE, HEADERS  # noqa: PLW0603 - 작은 실행 스크립트의 단일 클라이언트
    missing = [
        name
        for name in ("SUPABASE_URL", "SUPABASE_SERVICE_KEY")
        if not CONF.get(name)
    ]
    if missing:
        raise RuntimeError(f"필수 설정이 없습니다: {', '.join(missing)}")
    BASE = CONF["SUPABASE_URL"].rstrip("/") + "/rest/v1"
    HEADERS = {
        "apikey": CONF["SUPABASE_SERVICE_KEY"],
        "Authorization": "Bearer " + CONF["SUPABASE_SERVICE_KEY"],
        "Content-Type": "application/json",
        "User-Agent": "nongsanmul-loader/3.0",
    }


def req(method, path, body=None, prefer=None):
    if BASE is None or HEADERS is None:
        raise RuntimeError("Supabase 클라이언트가 설정되지 않았습니다")
    headers = dict(HEADERS)
    if prefer:
        headers["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        BASE + path, data=data, headers=headers, method=method
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                text = response.read().decode()
                return json.loads(text) if text else None
        except urllib.error.HTTPError as error:
            detail = error.read().decode()[:300]
            if attempt == 2:
                raise RuntimeError(
                    f"{method} {path} → {error.code}: {detail}"
                ) from error
            time.sleep(2 * (attempt + 1))
        except Exception:  # noqa: BLE001
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))


def count_for_market_date(iso_date, whsal_cd):
    """해당 정산일·시장의 기존 적재 건수를 Content-Range로 확인한다."""
    if BASE is None or HEADERS is None:
        raise RuntimeError("Supabase 클라이언트가 설정되지 않았습니다")
    headers = dict(HEADERS)
    headers["Prefer"] = "count=exact"
    headers["Range"] = "0-0"
    path = (
        f"{BASE}/auction_records?select=id&sale_date=eq.{iso_date}"
        f"&whsal_cd=eq.{whsal_cd}"
    )
    for attempt in range(3):
        request = urllib.request.Request(path, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                content_range = response.headers.get("Content-Range", "*/0")
                return int(content_range.split("/")[-1])
        except Exception as error:  # noqa: BLE001 - 읽기 전용 재시도
            if attempt == 2:
                raise RuntimeError(
                    f"기존 건수 조회 3회 실패: {iso_date} {whsal_cd}: {error}"
                ) from error
            time.sleep(2 * (attempt + 1))


def fetch_load_source_for_market_date(iso_date, whsal_cd):
    """해당 정산일·시장의 출처 판정에 필요한 한 행만 읽는다."""
    rows = req(
        "GET",
        f"/auction_records?select=load_source&sale_date=eq.{iso_date}"
        f"&whsal_cd=eq.{whsal_cd}&limit=1",
    )
    if not isinstance(rows, list) or len(rows) != 1:
        raise RuntimeError("기존 적재의 load_source 1행 조회에 실패했습니다")
    return _market_load_source(rows)


def fetch_existing_rows_for_market_date(iso_date, whsal_cd):
    """교체 보호와 origin 대조에 필요한 기존 행을 전 페이지 읽는다."""
    rows = []
    offset = 0
    page_size = 1000
    while True:
        page = req(
            "GET",
            f"/auction_records?select=large_code,mid_code,cost&sale_date=eq.{iso_date}"
            f"&whsal_cd=eq.{whsal_cd}&order=id.asc"
            f"&limit={page_size}&offset={offset}",
        )
        if not isinstance(page, list):
            raise RuntimeError("origin 정보성 대조 조회 응답이 배열이 아닙니다")
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def _new_load_summary():
    return {
        "groups_expected": 0,
        "ready": 0,
        "incomplete": 0,
        "skipped": 0,
        "holds": 0,
        "deleted_rows": 0,
        "loaded_rows": 0,
        "zero_row_runs": 0,
    }


def _write_load_summary(summary):
    with open(LOAD_SUMMARY_PATH, "w", encoding="utf-8") as summary_file:
        json.dump(summary, summary_file, ensure_ascii=False, indent=2)


def _append_load_step_summary(summary):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as step_summary:
        step_summary.write(
            "\n## 적재 요약\n\n"
            "| 항목 | 값 |\n"
            "|---|---:|\n"
            f"| 예상 날짜·시장 | {summary['groups_expected']} |\n"
            f"| 완전 후보 | {summary['ready']} |\n"
            f"| 불완전 후보 | {summary['incomplete']} |\n"
            f"| 건너뜀 | {summary['skipped']} |\n"
            f"| 교체 보류 | {summary['holds']} |\n"
            f"| 삭제 행 | {summary['deleted_rows']} |\n"
            f"| 적재 행 | {summary['loaded_rows']} |\n"
            f"| 0행 실행 | {summary['zero_row_runs']} |\n"
        )


def evaluate_gate(capture_summary, load_summary):
    errors = []
    warnings = []
    rows_by_source = capture_summary.get("rows_by_source") or {}
    captured_rows = sum(
        int(rows_by_source.get(source) or 0) for source in ("realtime", "origin")
    )
    loaded_rows = int(load_summary.get("loaded_rows") or 0)
    ready = int(load_summary.get("ready") or 0)
    incomplete = int(load_summary.get("incomplete") or 0)
    holds = int(load_summary.get("holds") or 0)
    stop_reason = capture_summary.get("stop_reason")

    protected_by_hold = holds > 0 and ready == holds + int(
        load_summary.get("skipped") or 0
    )
    if captured_rows > 0 and loaded_rows == 0 and not protected_by_hold:
        errors.append(f"수집 {captured_rows}행을 받았지만 적재 행이 0입니다")
    if stop_reason == "consecutive_failures":
        errors.append("Capture 연속 실패 차단기가 발동했습니다")
    if ready == 0 and incomplete > 0:
        errors.append(f"완전 후보가 없고 불완전 날짜·시장이 {incomplete}개입니다")

    if stop_reason == "time_budget":
        warnings.append("Capture가 시간 예산으로 조기 종료되었습니다")
    if holds > 0:
        warnings.append(f"교체 보류 날짜·시장이 {holds}개입니다")
    if captured_rows == 0 and loaded_rows == 0:
        warnings.append("수집·적재 행이 모두 0입니다(휴장일 가능)")
    if ready > 0 and incomplete > 0:
        warnings.append(f"일부 불완전 날짜·시장이 {incomplete}개입니다")
    return errors, warnings


def run_gate(raw_dir=RAW_DIR):
    summaries = {}
    required = {
        "capture": {
            "expected_bundles",
            "attempted",
            "success",
            "failed",
            "unattempted",
            "stop_reason",
            "api_attempts",
            "http_429_count",
            "rows_by_source",
        },
        "load": {
            "groups_expected",
            "ready",
            "incomplete",
            "skipped",
            "holds",
            "deleted_rows",
            "loaded_rows",
            "zero_row_runs",
        },
    }
    for name in ("capture", "load"):
        path = os.path.join(raw_dir, f"_{name}_summary.json")
        try:
            with open(path, encoding="utf-8") as summary_file:
                summary = json.load(summary_file)
            if not isinstance(summary, dict):
                raise ValueError("JSON object가 아닙니다")
            missing = sorted(required[name] - set(summary))
            if missing:
                raise ValueError("필수 필드 누락: " + ", ".join(missing))
            summaries[name] = summary
        except Exception as error:  # noqa: BLE001 - Gate는 모든 읽기 실패를 표시한다
            print(f"::error title=Snapshot gate::{name} 요약을 읽을 수 없습니다: {error}")
            return 1

    try:
        errors, warnings = evaluate_gate(summaries["capture"], summaries["load"])
    except Exception as error:  # noqa: BLE001 - 잘못된 계기판도 Gate 실패다
        print(f"::error title=Snapshot gate::요약 값이 잘못되었습니다: {error}")
        return 1
    for warning in warnings:
        print(f"::warning title=Snapshot gate::{warning}")
    for error in errors:
        print(f"::error title=Snapshot gate::{error}")
    if errors:
        return 1
    print("Gate 통과: 수집·적재 요약에 실패 조건이 없습니다")
    return 0


def load_collection_item_keys():
    rows = req(
        "GET",
        "/items?select=large_code,mid_code&collect=eq.true"
        "&order=large_code.asc,mid_code.asc",
    )
    keys = {
        (str(row.get("large_code") or ""), str(row.get("mid_code") or ""))
        for row in rows or []
    }
    if not keys or any(not large or not mid for large, mid in keys):
        raise RuntimeError("수집 품목 설정이 비었거나 코드가 잘못되었습니다")
    return keys


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_sbid_time(sbid):
    """aT API의 낙찰 시각을 KST aware datetime으로 해석한다."""
    parsed = datetime.fromisoformat(sbid.replace(" ", "T"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def _optional_float(value):
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def _optional_text(value):
    text = str(value or "").strip()
    return text or None


def at_to_record(row):
    """aT 행은 trd_clcln_ymd를 별도 cutoff 계산 없이 sale_date로 쓴다."""
    sale_date = datetime.strptime(row["trd_clcln_ymd"], "%Y-%m-%d").date()
    unit_qty = str(row.get("unit_qty") or "").strip()
    unit_name = str(row.get("unit_nm") or "").strip()
    package = _optional_text(row.get("pkg_nm"))
    std = f"{unit_qty}{unit_name}{package or ''}"
    weight = _optional_float(unit_qty) if unit_name.lower() == "kg" else None
    sbid = str(row.get("scsbd_dt") or "").strip()
    parsed_sbid = parse_sbid_time(sbid) if sbid else None
    return {
        "sale_date": sale_date.isoformat(),
        "whsal_cd": str(row.get("whsl_mrkt_cd") or ""),
        "cmp_cd": str(row.get("corp_cd") or ""),
        "large_code": str(row.get("gds_lclsf_cd") or ""),
        "mid_code": str(row.get("gds_mclsf_cd") or ""),
        "small_code": _optional_text(row.get("gds_sclsf_cd")),
        "small_name": _optional_text(row.get("gds_sclsf_nm")),
        "san_cd": _optional_text(row.get("plor_cd")),
        "san_name": _optional_text(row.get("plor_nm")),
        "cost": float(row["scsbd_prc"]),
        "qty": _optional_float(row.get("qty")),
        "std_raw": std,
        "weight_kg": weight,
        "package": package,
        "grade": _optional_text(row.get("grd_nm")),
        "size": _optional_text(row.get("sz_nm")),
        "sbid_time": parsed_sbid.isoformat() if parsed_sbid else None,
    }


CURRENT_FILENAME_RE = re.compile(
    r"^\d{8}_[^_]+_[^-]+-[^_]+_(?:realtime|origin)\.json$"
)


def capture_status(payload):
    rows = payload.get("rows") or []
    reported = int(payload.get("total_cnt") or 0)
    saved = int(payload.get("rows_saved", len(rows)))
    declared = payload.get("capture_status")
    if declared == "failed" or payload.get("error"):
        return "failed"
    if reported == len(rows) == saved:
        return "success"
    if reported > 0 and len(rows) == 0:
        return "failed"
    return "partial"


def _payload_source(payload):
    source = str(payload.get("source") or "")
    if source not in AT_SOURCES:
        raise RuntimeError(f"aT v2가 아닌 raw source는 적재할 수 없습니다: {source or '(없음)'}")
    return source


def _validate_payload_rows(filename, payload, key):
    expected_fields = {
        "trd_clcln_ymd": datetime.strptime(key[0], "%Y%m%d").date().isoformat(),
        "whsl_mrkt_cd": key[1],
        "gds_lclsf_cd": key[2],
        "gds_mclsf_cd": key[3],
    }
    for row in payload["rows"]:
        mismatch = {
            name: str(row.get(name) or "")
            for name, expected in expected_fields.items()
            if str(row.get(name) or "") != expected
        }
        if mismatch:
            raise RuntimeError(f"{filename} payload/row 불일치: {mismatch}")


def select_raw_payloads(files):
    """날짜·시장·품목·source별 raw 하나를 고르고 원문 불일치를 차단한다."""
    selected = {}
    for file_path in files:
        with open(file_path, encoding="utf-8") as raw_file:
            payload = json.load(raw_file)
        required = ("sale_date", "whsal_cd", "large", "mid", "rows", "total_cnt")
        missing = [name for name in required if name not in payload]
        if missing:
            raise RuntimeError(
                f"{os.path.basename(file_path)} 필수 필드 누락: {', '.join(missing)}"
            )
        source = _payload_source(payload)
        key = (
            str(payload["sale_date"]),
            str(payload["whsal_cd"]),
            str(payload["large"]),
            str(payload["mid"]),
            source,
        )
        _validate_payload_rows(os.path.basename(file_path), payload, key)
        payload["_capture_status"] = capture_status(payload)
        payload["_source"] = source
        priority = 1 if CURRENT_FILENAME_RE.match(os.path.basename(file_path)) else 0
        previous = selected.get(key)
        if previous is not None and previous[0] == priority:
            raise RuntimeError(
                f"동일 날짜·시장·품목·source raw가 중복입니다: "
                f"{os.path.basename(file_path)}"
            )
        if previous is None or priority > previous[0]:
            selected[key] = (priority, payload)
    return [entry[1] for entry in selected.values()]


def _post_run(body, prefer="return=minimal"):
    return req("POST", "/collection_runs", body, prefer=prefer)


def _median_rows(records):
    grouped = {}
    for record in records:
        key = (str(record.get("large_code") or ""), str(record.get("mid_code") or ""))
        grouped.setdefault(key, []).append(float(record["cost"]))
    return {
        key: {"count": len(costs), "median": statistics.median(costs)}
        for key, costs in grouped.items()
    }


def compare_origin_with_existing(
    choice, existing_rows, existing_source, existing_count=None
):
    existing_medians = _median_rows(existing_rows)
    origin_medians = _median_rows(choice["records"])
    items = []
    for key in sorted(set(existing_medians) | set(origin_medians)):
        existing = existing_medians.get(key, {"count": 0, "median": None})
        origin = origin_medians.get(key, {"count": 0, "median": None})
        delta_pct = None
        if existing["median"] not in {None, 0} and origin["median"] is not None:
            delta_pct = (
                100.0
                * (origin["median"] - existing["median"])
                / existing["median"]
            )
        items.append(
            {
                "item": f"{key[0]}-{key[1]}",
                "existing_count": existing["count"],
                "origin_count": origin["count"],
                "existing_median": existing["median"],
                "origin_median": origin["median"],
                "delta_pct": delta_pct,
            }
        )
    return {
        "date": datetime.strptime(choice["date"], "%Y%m%d").date().isoformat(),
        "market": choice["market"],
        "existing_source": existing_source,
        "existing_count": (
            len(existing_rows) if existing_count is None else existing_count
        ),
        "origin_count": len(choice["records"]),
        "origin_minus_existing": len(choice["records"])
        - (len(existing_rows) if existing_count is None else existing_count),
        "items": items,
    }


def write_origin_report(comparisons):
    """중앙값 차이는 일상 적재를 막지 않고 정보성 JSON으로만 남긴다."""
    os.makedirs(VALIDATION_DIR, exist_ok=True)
    dates = sorted(comparison["date"] for comparison in comparisons)
    label = f"{dates[0].replace('-', '')}-{dates[-1].replace('-', '')}"
    report_path = os.path.join(
        VALIDATION_DIR, f"collector_v2_{label}_origin_comparison.json"
    )
    report = {
        "generated_at": utc_now_iso(),
        "rule": (
            "existing item-price rows vs origin; "
            "dedup is not applied, so this differs from the app median; "
            "median deltas are informational and never block loading"
        ),
        "comparisons": comparisons,
    }
    with open(report_path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, ensure_ascii=False, indent=2)
    return report_path


def _candidate_state(payloads, expected_items):
    by_item = {
        (str(payload["large"]), str(payload["mid"])): payload
        for payload in payloads
    }
    missing = sorted(expected_items - set(by_item))
    extra = sorted(set(by_item) - expected_items)
    problems = []
    if missing:
        problems.append("누락 " + ",".join(f"{large}-{mid}" for large, mid in missing))
    if extra:
        problems.append("미등록 " + ",".join(f"{large}-{mid}" for large, mid in extra))
    for item_key, payload in sorted(by_item.items()):
        status = payload.get("_capture_status") or capture_status(payload)
        if status != "success":
            problems.append(f"{item_key[0]}-{item_key[1]}:{status}")
    return not problems, problems


def _record_item_counts(records):
    counts = {}
    for record in records or []:
        item_key = (
            str(record.get("large_code") or ""),
            str(record.get("mid_code") or ""),
        )
        counts[item_key] = counts.get(item_key, 0) + 1
    return counts


def _origin_item_deltas(existing_records, origin_records):
    before = _record_item_counts(existing_records)
    after = _record_item_counts(origin_records)
    return [
        {
            "item": f"{large}-{mid}",
            "before": before.get((large, mid), 0),
            "after": after.get((large, mid), 0),
            "delta": after.get((large, mid), 0) - before.get((large, mid), 0),
        }
        for large, mid in sorted(set(before) | set(after))
    ]


def _origin_item_delta_note(existing_records, origin_records):
    changes = _origin_item_deltas(existing_records, origin_records)
    return "origin_item_deltas=" + json.dumps(
        changes, ensure_ascii=False, separators=(",", ":")
    )


def _market_load_source(records):
    if not records:
        return None
    sources = {str(record.get("load_source") or "") for record in records}
    if len(sources) != 1 or "" in sources:
        raise RuntimeError(
            "기존 행의 load_source가 없거나 날짜·시장 안에서 섞였습니다: "
            + ",".join(sorted(sources))
        )
    return next(iter(sources))


def parse_origin_replace_keys(raw_value):
    text = str(raw_value or "").strip()
    if not text or text == "0":
        return set()
    if text.lower() in {"1", "true", "yes", "*"}:
        raise RuntimeError(
            "PLANNED_ORIGIN_REPLACE는 YYYY-MM-DD:시장코드 키만 허용합니다"
        )
    keys = set()
    for token in text.split(","):
        token = token.strip()
        try:
            date_text, market = token.rsplit(":", 1)
            datetime.strptime(date_text, "%Y-%m-%d")
        except (ValueError, TypeError) as error:
            raise RuntimeError(
                f"PLANNED_ORIGIN_REPLACE 키 형식 오류: {token}"
            ) from error
        if not market.isdigit():
            raise RuntimeError(
                f"PLANNED_ORIGIN_REPLACE 시장 코드 오류: {token}"
            )
        keys.add(f"{date_text}:{market}")
    return keys


def parse_item_zero_keys(raw_value):
    text = str(raw_value or "").strip()
    if not text or text == "0":
        return set()
    if text.lower() in {"1", "true", "yes", "*"}:
        raise RuntimeError(
            "PLANNED_ITEM_ZERO는 YYYY-MM-DD:시장코드:large-mid 키만 허용합니다"
        )
    keys = set()
    pattern = re.compile(
        r"^(?P<date>\d{4}-\d{2}-\d{2}):(?P<market>\d+):"
        r"(?P<large>\d+)-(?P<mid>\d+)$"
    )
    for token in text.split(","):
        token = token.strip()
        match = pattern.fullmatch(token)
        if not match:
            raise RuntimeError(f"PLANNED_ITEM_ZERO 키 형식 오류: {token}")
        try:
            datetime.strptime(match.group("date"), "%Y-%m-%d")
        except ValueError as error:
            raise RuntimeError(f"PLANNED_ITEM_ZERO 날짜 오류: {token}") from error
        keys.add(token)
    return keys


def _item_zero_override_key(iso_date, whsal_cd, item_key):
    return f"{iso_date}:{whsal_cd}:{item_key[0]}-{item_key[1]}"


def find_unplanned_item_zeros(
    existing_records,
    candidate_records,
    expected_items,
    iso_date,
    whsal_cd,
    planned_item_zero_keys=None,
):
    """기존 행이 있지만 완전 후보에서 0행이 된 미승인 품목을 찾는다."""
    before = _record_item_counts(existing_records)
    after = _record_item_counts(candidate_records)
    planned_item_zero_keys = planned_item_zero_keys or set()
    held = []
    for item_key in sorted(set(expected_items) & set(before)):
        if before[item_key] <= 0 or after.get(item_key, 0) != 0:
            continue
        override_key = _item_zero_override_key(iso_date, whsal_cd, item_key)
        if override_key not in planned_item_zero_keys:
            held.append(
                {
                    "item": f"{item_key[0]}-{item_key[1]}",
                    "existing": before[item_key],
                    "override_key": override_key,
                }
            )
    return held


def _at_records(payloads):
    records = []
    corps = {}
    for payload in payloads:
        for row in payload["rows"]:
            record = at_to_record(row)
            records.append(record)
            cmp_cd = str(row.get("corp_cd") or "")
            if cmp_cd:
                corps[cmp_cd] = str(row.get("corp_nm") or "").strip()
    return records, corps


def _prepare_at_choices(
    payloads, expected_items, only_date=None, date_from=None, date_to=None
):
    if only_date:
        datetime.strptime(only_date, "%Y%m%d")
    if date_from:
        datetime.strptime(date_from, "%Y%m%d")
    if date_to:
        datetime.strptime(date_to, "%Y%m%d")
    if date_from and date_to and date_from > date_to:
        raise RuntimeError("적재 시작일은 종료일보다 늦을 수 없습니다")
    grouped = {}
    for payload in payloads:
        source = _payload_source(payload)
        date_key = str(payload["sale_date"])
        if only_date and date_key != only_date:
            continue
        if date_from and date_key < date_from:
            continue
        if date_to and date_key > date_to:
            continue
        key = (date_key, str(payload["whsal_cd"]))
        grouped.setdefault(key, {}).setdefault(source, []).append(payload)

    choices = []
    incomplete = []
    for (date_key, market), sources in sorted(grouped.items(), reverse=True):
        origin = sources.get("at-origin")
        realtime = sources.get("at-realtime")
        chosen_source = None
        chosen_payloads = None
        problems = []
        if origin is not None:
            complete, origin_problems = _candidate_state(origin, expected_items)
            if complete:
                chosen_source, chosen_payloads = "at-origin", origin
            else:
                problems.extend("origin " + problem for problem in origin_problems)
        if chosen_source is None and realtime is not None:
            complete, realtime_problems = _candidate_state(realtime, expected_items)
            if complete:
                chosen_source, chosen_payloads = "at-realtime", realtime
            else:
                problems.extend("realtime " + problem for problem in realtime_problems)
        if chosen_source is None:
            incomplete.append(
                {"date": date_key, "market": market, "problems": problems}
            )
            continue
        records, corps = _at_records(chosen_payloads)
        fallback_note = None
        if chosen_source == "at-realtime" and origin is not None:
            fallback_note = "origin 보류 후 realtime 폴백: " + ", ".join(
                problems or ["origin 확정본 사용 불가"]
            )
        choices.append(
            {
                "date": date_key,
                "market": market,
                "source": chosen_source,
                "records": records,
                "corps": corps,
                "note": fallback_note,
            }
        )
    return choices, incomplete


def _record_noop_run(iso_date, whsal_cd, reported, message, dry_run=False):
    if not dry_run:
        now = utc_now_iso()
        _post_run(
            {
                "started_at": now,
                "finished_at": now,
                "target_date": iso_date,
                "whsal_cd": whsal_cd,
                "status": "success",
                "rows_loaded": 0,
                "total_cnt_reported": reported,
                "error_msg": message,
            }
        )
    print(f"{iso_date} {whsal_cd}: [SKIP] {message}")


def _record_partial_run(iso_date, whsal_cd, reported, message, dry_run=False):
    if not dry_run:
        now = utc_now_iso()
        _post_run(
            {
                "started_at": now,
                "finished_at": now,
                "target_date": iso_date,
                "whsal_cd": whsal_cd,
                "status": "partial",
                "rows_loaded": 0,
                "total_cnt_reported": reported,
                "error_msg": message,
            }
        )


def _replace_market_date(
    iso_date, whsal_cd, records, corps, existing, source, advisory=None
):
    if not records and existing > 0:
        message = f"{source} 신규 총 0행, 미공개 추정으로 기존 {existing}행 유지"
        now = utc_now_iso()
        _post_run(
            {
                "started_at": now,
                "finished_at": now,
                "target_date": iso_date,
                "whsal_cd": whsal_cd,
                "status": "partial",
                "rows_loaded": 0,
                "total_cnt_reported": 0,
                "error_msg": message,
            }
        )
        print(f"{iso_date} {whsal_cd}: [WARN] {message}")
        return 0
    if corps:
        req(
            "POST",
            "/corporations?on_conflict=cmp_cd",
            [
                {
                    "cmp_cd": code,
                    "cmp_name": name or code,
                    "whsal_cd": whsal_cd,
                }
                for code, name in sorted(corps.items())
            ],
            prefer="resolution=ignore-duplicates,return=minimal",
        )
    run = _post_run(
        {
            "started_at": utc_now_iso(),
            "target_date": iso_date,
            "whsal_cd": whsal_cd,
            "status": "partial",
            "total_cnt_reported": len(records),
            "error_msg": advisory,
        },
        prefer="return=representation",
    )
    run_id = run[0]["id"]
    for record in records:
        record["run_id"] = run_id
        record["load_source"] = source
    if existing > 0:
        req(
            "DELETE",
            f"/auction_records?sale_date=eq.{iso_date}&whsal_cd=eq.{whsal_cd}",
        )
    loaded = 0
    for start in range(0, len(records), BATCH):
        batch = records[start : start + BATCH]
        if batch:
            req("POST", "/auction_records", batch, prefer="return=minimal")
            loaded += len(batch)
    req(
        "PATCH",
        f"/collection_runs?id=eq.{run_id}",
        {
            "finished_at": utc_now_iso(),
            "status": "success",
            "rows_loaded": loaded,
            "error_msg": advisory,
        },
        prefer="return=minimal",
    )
    print(
        f"{iso_date} {whsal_cd}: {loaded}건 정산일 적재 "
        f"(source={source}, 기존 {existing} 교체)"
    )
    return loaded


def _at_load_payloads(
    payloads,
    expected_items,
    only_date=None,
    dry_run=False,
    planned_origin_replace_keys=None,
    planned_item_zero_keys=None,
    date_from=None,
    date_to=None,
    summary=None,
):
    choices, incomplete = _prepare_at_choices(
        payloads,
        expected_items,
        only_date=only_date,
        date_from=date_from,
        date_to=date_to,
    )
    initial_incomplete = list(incomplete)
    summary = summary if summary is not None else _new_load_summary()
    summary["groups_expected"] = len(choices) + len(initial_incomplete)
    summary["ready"] = len(choices)
    summary["incomplete"] = len(initial_incomplete)
    planned_origin_replace_keys = planned_origin_replace_keys or set()
    planned_item_zero_keys = planned_item_zero_keys or set()
    existing_by_key = {}
    comparisons = []
    ready_choices = []
    skipped_choices = []
    held_choices = []
    for choice in choices:
        iso_date = datetime.strptime(choice["date"], "%Y%m%d").date().isoformat()
        try:
            existing = count_for_market_date(iso_date, choice["market"])
            existing_source = (
                fetch_load_source_for_market_date(iso_date, choice["market"])
                if existing > 0
                else None
            )
            existing_records = (
                fetch_existing_rows_for_market_date(iso_date, choice["market"])
                if existing > 0
                else []
            )
        except Exception as error:  # noqa: BLE001 - 이 날짜·시장만 보류한다
            held_choices.append(
                {
                    "date": choice["date"],
                    "market": choice["market"],
                    "reported": len(choice["records"]),
                    "problems": [f"기존 적재 상태 확인 실패: {error}"],
                }
            )
            continue
        existing_by_key[(choice["date"], choice["market"])] = existing
        if choice["source"] == "at-realtime" and existing_source == "at-origin":
            reason = f"; {choice['note']}" if choice.get("note") else ""
            skipped_choices.append(
                {
                    "date": choice["date"],
                    "market": choice["market"],
                    "reported": len(choice["records"]),
                    "message": (
                        "realtime 적재 건너뜀: 기존 at-origin 확정본 "
                        f"{existing}건 유지{reason}"
                    ),
                }
            )
            continue

        unplanned_zeros = find_unplanned_item_zeros(
            existing_records,
            choice["records"],
            expected_items,
            iso_date,
            choice["market"],
            planned_item_zero_keys,
        )
        if unplanned_zeros:
            details = "; ".join(
                f"{item['item']} 기존 {item['existing']}행, "
                f"PLANNED_ITEM_ZERO={item['override_key']} 필요"
                for item in unplanned_zeros
            )
            held_choices.append(
                {
                    "date": choice["date"],
                    "market": choice["market"],
                    "reported": len(choice["records"]),
                    "problems": [f"success·0행 품목 교체 보류: {details}"],
                    "item_zero": True,
                }
            )
            continue
        if choice["source"] == "at-origin":
            if not choice["records"] and existing > 0:
                held_choices.append(
                    {
                        "date": choice["date"],
                        "market": choice["market"],
                        "reported": 0,
                        "problems": [
                            "origin 총 0행, 미공개 추정으로 기존 "
                            f"{existing}행 유지"
                        ],
                    }
                )
                continue
            approval_key = f"{iso_date}:{choice['market']}"
            needs_approval = (
                existing_source == "at-origin"
                and len(choice["records"]) < existing
            )
            if needs_approval and approval_key not in planned_origin_replace_keys:
                held_choices.append(
                    {
                        "date": choice["date"],
                        "market": choice["market"],
                        "reported": len(choice["records"]),
                        "problems": [
                            "origin끼리 건수 감소 교체 보류: "
                            f"기존 {existing} > "
                            f"신규 {len(choice['records'])}; "
                            f"PLANNED_ORIGIN_REPLACE={approval_key} 필요"
                        ],
                    }
                )
                continue
            comparison = compare_origin_with_existing(
                choice,
                existing_records,
                existing_source,
                existing_count=existing,
            )
            comparison["item_count_changes"] = _origin_item_deltas(
                existing_records, choice["records"]
            )
            comparison["approval_key"] = approval_key if needs_approval else None
            comparison["approved_shrink"] = (
                needs_approval and approval_key in planned_origin_replace_keys
            )
            comparisons.append(comparison)
            choice["note"] = _origin_item_delta_note(
                existing_records, choice["records"]
            )
        ready_choices.append(choice)
    choices = ready_choices
    summary["skipped"] = len(skipped_choices)
    summary["holds"] = len(held_choices)

    report_path = None
    if comparisons:
        report_path = write_origin_report(comparisons)

    grand = 0
    for failure in initial_incomplete:
        iso_date = datetime.strptime(failure["date"], "%Y%m%d").date().isoformat()
        message = "aT 교체 보류: " + ", ".join(
            failure["problems"] or ["완전한 source 없음"]
        )
        _record_partial_run(
            iso_date,
            failure["market"],
            failure.get("reported", 0),
            message,
            dry_run=dry_run,
        )
        print(f"{failure['date']} {failure['market']}: [WARN] {message}")
        if not dry_run:
            summary["zero_row_runs"] += 1

    for held in held_choices:
        iso_date = datetime.strptime(held["date"], "%Y%m%d").date().isoformat()
        message = "aT 교체 보류: " + ", ".join(held["problems"])
        _record_partial_run(
            iso_date,
            held["market"],
            held.get("reported", 0),
            message,
            dry_run=dry_run,
        )
        label = "HOLD" if held.get("item_zero") else "WARN"
        print(f"{held['date']} {held['market']}: [{label}] {message}")
        if not dry_run:
            summary["zero_row_runs"] += 1

    for skipped in skipped_choices:
        iso_date = datetime.strptime(skipped["date"], "%Y%m%d").date().isoformat()
        _record_noop_run(
            iso_date,
            skipped["market"],
            skipped["reported"],
            skipped["message"],
            dry_run=dry_run,
        )
        if not dry_run:
            summary["zero_row_runs"] += 1

    for choice in choices:
        iso_date = datetime.strptime(choice["date"], "%Y%m%d").date().isoformat()
        existing = existing_by_key[(choice["date"], choice["market"])]
        if dry_run:
            print(
                f"{choice['date']} {choice['market']}: [DRY-RUN {choice['source']}] "
                f"기존 {existing}, 신규 {len(choice['records'])}"
            )
            grand += len(choice["records"])
            continue
        loaded = _replace_market_date(
            iso_date,
            choice["market"],
            choice["records"],
            choice["corps"],
            existing,
            choice["source"],
            choice.get("note"),
        )
        grand += loaded
        summary["loaded_rows"] += loaded
        if existing > 0 and choice["records"]:
            summary["deleted_rows"] += existing
        if loaded == 0:
            summary["zero_row_runs"] += 1
    if report_path:
        print(f"origin 정보성 대조표: {report_path}")
    return grand


def load_payloads(
    payloads,
    only_date=None,
    *,
    expected_items=None,
    dry_run=False,
    planned_origin_replace_keys=None,
    planned_item_zero_keys=None,
    date_from=None,
    date_to=None,
    summary=None,
):
    """aT v2 raw를 정산일 기준으로 검증하고 적재한다."""
    if not expected_items:
        raise RuntimeError("at-v2 적재에는 collect=true 품목 목록이 필요합니다")
    return _at_load_payloads(
        payloads,
        set(expected_items),
        only_date=only_date,
        dry_run=dry_run,
        planned_origin_replace_keys=planned_origin_replace_keys,
        planned_item_zero_keys=planned_item_zero_keys,
        date_from=date_from,
        date_to=date_to,
        summary=summary,
    )


def main():
    if sys.argv[1:] == ["--gate"]:
        raise SystemExit(run_gate())
    if os.path.exists(LOAD_SUMMARY_PATH):
        os.remove(LOAD_SUMMARY_PATH)
    configure()
    if len(sys.argv) > 3:
        raise RuntimeError(
            "사용: load_raw_to_db.py [정산일 YYYYMMDD] 또는 [시작일 종료일]"
        )
    only_date = sys.argv[1] if len(sys.argv) == 2 else None
    date_from = sys.argv[1] if len(sys.argv) == 3 else None
    date_to = sys.argv[2] if len(sys.argv) == 3 else None
    files = sorted(
        path
        for path in glob.glob(os.path.join(RAW_DIR, "*.json"))
        if not os.path.basename(path).startswith("_")
    )
    payloads = select_raw_payloads(files)
    dry_run = CONF.get("LOADER_DRY_RUN", "0").lower() in {"1", "true", "yes"}
    planned_origin_replace_keys = parse_origin_replace_keys(
        CONF.get("PLANNED_ORIGIN_REPLACE")
    )
    planned_item_zero_keys = parse_item_zero_keys(CONF.get("PLANNED_ITEM_ZERO"))
    expected_items = load_collection_item_keys()
    summary = _new_load_summary()
    grand = load_payloads(
        payloads,
        only_date=only_date,
        expected_items=expected_items,
        dry_run=dry_run,
        planned_origin_replace_keys=planned_origin_replace_keys,
        planned_item_zero_keys=planned_item_zero_keys,
        date_from=date_from,
        date_to=date_to,
        summary=summary,
    )
    _write_load_summary(summary)
    _append_load_step_summary(summary)
    prefix = "검증 완료" if dry_run else "완료"
    print(f"{prefix}: 총 {grand}건")


if __name__ == "__main__":
    main()
