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
MORNING_BASELINE_CRONS = {"17 20 * * *", "7 22 * * *"}
KNOWN_SCHEDULE_CRONS = {
    "17 17 * * *",
    "7 19 * * *",
    "17 20 * * *",
    "7 22 * * *",
    "7 3 * * *",
    "7 12 * * *",
}


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
        "PLANNED_CLOSURE",
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
        "morning_baseline": {
            "required": False,
            "rule": "all_active_markets_strict",
            "passed": True,
            "expected_pairs": 0,
            "actual_pairs": 0,
            "targets": [],
        },
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
        baseline = summary["morning_baseline"]
        if baseline.get("required"):
            step_summary.write(
                "\n## 아침 기준선\n\n"
                f"- 규칙: `{baseline['rule']}`\n"
                f"- 시장·날짜 충족: {baseline['actual_pairs']}/"
                f"{baseline['expected_pairs']}\n"
                f"- 결과: {'통과' if baseline.get('passed') else '미달'}\n"
            )
            for target in baseline.get("targets") or []:
                market_rows = ", ".join(
                    (
                        f"{market['market']}=휴장({market['closed_reason']})"
                        if market.get("closed_reason")
                        else f"{market['market']}={market['rows']}행"
                    )
                    for market in target.get("markets") or []
                )
                step_summary.write(
                    f"- {target['date']} {target['source']}: {market_rows}\n"
                )
                zero_markets = target.get("success_zero_markets") or []
                if zero_markets:
                    step_summary.write(
                        "  - 참고: API 성공+0행 관측(휴장 면제 아님): "
                        + ", ".join(zero_markets)
                        + "\n"
                    )


def morning_baseline_required(*, event_name=None, event_path=None, now=None):
    """GitHub 이벤트 원문과 KST 시각으로 아침 기준선 무장 여부를 정한다."""
    event_name = (
        os.environ.get("GITHUB_EVENT_NAME", "")
        if event_name is None
        else str(event_name)
    )
    if event_name == "schedule":
        event_path = (
            os.environ.get("GITHUB_EVENT_PATH", "")
            if event_path is None
            else str(event_path)
        )
        if not event_path:
            raise RuntimeError("schedule 이벤트의 GITHUB_EVENT_PATH가 없습니다")
        try:
            with open(event_path, encoding="utf-8") as event_file:
                payload = json.load(event_file)
        except Exception as error:  # noqa: BLE001 - 무장 정보 부재는 fail-closed
            raise RuntimeError(
                f"schedule 이벤트 payload를 읽을 수 없습니다: {type(error).__name__}"
            ) from error
        schedule = str(payload.get("schedule") or "")
        if schedule not in KNOWN_SCHEDULE_CRONS:
            raise RuntimeError(
                f"등록되지 않은 schedule cron입니다: {schedule or '(없음)'}"
            )
        return schedule in MORNING_BASELINE_CRONS
    if event_name == "workflow_dispatch":
        current = now or datetime.now(KST)
        if current.tzinfo is None:
            current = current.replace(tzinfo=KST)
        current = current.astimezone(KST)
        return (current.hour, current.minute) >= (4, 30) and current.hour < 12
    return False


def _expected_timeout_is_safe(capture_summary, load_summary):
    rows_by_source = capture_summary.get("rows_by_source") or {}
    captured_rows = sum(
        int(rows_by_source.get(source) or 0) for source in ("realtime", "origin")
    )
    attempted = int(capture_summary.get("attempted") or 0)
    failed = int(capture_summary.get("failed") or 0)
    return (
        capture_summary.get("stop_reason") == "consecutive_failures"
        and attempted > 0
        and failed == attempted
        and int(capture_summary.get("success") or 0) == 0
        and int(capture_summary.get("timeout_failures") or 0) == failed
        and int(capture_summary.get("non_timeout_failures") or 0) == 0
        and captured_rows == 0
        and int(load_summary.get("ready") or 0) == 0
        and int(load_summary.get("incomplete") or 0) > 0
        and int(load_summary.get("skipped") or 0) == 0
        and int(load_summary.get("holds") or 0) == 0
        and int(load_summary.get("deleted_rows") or 0) == 0
        and int(load_summary.get("loaded_rows") or 0) == 0
    )


def _baseline_missing_pairs(baseline):
    missing = []
    for target in baseline.get("targets") or []:
        for market in target.get("markets") or []:
            if not market.get("closed_reason") and not market.get("present"):
                missing.append(
                    f"{target['date']}:{market['market']}({target['source']})"
                )
    return missing


def evaluate_gate(
    capture_summary,
    load_summary,
    *,
    backup_outcome=None,
):
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
    expected_timeout = _expected_timeout_is_safe(capture_summary, load_summary)

    protected_by_hold = holds > 0 and ready == holds + int(
        load_summary.get("skipped") or 0
    )
    if captured_rows > 0 and loaded_rows == 0 and not protected_by_hold:
        errors.append(f"수집 {captured_rows}행을 받았지만 적재 행이 0입니다")
    if stop_reason == "consecutive_failures" and not expected_timeout:
        errors.append("Capture 연속 실패 차단기가 발동했습니다")
    if ready == 0 and incomplete > 0 and not expected_timeout:
        errors.append(f"완전 후보가 없고 불완전 날짜·시장이 {incomplete}개입니다")
    if expected_timeout:
        warnings.append(
            "API 전면 timeout 차단기 수집 0행이며 DB 삭제·적재 0행이라 "
            "기존 데이터를 유지했습니다"
        )

    baseline = load_summary.get("morning_baseline") or {}
    if baseline.get("required") and not baseline.get("passed"):
        missing = _baseline_missing_pairs(baseline)
        detail = ", ".join(missing) if missing else str(
            baseline.get("error") or "판정 실패"
        )
        errors.append(
            "아침 기준선 미달: "
            f"{baseline.get('actual_pairs', 0)}/"
            f"{baseline.get('expected_pairs', 0)}; {detail}"
        )

    if backup_outcome != "success":
        errors.append(
            "비공개 백업 결과가 success가 아닙니다"
            f"(outcome={backup_outcome or 'missing'})"
        )

    if stop_reason == "time_budget":
        warnings.append("Capture가 시간 예산으로 조기 종료되었습니다")
    if holds > 0:
        warnings.append(f"교체 보류 날짜·시장이 {holds}개입니다")
    if captured_rows == 0 and loaded_rows == 0 and not expected_timeout:
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
            "timeout_failures",
            "non_timeout_failures",
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
            "morning_baseline",
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
        errors, warnings = evaluate_gate(
            summaries["capture"],
            summaries["load"],
            backup_outcome=os.environ.get("BACKUP_OUTCOME"),
        )
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


def load_collection_market_codes():
    rows = req(
        "GET",
        "/markets?select=whsal_cd&collect=eq.true&order=display_order.asc",
    )
    codes = [str(row.get("whsal_cd") or "") for row in rows or []]
    if not codes or any(not code for code in codes) or len(codes) != len(set(codes)):
        raise RuntimeError("수집 시장 설정이 비었거나 코드가 잘못되었습니다")
    return codes


def count_for_market_date_source(iso_date, whsal_cd, source):
    """기준선용 정산일·시장·source 행수를 Content-Range로 확인한다."""
    if source not in AT_SOURCES:
        raise RuntimeError(f"기준선 source가 잘못되었습니다: {source}")
    if BASE is None or HEADERS is None:
        raise RuntimeError("Supabase 클라이언트가 설정되지 않았습니다")
    headers = dict(HEADERS)
    headers["Prefer"] = "count=exact"
    headers["Range"] = "0-0"
    path = (
        f"{BASE}/auction_records?select=id&sale_date=eq.{iso_date}"
        f"&whsal_cd=eq.{whsal_cd}&load_source=eq.{source}"
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
                    "기준선 행수 조회 3회 실패: "
                    f"{iso_date} {whsal_cd} {source}: {type(error).__name__}"
                ) from error
            time.sleep(2 * (attempt + 1))


def _success_zero_markets(
    payloads,
    compact_date,
    source,
    market_codes,
    expected_items,
):
    observed = {market: {} for market in market_codes}
    for payload in payloads:
        if str(payload.get("sale_date") or "") != compact_date:
            continue
        if _payload_source(payload) != source:
            continue
        market = str(payload.get("whsal_cd") or "")
        if market not in observed:
            continue
        item_key = (
            str(payload.get("large") or ""),
            str(payload.get("mid") or ""),
        )
        observed[market][item_key] = payload
    zero_markets = []
    for market in market_codes:
        by_item = observed[market]
        if set(by_item) != set(expected_items):
            continue
        if all(
            (payload.get("_capture_status") or capture_status(payload)) == "success"
            and int(payload.get("total_cnt") or 0) == 0
            and len(payload.get("rows") or []) == 0
            for payload in by_item.values()
        ):
            zero_markets.append(market)
    return zero_markets


def _planned_closure_reason(day, market, planned_closure_keys):
    if day.weekday() == 6:
        return "sunday"
    date_key = day.isoformat()
    if (
        date_key in planned_closure_keys
        or f"{date_key}:{market}" in planned_closure_keys
    ):
        return "planned-closure"
    return None


def evaluate_morning_baseline(
    payloads,
    expected_items,
    market_codes,
    *,
    required,
    planned_closure_keys=None,
    now=None,
):
    result = {
        "required": bool(required),
        "rule": "all_active_markets_strict",
        "passed": True,
        "expected_pairs": 0,
        "actual_pairs": 0,
        "targets": [],
    }
    if not required:
        return result
    planned_closure_keys = planned_closure_keys or set()

    current = now or datetime.now(KST)
    if current.tzinfo is None:
        current = current.replace(tzinfo=KST)
    today = current.astimezone(KST).date()
    target_specs = (
        (today, "at-realtime"),
        (today - timedelta(days=1), "at-origin"),
    )
    for day, source in target_specs:
        compact_date = day.strftime("%Y%m%d")
        target = {
            "date": day.isoformat(),
            "source": source,
            "success_zero_markets": _success_zero_markets(
                payloads,
                compact_date,
                source,
                market_codes,
                expected_items,
            ),
            "markets": [],
        }
        for market in market_codes:
            closed_reason = _planned_closure_reason(
                day, market, planned_closure_keys
            )
            if closed_reason:
                target["markets"].append(
                    {
                        "market": market,
                        "rows": 0,
                        "present": True,
                        "closed_reason": closed_reason,
                    }
                )
                continue
            result["expected_pairs"] += 1
            rows = count_for_market_date_source(day.isoformat(), market, source)
            present = rows > 0
            result["actual_pairs"] += int(present)
            target["markets"].append(
                {
                    "market": market,
                    "rows": rows,
                    "present": present,
                    "closed_reason": None,
                }
            )
        result["targets"].append(target)

    result["passed"] = result["actual_pairs"] == result["expected_pairs"]
    return result


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


def parse_planned_closure_keys(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return set()
    keys = set()
    pattern = re.compile(
        r"^(?P<date>\d{4}-\d{2}-\d{2})(?::(?P<market>\d+))?$"
    )
    for token in text.split(","):
        token = token.strip()
        match = pattern.fullmatch(token)
        if not match:
            raise RuntimeError(f"PLANNED_CLOSURE 키 형식 오류: {token}")
        try:
            datetime.strptime(match.group("date"), "%Y-%m-%d")
        except ValueError as error:
            raise RuntimeError(f"PLANNED_CLOSURE 날짜 오류: {token}") from error
        keys.add(token)
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
        return {"deleted": 0, "inserted": 0}
    if not records:
        # 신규·기존 모두 0행(휴장일·미래일) — staging·RPC 호출 자체가 불필요하다
        now = utc_now_iso()
        _post_run(
            {
                "started_at": now,
                "finished_at": now,
                "target_date": iso_date,
                "whsal_cd": whsal_cd,
                "status": "success",
                "rows_loaded": 0,
                "total_cnt_reported": 0,
                "error_msg": advisory,
            }
        )
        print(f"{iso_date} {whsal_cd}: 신규·기존 모두 0행, 교체 생략")
        return {"deleted": 0, "inserted": 0}
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

    try:
        for start in range(0, len(records), BATCH):
            batch = records[start : start + BATCH]
            if batch:
                req(
                    "POST",
                    "/auction_records_staging",
                    batch,
                    prefer="return=minimal",
                )
        replace_result = req(
            "POST",
            "/rpc/replace_market_date_atomic",
            {
                "p_sale_date": iso_date,
                "p_whsal_cd": whsal_cd,
                "p_run_id": run_id,
            },
        )
    except Exception as error:  # noqa: BLE001 - 실행 실패를 run에 남긴 뒤 전파한다
        message = (
            f"{source} staging/RPC 적재 실패: {error}; 기존 auction_records는 "
            "변경되지 않았고 staging 잔재는 cleanup 대상입니다"
        )
        try:
            req(
                "PATCH",
                f"/collection_runs?id=eq.{run_id}",
                {
                    "finished_at": utc_now_iso(),
                    "status": "partial",
                    "rows_loaded": 0,
                    "error_msg": message,
                },
                prefer="return=minimal",
            )
        except Exception as patch_error:  # noqa: BLE001 - 원래 실패를 보존한다
            message += f"; collection_runs 실패 기록도 실패: {patch_error}"
        raise RuntimeError(message) from error

    valid_counts = (
        isinstance(replace_result, dict)
        and isinstance(replace_result.get("deleted"), int)
        and not isinstance(replace_result.get("deleted"), bool)
        and replace_result["deleted"] >= 0
        and isinstance(replace_result.get("inserted"), int)
        and not isinstance(replace_result.get("inserted"), bool)
        and replace_result["inserted"] >= 0
    )
    replace_error = (
        not valid_counts
        or "error" in replace_result
        or replace_result["inserted"] != len(records)
    )
    if replace_error:
        returned = json.dumps(replace_result, ensure_ascii=False, default=str)
        message = (
            "원자 교체 RPC 반환 검증 실패: "
            f"expected_inserted={len(records)}, returned={returned}; "
            "RPC가 정상 반환되어 트랜잭션은 이미 커밋되었으므로 자동 복구하지 않습니다"
        )
        inserted = (
            replace_result.get("inserted")
            if isinstance(replace_result, dict)
            and isinstance(replace_result.get("inserted"), int)
            and not isinstance(replace_result.get("inserted"), bool)
            and replace_result["inserted"] >= 0
            else 0
        )
        req(
            "PATCH",
            f"/collection_runs?id=eq.{run_id}",
            {
                "finished_at": utc_now_iso(),
                "status": "partial",
                "rows_loaded": inserted,
                "error_msg": message,
            },
            prefer="return=minimal",
        )
        raise RuntimeError(message)

    deleted = replace_result["deleted"]
    inserted = replace_result["inserted"]
    try:
        req(
            "PATCH",
            f"/collection_runs?id=eq.{run_id}",
            {
                "finished_at": utc_now_iso(),
                "status": "success",
                "rows_loaded": inserted,
                "error_msg": advisory,
            },
            prefer="return=minimal",
        )
    except Exception as error:  # noqa: BLE001 - RPC 결과는 이미 커밋되었다
        raise RuntimeError(
            "원자 교체 RPC는 커밋되었지만 collection_runs 성공 기록에 실패했습니다; "
            f"자동 복구하지 않습니다: {error}"
        ) from error
    print(
        f"{iso_date} {whsal_cd}: {inserted}건 정산일 적재 "
        f"(source={source}, RPC 삭제 {deleted})"
    )
    return {"deleted": deleted, "inserted": inserted}


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
    load_errors = []
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
        try:
            replace_result = _replace_market_date(
                iso_date,
                choice["market"],
                choice["records"],
                choice["corps"],
                existing,
                choice["source"],
                choice.get("note"),
            )
        except Exception as error:  # noqa: BLE001 - 다른 날짜·시장 적재는 계속한다
            load_errors.append(f"{iso_date} {choice['market']}: {error}")
            summary["zero_row_runs"] += 1
            print(f"{iso_date} {choice['market']}: [ERROR] {error}")
            continue
        inserted = replace_result["inserted"]
        grand += inserted
        summary["loaded_rows"] += inserted
        summary["deleted_rows"] += replace_result["deleted"]
        if inserted == 0:
            summary["zero_row_runs"] += 1
    if report_path:
        print(f"origin 정보성 대조표: {report_path}")
    if load_errors:
        raise RuntimeError("날짜·시장 적재 실패: " + " | ".join(load_errors))
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
    try:
        cleaned = req(
            "POST",
            "/rpc/cleanup_stale_staging",
            {"p_hours": 24},
        )
        print(f"staging cleanup: {cleaned}건 삭제")
    except Exception as error:  # noqa: BLE001 - cleanup 실패는 적재를 막지 않는다
        print(f"::warning title=Staging cleanup::24시간 초과 행 정리 실패: {error}")
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
    planned_closure_keys = parse_planned_closure_keys(
        CONF.get("PLANNED_CLOSURE")
    )
    planned_item_zero_keys = parse_item_zero_keys(CONF.get("PLANNED_ITEM_ZERO"))
    expected_items = load_collection_item_keys()
    market_codes = load_collection_market_codes()
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
    baseline_required = False
    try:
        baseline_required = morning_baseline_required()
        summary["morning_baseline"] = evaluate_morning_baseline(
            payloads,
            expected_items,
            market_codes,
            required=baseline_required,
            planned_closure_keys=planned_closure_keys,
        )
    except Exception as error:  # noqa: BLE001 - Gate가 빨간불로 판정할 근거를 남긴다
        summary["morning_baseline"] = {
            "required": True,
            "rule": "all_active_markets_strict",
            "passed": False,
            "expected_pairs": 0,
            "actual_pairs": 0,
            "targets": [],
            "error": f"{type(error).__name__}: {error}",
        }
    _write_load_summary(summary)
    _append_load_step_summary(summary)
    prefix = "검증 완료" if dry_run else "완료"
    print(f"{prefix}: 총 {grand}건")


if __name__ == "__main__":
    main()
