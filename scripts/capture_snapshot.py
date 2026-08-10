# 경락 데이터 스냅샷 수집기 (일회성/재실행 가능)
# - aT katRealTime2(당일 잠정) + katOrigin(익일 확정) 전용
# 사용: python scripts/capture_snapshot.py [시작일 YYYYMMDD] [종료일 YYYYMMDD]

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(ROOT, "data", "raw")
PAGE = 1000
KST = timezone(timedelta(hours=9))
AT_ENDPOINTS = {
    "realtime": "https://apis.data.go.kr/B552845/katRealTime2/trades2",
    "origin": "https://apis.data.go.kr/B552845/katOrigin/trades",
}
AT_KEY_PARAMETER = "service" + "Key"
TIME_BUDGET_ERROR = "시간 예산 초과"
CAPTURE_SUMMARY_PATH = os.path.join(RAW_DIR, "_capture_summary.json")
API_ATTEMPTS = 0
HTTP_429_COUNT = 0


class CaptureTimeBudgetExceeded(RuntimeError):
    """전체 수집 시간 예산이 끝났음을 상위 순회에 알린다."""


def _nonnegative_env_int(name, default):
    """환경변수를 0 이상의 정수로 읽고 잘못된 값은 기본값으로 돌린다."""
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def _time_budget_exceeded(deadline):
    return deadline is not None and time.monotonic() >= deadline


def _unquote(value: str) -> str:
    return value.strip().strip('"').strip("'")


def load_config():
    """클라우드에서는 환경변수, 로컬에서는 지정된 .env 설정을 읽는다."""
    config = {}
    env_path = os.environ.get("NONGSANMUL_ENV_FILE", os.path.join(ROOT, ".env"))
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as env_file:
            for line in env_file:
                if "=" in line and not line.lstrip().startswith("#"):
                    name, value = line.split("=", 1)
                    config[name.strip()] = _unquote(value)
    for name in (
        "AT_CAPTURE_SOURCES",
        "DATA_GO_KR_API_KEY_ENC",
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
    ):
        if os.environ.get(name):
            config[name] = _unquote(os.environ[name])

    at_sources = {
        source.strip().lower()
        for source in config.get("AT_CAPTURE_SOURCES", "realtime,origin").split(",")
        if source.strip()
    }
    if not at_sources or not at_sources <= set(AT_ENDPOINTS):
        raise SystemExit("AT_CAPTURE_SOURCES는 realtime,origin 중 하나 이상이어야 합니다")
    config["AT_CAPTURE_SOURCES"] = at_sources
    required = [
        "DATA_GO_KR_API_KEY_ENC",
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
    ]
    missing = [name for name in required if not config.get(name)]
    if missing:
        raise SystemExit(f"필수 설정이 없습니다: {', '.join(missing)}")
    return config


def supabase_rows(config, path):
    """service role로 수집 대상 사전만 읽는다."""
    base = config["SUPABASE_URL"].rstrip("/") + "/rest/v1"
    key = config["SUPABASE_SERVICE_KEY"]
    request = urllib.request.Request(
        base + path,
        headers={
            "apikey": key,
            "Authorization": "Bearer " + key,
            "User-Agent": "nongsanmul-collector/3.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"수집 대상 DB 조회 실패({error.code}): {detail}") from error
    if not isinstance(result, list):
        raise RuntimeError("수집 대상 DB 응답이 배열이 아닙니다")
    return result


def load_collection_targets(config):
    items = supabase_rows(
        config,
        "/items?select=large_code,mid_code,item_name&collect=eq.true"
        "&order=large_code.asc,mid_code.asc",
    )
    markets = supabase_rows(
        config,
        "/markets?select=whsal_cd,market_name&collect=eq.true&order=display_order.asc",
    )
    if not items:
        raise RuntimeError("items 테이블에 collect=true 품목이 없습니다")
    if not markets:
        raise RuntimeError("markets 테이블에 collect=true 시장이 없습니다")
    return (
        [(row["large_code"], row["mid_code"], row["item_name"]) for row in items],
        [(row["whsal_cd"], row["market_name"]) for row in markets],
    )


def fetch(url, retries=3):
    global API_ATTEMPTS, HTTP_429_COUNT  # noqa: PLW0603 - 실행 단위 계기판
    last_err = None
    for attempt in range(retries):
        API_ATTEMPTS += 1
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "nongsanmul-collector/3.0"}
            )
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            if error.code == 429:
                HTTP_429_COUNT += 1
            last_err = error
            time.sleep(2 * (attempt + 1))
        except Exception as error:  # noqa: BLE001 - 기록 후 재시도
            last_err = error
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"3회 실패: {last_err}")


def build_at_url(encoded_key, source, settlement_date, whsal_cd, large, mid, page):
    """인코딩된 serviceKey 값은 그대로 두고 나머지 쿼리만 인코딩한다."""
    if source not in AT_ENDPOINTS:
        raise ValueError(f"알 수 없는 aT 창구: {source}")
    query = urllib.parse.urlencode(
        [
            ("pageNo", page),
            ("numOfRows", PAGE),
            ("cond[whsl_mrkt_cd::EQ]", whsal_cd),
            ("cond[trd_clcln_ymd::EQ]", settlement_date),
            ("cond[gds_lclsf_cd::EQ]", large),
            ("cond[gds_mclsf_cd::EQ]", mid),
        ]
    )
    return f"{AT_ENDPOINTS[source]}?{AT_KEY_PARAMETER}={encoded_key}&{query}"


def _at_page(data):
    response = data.get("response") or {}
    header = response.get("header") or {}
    if str(header.get("resultCode")) not in {"0", "00"}:
        raise RuntimeError(
            f"aT 응답 오류 {header.get('resultCode')}: {header.get('resultMsg')}"
        )
    body = response.get("body") or {}
    effective_page_size = int(body.get("numOfRows") or 0)
    if effective_page_size > PAGE:
        raise RuntimeError(f"aT 페이지 상한 초과 응답: {effective_page_size}")
    item = (body.get("items") or {}).get("item")
    if item is None:
        rows = []
    elif isinstance(item, list):
        rows = item
    elif isinstance(item, dict):
        rows = [item]
    else:
        raise RuntimeError("aT items.item 형식이 배열/객체가 아닙니다")
    return rows, int(body.get("totalCount") or 0)


def _at_row_key(row):
    fields = (
        "auctn_seq",
        "auctn_seq2",
        "corp_cd",
        "corp_gds_cd",
        "spm_no",
        "scsbd_dt",
        "scsbd_prc",
        "qty",
    )
    return tuple(str(row.get(field) or "") for field in fields)


def capture_at_day_item(
    key, source, settlement_date, whsal_cd, large, mid, deadline=None
):
    """aT 신형 창구 한 묶음을 최대 1000행 페이지로 끝까지 수집한다."""
    rows = []
    seen = set()
    total = None
    page = 1
    while True:
        if _time_budget_exceeded(deadline):
            raise CaptureTimeBudgetExceeded(TIME_BUDGET_ERROR)
        url = build_at_url(
            key, source, settlement_date, whsal_cd, large, mid, page
        )
        got, reported = _at_page(fetch(url))
        keys = {_at_row_key(row) for row in got}
        overlap = seen.intersection(keys)
        if overlap:
            raise RuntimeError(
                f"aT 페이지 경계 중복 감지: {source} {settlement_date} "
                f"{whsal_cd} {large}-{mid} page={page} overlap={len(overlap)}"
            )
        if total is not None and reported < len(rows):
            raise RuntimeError(
                f"aT 수집 중 totalCount 감소: {total} -> {reported}"
            )
        total = reported
        rows.extend(got)
        seen.update(keys)
        if total == 0 or len(rows) >= total or not got:
            break
        page += 1
        if page > (total // PAGE) + 3:
            raise RuntimeError("aT 페이지네이션 종료 조건을 확인하지 못했습니다")
        time.sleep(0.5)
    return rows, int(total or 0)


def classify_capture(total, rows):
    """원 API 보고 건수와 실제 수신 건수로 묶음 상태를 정한다."""
    received = len(rows)
    if received == total:
        return "success"
    if total > 0 and received == 0:
        return "failed"
    return "partial"


def _date_range():
    if len(sys.argv) >= 3:
        start_text, end_text = sys.argv[1], sys.argv[2]
        first = datetime.strptime(start_text, "%Y%m%d").date()
        last = datetime.strptime(end_text, "%Y%m%d").date()
    else:
        today = datetime.now(KST).date()
        first = today - timedelta(days=5)
        last = today + timedelta(days=1)
    if first > last:
        raise SystemExit("시작일은 종료일보다 늦을 수 없습니다")
    dates = []
    current = last
    while current >= first:
        dates.append(current)
        current -= timedelta(days=1)
    return dates


def _raw_path(compact_date, whsal_cd, large, mid, source):
    return os.path.join(
        RAW_DIR, f"{compact_date}_{whsal_cd}_{large}-{mid}_{source}.json"
    )


def _capture_sources(config, day, today_kst):
    sources = []
    if "realtime" in config["AT_CAPTURE_SOURCES"]:
        sources.append("realtime")
    if "origin" in config["AT_CAPTURE_SOURCES"] and day < today_kst:
        sources.append("origin")
    return sources


def _early_stop_summary(reason, attempted_bundles, total_bundles):
    if reason == "time_budget":
        prefix = "시간 예산 초과로 조기 종료"
    else:
        prefix = "연속 실패 차단기 발동"
    return f"{prefix} — 시도 {attempted_bundles}/전체 {total_bundles}묶음"


def _payload(
    *,
    compact_date,
    whsal_cd,
    market_name,
    large,
    mid,
    item_name,
    source,
    total,
    rows,
    status,
    error=None,
):
    return {
        "schema_version": 2,
        "source": source,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sale_date": compact_date,
        "settlement_date": datetime.strptime(compact_date, "%Y%m%d")
        .date()
        .isoformat(),
        "whsal_cd": whsal_cd,
        "market_name": market_name,
        "large": large,
        "mid": mid,
        "item_name": item_name,
        "total_cnt": total,
        "rows_saved": len(rows),
        "capture_status": status,
        "error": error,
        "rows": rows,
    }


def _write_capture_summary(summary):
    with open(CAPTURE_SUMMARY_PATH, "w", encoding="utf-8") as summary_file:
        json.dump(summary, summary_file, ensure_ascii=False, indent=2)


def _append_capture_step_summary(summary):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    rows = summary["rows_by_source"]
    with open(summary_path, "a", encoding="utf-8") as step_summary:
        step_summary.write(
            "\n## 수집 요약\n\n"
            "| 항목 | 값 |\n"
            "|---|---:|\n"
            f"| 예상 묶음 | {summary['expected_bundles']} |\n"
            f"| 시도 | {summary['attempted']} |\n"
            f"| 성공 | {summary['success']} |\n"
            f"| 실패 | {summary['failed']} |\n"
            f"| 미시도 | {summary['unattempted']} |\n"
            f"| 중단 사유 | {summary['stop_reason'] or '-'} |\n"
            f"| API HTTP 시도 | {summary['api_attempts']} |\n"
            f"| HTTP 429 | {summary['http_429_count']} |\n"
            f"| realtime 수신 행 | {rows['realtime']} |\n"
            f"| origin 수신 행 | {rows['origin']} |\n"
        )


def main():
    global API_ATTEMPTS, HTTP_429_COUNT  # noqa: PLW0603 - 실행 단위 초기화
    API_ATTEMPTS = 0
    HTTP_429_COUNT = 0
    if os.path.exists(CAPTURE_SUMMARY_PATH):
        os.remove(CAPTURE_SUMMARY_PATH)
    time_budget_seconds = _nonnegative_env_int("AT_TIME_BUDGET_SECONDS", 0)
    deadline = (
        time.monotonic() + time_budget_seconds if time_budget_seconds else None
    )
    max_consecutive_failures = _nonnegative_env_int(
        "AT_MAX_CONSECUTIVE_FAILURES", 5
    )
    config = load_config()
    items, markets = load_collection_targets(config)
    os.makedirs(RAW_DIR, exist_ok=True)
    dates = _date_range()
    today_kst = datetime.now(KST).date()
    log_path = os.path.join(RAW_DIR, "_capture_log.txt")
    grand_rows = 0
    fails = []
    attempted_bundles = 0
    consecutive_failures = 0
    stop_reason = None
    successful_bundles = 0
    rows_by_source = {"realtime": 0, "origin": 0}
    total_bundles = sum(
        len(_capture_sources(config, day, today_kst)) for day in dates
    ) * len(markets) * len(items)

    with open(log_path, "a", encoding="utf-8") as log:
        log.write(
            f"\n=== 스냅샷 시작 {datetime.now(timezone.utc).isoformat(timespec='seconds')} "
            f"mode=at-v2 ({dates[-1]}~{dates[0]}, "
            f"{len(markets)}시장×{len(items)}품목) ===\n"
        )
        for day in dates:
            compact_date = day.strftime("%Y%m%d")
            settlement_date = day.isoformat()
            day_rows = 0
            for source in _capture_sources(config, day, today_kst):
                api_key = config["DATA_GO_KR_API_KEY_ENC"]
                for whsal_cd, market_name in markets:
                    for large, mid, item_name in items:
                        if _time_budget_exceeded(deadline):
                            stop_reason = "time_budget"
                            break
                        attempted_bundles += 1
                        payload_source = f"at-{source}"
                        output = _raw_path(
                            compact_date,
                            whsal_cd,
                            large,
                            mid,
                            source,
                        )
                        try:
                            rows, total = capture_at_day_item(
                                api_key,
                                source,
                                settlement_date,
                                whsal_cd,
                                large,
                                mid,
                                deadline,
                            )
                            status = classify_capture(total, rows)
                            payload = _payload(
                                compact_date=compact_date,
                                whsal_cd=whsal_cd,
                                market_name=market_name,
                                large=large,
                                mid=mid,
                                item_name=item_name,
                                source=payload_source,
                                total=total,
                                rows=rows,
                                status=status,
                            )
                            with open(output, "w", encoding="utf-8") as raw_file:
                                json.dump(payload, raw_file, ensure_ascii=False)
                            if status != "success":
                                fails.append(
                                    (
                                        compact_date,
                                        source,
                                        market_name,
                                        item_name,
                                        f"API 보고 {total}, 수신 {len(rows)}",
                                    )
                                )
                                consecutive_failures += 1
                            else:
                                successful_bundles += 1
                                consecutive_failures = 0
                            log.write(
                                f"{compact_date} {source} {market_name}({whsal_cd}) "
                                f"{item_name}: {total}건, 수신 {len(rows)} ({status})\n"
                            )
                            day_rows += len(rows)
                            rows_by_source[source] += len(rows)
                        except Exception as error:  # noqa: BLE001
                            time_budget_failed = isinstance(
                                error, CaptureTimeBudgetExceeded
                            )
                            payload = _payload(
                                compact_date=compact_date,
                                whsal_cd=whsal_cd,
                                market_name=market_name,
                                large=large,
                                mid=mid,
                                item_name=item_name,
                                source=payload_source,
                                total=0,
                                rows=[],
                                status="failed",
                                error=(
                                    TIME_BUDGET_ERROR
                                    if time_budget_failed
                                    else "API 호출 실패"
                                ),
                            )
                            with open(output, "w", encoding="utf-8") as raw_file:
                                json.dump(payload, raw_file, ensure_ascii=False)
                            fails.append(
                                (compact_date, source, market_name, item_name, str(error))
                            )
                            log.write(
                                f"{compact_date} {source} {market_name}({whsal_cd}) "
                                f"{item_name}: 실패 — {error}\n"
                            )
                            consecutive_failures += 1
                            if time_budget_failed:
                                stop_reason = "time_budget"
                        if (
                            stop_reason is None
                            and max_consecutive_failures
                            and consecutive_failures >= max_consecutive_failures
                        ):
                            stop_reason = "consecutive_failures"
                        if stop_reason is not None:
                            break
                    if stop_reason is not None:
                        break
                if stop_reason is not None:
                    break
            log.write(f"-- {compact_date} 소계 {day_rows}건\n")
            log.flush()
            grand_rows += day_rows
            if stop_reason is not None:
                break
        if stop_reason is not None:
            log.write(
                f"=== {_early_stop_summary(stop_reason, attempted_bundles, total_bundles)} ===\n"
            )
        log.write(
            f"=== 완료 {datetime.now(timezone.utc).isoformat(timespec='seconds')} "
            f"총 {grand_rows}건, 실패 {len(fails)}묶음 ===\n"
        )
    summary = {
        "expected_bundles": total_bundles,
        "attempted": attempted_bundles,
        "success": successful_bundles,
        "failed": len(fails),
        "unattempted": total_bundles - attempted_bundles,
        "stop_reason": stop_reason,
        "api_attempts": API_ATTEMPTS,
        "http_429_count": HTTP_429_COUNT,
        "rows_by_source": rows_by_source,
    }
    _write_capture_summary(summary)
    _append_capture_step_summary(summary)
    if stop_reason is not None:
        stop_summary = _early_stop_summary(
            stop_reason, attempted_bundles, total_bundles
        )
        print(stop_summary)
        warning_title = (
            "Capture time budget exceeded"
            if stop_reason == "time_budget"
            else "Capture consecutive failure circuit breaker"
        )
        print(f"::warning title={warning_title}::{stop_summary}")
    print(f"완료: 총 {grand_rows}건 저장, 실패 {len(fails)}묶음")
    for failure in fails:
        print("실패:", failure)


if __name__ == "__main__":
    main()
