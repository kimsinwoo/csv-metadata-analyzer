#!/usr/bin/env python3
"""
Tailing / hub_project CSV 간격·샘플링 분석.

지원 형식
---------
1) 신규(2단 헤더): 첫 줄 hub_id,device_mac,start_time,end_time → 둘째 줄 세션 값
   → 셋째 줄 timestamp,ir,red,green,hr,spo2,temp,battery,gyro → 이후 데이터
   세션 start_time 은 YYMMDD-HH:mm:ss:SSS (예: 260410-17:58:14:128, KST).
   데이터 행 timestamp 는 HH:mm:ss:SSS 만 있으며 자정 넘김 시 일 단위 증가로 해석.

2) 구형(단일 헤더): 각 행에 timestamp·hr·spo2·temp (및 선택 hub_id,device_mac).

갭 정의: 배치 끝으로 보는 행(row_is_gap_anchor)의 timestamp → 바로 다음 행 timestamp 차이.
  - Poor Signal 만 있고 spo2/temp 도 비어 있으면 앵커에서 제외(과도한 소간격 샘플 방지).
  - spo2/temp 중 실값이 있으면 Poor Signal 행도 앵커 가능.

PPG 샘플링: 동일 세션에서 연속 행 timestamp 간격(ms) 전체 통계(디버깅용).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

KST = timezone(timedelta(hours=9))

TIME_ONLY = re.compile(r"^(\d{2}):(\d{2}):(\d{2}):(\d{3})$")
# YYMMDD-HH:mm:ss:SSS
COMPACT_SESSION_TS = re.compile(
    r"^(\d{2})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2}):(\d{3})$"
)
FILENAME_YMD_DASH = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
FILENAME_YMD_COMPACT = re.compile(r"(20\d{2})(\d{2})(\d{2})")
POOR_SIGNAL_HR = re.compile(r"^poor\s*signal$", re.I)


def strip_cell(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
        t = t[1:-1].replace('""', '"')
    return t.strip()


def _cell_blank_or_zero(s: str) -> bool:
    t = strip_cell(s)
    if t == "":
        return True
    try:
        return float(t.replace(",", "")) == 0.0
    except ValueError:
        return False


def row_has_metadata(hr: str, spo2: str, temp: str) -> bool:
    a, b, c = strip_cell(hr), strip_cell(spo2), strip_cell(temp)
    if _cell_blank_or_zero(a) and _cell_blank_or_zero(b) and _cell_blank_or_zero(c):
        return False
    return True


def poor_signal_hr(hr: str) -> bool:
    return bool(POOR_SIGNAL_HR.match(strip_cell(hr).replace("_", " ")))


def row_is_gap_anchor(hr: str, spo2: str, temp: str) -> bool:
    """배치 경계(메타→다음 행) 간격 계산에 쓸 행인지."""
    if poor_signal_hr(hr) and _cell_blank_or_zero(spo2) and _cell_blank_or_zero(temp):
        return False
    return row_has_metadata(hr, spo2, temp)


def time_only_to_ms(cell: str) -> Optional[int]:
    m = TIME_ONLY.match(strip_cell(cell))
    if not m:
        return None
    h, mi, se, ms = map(int, m.groups())
    return ((h * 60 + mi) * 60 + se) * 1000 + ms


def parse_compact_session_ts(raw: str) -> Optional[tuple[int, int, int, int, int, int, int]]:
    """YYMMDD-HH:mm:ss:SSS → (year, month, day, h, mi, se, ms). KST 달력."""
    m = COMPACT_SESSION_TS.match(strip_cell(raw))
    if not m:
        return None
    yy, mo, d, h, mi, se, ms = map(int, m.groups())
    year = 2000 + yy if yy < 100 else yy
    return year, mo, d, h, mi, se, ms


def wall_kst_to_epoch_ms(y: int, mo: int, d: int, h: int, mi: int, se: int, ms: int) -> float:
    dt = datetime(y, mo, d, h, mi, se, ms * 1000, tzinfo=KST)
    return dt.timestamp() * 1000


def midnight_kst_epoch_ms(y: int, mo: int, d: int) -> float:
    dt = datetime(y, mo, d, 0, 0, 0, 0, tzinfo=KST)
    return dt.timestamp() * 1000


def attach_epoch_ms_for_session(
    session_start_compact: str, timestamp_cells: list[str]
) -> tuple[list[Optional[float]], dict]:
    """
    세션 시작일의 KST 자정을 기준으로, time-only 타임스탬프를 절대 epoch ms 로 변환.
    time-of-day 가 감소하면(자정 통과) 일 단위 +1.
    """
    dbg: dict = {"session_start": session_start_compact, "rollovers": 0}
    parsed = parse_compact_session_ts(session_start_compact)
    if not parsed:
        return [None] * len(timestamp_cells), {**dbg, "error": "bad_session_start"}
    y0, m0, d0, _, _, _, _ = parsed
    base_midnight = midnight_kst_epoch_ms(y0, m0, d0)

    out: list[Optional[float]] = []
    prev_tod: Optional[int] = None
    day_off = 0
    for cell in timestamp_cells:
        tod = time_only_to_ms(cell)
        if tod is None:
            out.append(None)
            continue
        if prev_tod is not None and tod < prev_tod:
            day_off += 1
            dbg["rollovers"] += 1
        prev_tod = tod
        out.append(base_midnight + day_off * 86400000.0 + tod)
    return out, dbg


def infer_base_date_from_path(path: Path) -> Optional[str]:
    name = path.name
    m = FILENAME_YMD_DASH.search(name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = FILENAME_YMD_COMPACT.search(name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def parse_timestamp_cell(cell: str, base_date: Optional[str]) -> Optional[float]:
    """구형 전체 날짜 또는 time-only + base_date."""
    raw = strip_cell(cell)
    if not raw:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+", raw):
        parts = raw.split(None, 1)
        if len(parts) == 2:
            d_s, t_s = parts[0], parts[1].replace(" ", "")
            m = TIME_ONLY.match(t_s)
            if m:
                h, mi, se, ms = map(int, m.groups())
                try:
                    y, mo, d = map(int, d_s.split("-"))
                except ValueError:
                    return None
                return wall_kst_to_epoch_ms(y, mo, d, h, mi, se, ms)
    m = TIME_ONLY.match(raw)
    if not m:
        return None
    h, mi, se, ms = map(int, m.groups())
    mod = ((h * 60 + mi) * 60 + se) * 1000 + ms
    if base_date:
        try:
            y, mo, d = map(int, base_date.split("-"))
        except ValueError:
            return float(mod)
        return midnight_kst_epoch_ms(y, mo, d) + mod
    return float(mod)


def delta_consecutive(prev_ms: float, next_ms: float, time_only: bool) -> float:
    d = next_ms - prev_ms
    if time_only and d < 0:
        d += 86400000
    return d


@dataclass
class GapRecord:
    line_meta: int
    delta_ts_ms: float
    start_time_changed: bool


@dataclass
class SessionBlock:
    hub_id: str
    device_mac: str
    start_time_raw: str
    end_time_raw: str
    header_lower: list[str]
    data_rows: list[tuple[int, list[str]]]  # lineno, cells
    source_line_preamble: int
    time_debug: dict = field(default_factory=dict)


def detect_delimiter(first_line: str) -> str:
    return "\t" if first_line.count("\t") > first_line.count(",") else ","


def read_csv_rows(path: Path) -> tuple[str, list[tuple[int, list[str]]]]:
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        first = f.readline()
        if not first:
            return ",", []
        delim = detect_delimiter(first)
        f.seek(0)
        reader = csv.reader(f, delimiter=delim)
        rows = []
        for lineno, row in enumerate(reader, start=1):
            rows.append((lineno, row))
    return delim, rows


def is_new_format_preamble(header_lower: list[str]) -> bool:
    return (
        "hub_id" in header_lower
        and "start_time" in header_lower
        and "timestamp" not in header_lower
    )


def col_idx(header_lower: list[str], *names: str) -> Optional[int]:
    for n in names:
        if n in header_lower:
            return header_lower.index(n)
    return None


def parse_sessions_from_rows(
    rows: list[tuple[int, list[str]]],
) -> tuple[list[SessionBlock], list[tuple[int, list[str]]]]:
    """
    신규 형식 세션 블록 목록 + (신규가 없으면) 구형 단일 헤더용 전체 rows.
    """
    sessions: list[SessionBlock] = []
    i = 0
    n = len(rows)

    while i < n:
        ln, row = rows[i]
        if not row or not any(strip_cell(c) for c in row):
            i += 1
            continue
        hlow = [strip_cell(c).lower() for c in row]

        if is_new_format_preamble(hlow):
            i_hub = col_idx(hlow, "hub_id")
            i_dev = col_idx(hlow, "device_mac")
            i_st = col_idx(hlow, "start_time")
            i_et = col_idx(hlow, "end_time")
            if None in (i_hub, i_dev, i_st, i_et) or i + 2 >= n:
                i += 1
                continue
            _, sess_row = rows[i + 1]
            ln_hdr2, hdr2 = rows[i + 2]
            h2 = [strip_cell(c).lower() for c in hdr2]
            if "timestamp" not in h2:
                i += 1
                continue

            def gv(r: list[str], idx: Optional[int]) -> str:
                if idx is None or idx >= len(r):
                    return ""
                return strip_cell(r[idx])

            hub = gv(sess_row, i_hub)
            dev = gv(sess_row, i_dev)
            st_raw = gv(sess_row, i_st)
            et_raw = gv(sess_row, i_et)

            data: list[tuple[int, list[str]]] = []
            j = i + 3
            while j < n:
                lnj, rj = rows[j]
                if not rj:
                    j += 1
                    continue
                if strip_cell(rj[0]).lower() == "hub_id" and len(rj) <= 6:
                    break
                data.append((lnj, rj))
                j += 1

            sessions.append(
                SessionBlock(
                    hub_id=hub,
                    device_mac=dev,
                    start_time_raw=st_raw,
                    end_time_raw=et_raw,
                    header_lower=h2,
                    data_rows=data,
                    source_line_preamble=ln,
                )
            )
            i = j
            continue
        i += 1

    if sessions:
        return sessions, []

    # 구형: 첫 비어 있지 않은 행을 헤더로
    for idx, (ln, row) in enumerate(rows):
        if not row or not any(strip_cell(c) for c in row):
            continue
        hlow = [strip_cell(c).lower() for c in row]
        if "timestamp" in hlow or "time" in hlow:
            rest = rows[idx + 1 :]
            return (
                [
                    SessionBlock(
                        hub_id="",
                        device_mac="",
                        start_time_raw="",
                        end_time_raw="",
                        header_lower=hlow,
                        data_rows=rest,
                        source_line_preamble=ln,
                    )
                ],
                [],
            )
        break

    return [], rows


def classify_row(hr: str, spo2: str, temp: str) -> str:
    if poor_signal_hr(hr):
        return "poor_signal"
    if row_has_metadata(hr, spo2, temp):
        return "vitals_or_mixed"
    return "ppg_only"


def analyze_session(
    block: SessionBlock,
    base_date: Optional[str],
    legacy_time_only_default: bool,
) -> tuple[list[GapRecord], list[float], dict]:
    """갭 레코드, PPG 연속 간격(ms), 메타."""
    meta = {
        "hub_id": block.hub_id,
        "device_mac": block.device_mac,
        "start_time": block.start_time_raw,
        "end_time": block.end_time_raw,
        "rows": 0,
        "metadata_rows_legacy": 0,
        "gap_anchor_rows": 0,
        "class_ppg_only": 0,
        "class_poor_signal": 0,
        "class_vitals": 0,
        "gaps": 0,
        "skipped_no_next": 0,
        "skipped_parse": 0,
        "skipped_stream_break": 0,
        "time_debug": dict(block.time_debug),
    }

    h = block.header_lower
    i_ts = col_idx(h, "timestamp", "time")
    i_hr = col_idx(h, "hr")
    i_spo2 = col_idx(h, "spo2")
    i_temp = col_idx(h, "temp")
    i_start = col_idx(h, "start_time")
    i_hub = col_idx(h, "hub_id")
    i_dev = col_idx(h, "device_mac")

    gaps: list[GapRecord] = []
    ppg_deltas: list[float] = []

    if i_ts is None:
        meta["error"] = "timestamp 열 없음"
        return gaps, ppg_deltas, meta
    if i_hr is None or i_spo2 is None or i_temp is None:
        meta["error"] = "필수 열 없음 (hr, spo2, temp)"
        return gaps, ppg_deltas, meta

    idx_need = [i_ts, i_hr, i_spo2, i_temp]
    if i_hub is not None:
        idx_need.append(i_hub)
    if i_dev is not None:
        idx_need.append(i_dev)
    max_idx = max(idx_need)

    prepared: list[tuple[int, list[str], Optional[float]]] = []
    ts_cells = [strip_cell(r[i_ts]) if len(r) > i_ts else "" for _, r in block.data_rows]

    if block.start_time_raw and parse_compact_session_ts(block.start_time_raw):
        epochs, tdbg = attach_epoch_ms_for_session(block.start_time_raw, ts_cells)
        meta["time_debug"].update(tdbg)
        time_only_mode = False
        for k, (ln, r) in enumerate(block.data_rows):
            meta["rows"] += 1
            ep = epochs[k] if k < len(epochs) else None
            if len(r) > max_idx:
                cls = classify_row(r[i_hr], r[i_spo2], r[i_temp])
                if cls == "ppg_only":
                    meta["class_ppg_only"] += 1
                elif cls == "poor_signal":
                    meta["class_poor_signal"] += 1
                else:
                    meta["class_vitals"] += 1
            prepared.append((ln, r, ep))
    else:
        time_only_mode = legacy_time_only_default or base_date is None
        sample = ts_cells[0] if ts_cells else ""
        if sample and re.match(r"^\d{4}-\d{2}-\d{2}\s+", sample):
            time_only_mode = False
        for ln, r in block.data_rows:
            meta["rows"] += 1
            ep = parse_timestamp_cell(r[i_ts], base_date) if len(r) > i_ts else None
            if len(r) > max_idx:
                cls = classify_row(r[i_hr], r[i_spo2], r[i_temp])
                if cls == "ppg_only":
                    meta["class_ppg_only"] += 1
                elif cls == "poor_signal":
                    meta["class_poor_signal"] += 1
                else:
                    meta["class_vitals"] += 1
            prepared.append((ln, r, ep))

    for j in range(len(prepared) - 1):
        ln_a, a, e_a = prepared[j]
        ln_b, b, e_b = prepared[j + 1]
        if e_a is not None and e_b is not None:
            ppg_deltas.append(e_b - e_a)

    for j in range(len(prepared) - 1):
        ln_a, a, e_a = prepared[j]
        ln_b, b, e_b = prepared[j + 1]
        if len(a) <= max_idx or len(b) <= max_idx:
            continue

        if row_has_metadata(a[i_hr], a[i_spo2], a[i_temp]):
            meta["metadata_rows_legacy"] += 1
        if not row_is_gap_anchor(a[i_hr], a[i_spo2], a[i_temp]):
            continue
        meta["gap_anchor_rows"] += 1

        if i_hub is not None:
            ha = strip_cell(a[i_hub]) if len(a) > i_hub else ""
            hb = strip_cell(b[i_hub]) if len(b) > i_hub else ""
            if ha and hb and ha != hb:
                meta["skipped_stream_break"] += 1
                continue
        if i_dev is not None:
            da = strip_cell(a[i_dev]) if len(a) > i_dev else ""
            db = strip_cell(b[i_dev]) if len(b) > i_dev else ""
            if da and db and da != db:
                meta["skipped_stream_break"] += 1
                continue

        if e_a is None or e_b is None:
            meta["skipped_parse"] += 1
            continue

        d_ts = delta_consecutive(e_a, e_b, time_only_mode and not block.start_time_raw)

        st_changed = False
        if i_start is not None and len(a) > i_start and len(b) > i_start:
            sa = strip_cell(a[i_start])
            sb = strip_cell(b[i_start])
            st_changed = bool(sa and sb and sa != sb)

        gaps.append(GapRecord(line_meta=ln_a, delta_ts_ms=d_ts, start_time_changed=st_changed))
        meta["gaps"] += 1

    if meta["rows"] and meta["gap_anchor_rows"] == 0:
        meta["note"] = (
            "갭 앵커 행 없음(hr/spo2/temp 실측 또는 Poor+실측). "
            "PPG 샘플링 통계는 아래 ppg_* 참고."
        )

    return gaps, ppg_deltas, meta


def summarize(values: list[float]) -> dict:
    if not values:
        return {}
    values_sorted = sorted(values)
    n = len(values)

    def pct(p: float) -> float:
        k = int(round((p / 100.0) * (n - 1)))
        return values_sorted[k]

    return {
        "n": n,
        "mean_ms": statistics.mean(values),
        "median_ms": statistics.median(values),
        "stdev_ms": statistics.stdev(values) if n > 1 else 0.0,
        "min_ms": min(values),
        "max_ms": max(values),
        "p95_ms": pct(95),
        "p99_ms": pct(99),
    }


def summarize_clipped(values: list[float], max_gap_seconds: float) -> Optional[dict]:
    if not values or max_gap_seconds <= 0:
        return None
    cap_ms = max_gap_seconds * 1000.0
    kept = [v for v in values if v <= cap_ms]
    if not kept:
        return None
    out = summarize(kept)
    out["n_all"] = len(values)
    out["n_excluded_long"] = len(values) - len(kept)
    out["clip_max_seconds"] = max_gap_seconds
    return out


def print_summary_lines(s: dict, indent: str = "  ") -> None:
    print(f"{indent}샘플 수: {s['n']}")
    print(f"{indent}평균: {s['mean_ms']:.3f} ms ({s['mean_ms'] / 1000:.6f} 초)")
    print(f"{indent}중앙값: {s['median_ms']:.3f} ms ({s['median_ms'] / 1000:.6f} 초)")
    print(f"{indent}표준편차: {s['stdev_ms']:.3f} ms")
    print(f"{indent}최소 / 최대: {s['min_ms']:.3f} / {s['max_ms']:.3f} ms")
    print(f"{indent}p95: {s['p95_ms']:.3f} ms · p99: {s.get('p99_ms', 0):.3f} ms")


def iter_csv_files(paths: list[Path]) -> Iterator[Path]:
    for p in paths:
        if p.is_file() and p.suffix.lower() == ".csv":
            yield p
        elif p.is_dir():
            yield from sorted(p.rglob("*.csv"))


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="CSV 메타데이터(배치) 간격 + PPG 연속 샘플링 간격 통계"
    )
    ap.add_argument("inputs", nargs="+", type=Path, help="CSV 파일 또는 디렉터리")
    ap.add_argument("--base-date", metavar="YYYY-MM-DD", help="구형 time-only 용 KST 날짜")
    ap.add_argument(
        "--infer-date-from-filename",
        action="store_true",
        help="파일명에서 YYYY-MM-DD 또는 YYYYMMDD 추출",
    )
    ap.add_argument(
        "--clip-gap-seconds",
        type=float,
        default=600.0,
        metavar="SEC",
        help="갭 통계에서 초과 간격 제외 (0이면 미사용). 기본 600",
    )
    ap.add_argument(
        "--clip-ppg-seconds",
        type=float,
        default=1.0,
        metavar="SEC",
        help="PPG 샘플링 요약에서 초과 간격 제외 (이상치·끊김). 기본 1초, 0이면 전체",
    )
    ap.add_argument("--json", action="store_true", help="결과 JSON")
    args = ap.parse_args(argv)

    files = list(iter_csv_files(args.inputs))
    if not files:
        print("CSV 파일이 없습니다.", file=sys.stderr)
        return 1

    base_date_eff: Optional[str] = args.base_date
    if args.infer_date_from_filename and not base_date_eff:
        for fp in files:
            if "_vitals" in fp.name.lower():
                continue
            inferred = infer_base_date_from_path(fp)
            if inferred:
                base_date_eff = inferred
                break
        if base_date_eff and not args.json:
            print(f"[기준 날짜] 파일명에서 추출: {base_date_eff} (KST)", file=sys.stderr)

    all_gap_deltas: list[float] = []
    all_ppg_deltas: list[float] = []
    start_time_changes = 0
    file_reports: list[dict] = []

    for fp in files:
        if "_vitals" in fp.name.lower():
            continue
        _, rows = read_csv_rows(fp)
        sessions, _ = parse_sessions_from_rows(rows)
        if not sessions:
            file_reports.append(
                {
                    "path": str(fp),
                    "error": "파싱할 세션/헤더 없음",
                }
            )
            continue

        f_gaps: list[float] = []
        f_ppg: list[float] = []
        sess_metas: list[dict] = []

        for block in sessions:
            gaps, ppg_d, sm = analyze_session(
                block,
                base_date_eff,
                legacy_time_only_default=base_date_eff is None,
            )
            if sm.get("error"):
                sess_metas.append({**sm, "session_preamble_line": block.source_line_preamble})
                continue
            f_gaps.extend(g.delta_ts_ms for g in gaps)
            f_ppg.extend(ppg_d)
            stc = sum(1 for g in gaps if g.start_time_changed)
            start_time_changes += stc
            sm["start_time_changed_count"] = stc
            sm["gaps_detail_n"] = len(gaps)
            sm["ppg_pairs_n"] = len(ppg_d)
            sm["summary_gap_ms"] = summarize([g.delta_ts_ms for g in gaps])
            clip_p = args.clip_ppg_seconds
            if clip_p and clip_p > 0:
                sm["summary_ppg_ms_clipped"] = summarize_clipped(ppg_d, clip_p)
            sm["summary_ppg_ms"] = summarize(ppg_d) if ppg_d else {}
            sess_metas.append({**sm, "session_preamble_line": block.source_line_preamble})

        all_gap_deltas.extend(f_gaps)
        all_ppg_deltas.extend(f_ppg)

        row = {
            "path": str(fp),
            "sessions": len(sessions),
            "gap_deltas_n": len(f_gaps),
            "ppg_pairs_n": len(f_ppg),
            "session_reports": sess_metas,
            "summary_gap_ms": summarize(f_gaps),
            "summary_gap_ms_clipped": summarize_clipped(f_gaps, args.clip_gap_seconds)
            if args.clip_gap_seconds > 0
            else None,
        }
        if args.clip_ppg_seconds > 0:
            row["summary_ppg_ms_clipped"] = summarize_clipped(
                f_ppg, args.clip_ppg_seconds
            )
        row["summary_ppg_ms"] = summarize(f_ppg) if f_ppg else {}
        file_reports.append(row)

    clip_sec = args.clip_gap_seconds
    s_all_gap = summarize(all_gap_deltas)
    s_clip_gap = summarize_clipped(all_gap_deltas, clip_sec) if clip_sec > 0 else None
    clip_ppg = args.clip_ppg_seconds
    s_ppg_all = summarize(all_ppg_deltas) if all_ppg_deltas else {}
    s_ppg_clip = (
        summarize_clipped(all_ppg_deltas, clip_ppg) if clip_ppg and clip_ppg > 0 else None
    )

    if args.json:
        out = {
            "files": file_reports,
            "overall_gap_delta_ms": s_all_gap,
            "overall_gap_delta_ms_clipped": s_clip_gap,
            "overall_ppg_interval_ms": s_ppg_all,
            "overall_ppg_interval_ms_clipped": s_ppg_clip,
            "base_date_kst": base_date_eff,
            "start_time_column_changed_after_metadata_rows": start_time_changes,
            "clip_gap_seconds": clip_sec,
            "clip_ppg_seconds": clip_ppg,
        }
        print(json.dumps(out, ensure_ascii=False))
        return 0

    print(f"분석 파일 수: {len(files)} (vitals 제외)")
    print(f"갭 샘플 수(전체): {len(all_gap_deltas)} · PPG 연속쌍 수: {len(all_ppg_deltas)}")
    print(
        "\n갭: 배치 앵커 행(실측 vitals, Poor Signal+빈 spo2/temp 제외) → 다음 행 timestamp 차이."
    )
    if s_ppg_clip:
        print(f"\n=== PPG 연속 간격 (≤ {clip_ppg:g}초만, 끊김 제외) ===")
        print_summary_lines(s_ppg_clip)
    elif s_ppg_all:
        print("\n=== PPG 연속 간격 (전체) ===")
        print_summary_lines(s_ppg_all)
    if s_clip_gap:
        print(f"\n=== 갭 권장 요약 (≤ {clip_sec:g}초) ===")
        print_summary_lines(s_clip_gap)
    elif clip_sec > 0 and s_all_gap:
        print(
            f"\n[알림] 갭 --clip-gap-seconds {clip_sec:g} 적용 시 남는 샘플 없음.",
            file=sys.stderr,
        )
    if s_all_gap:
        print("\n=== 갭 전체 샘플 (참고) ===")
        print_summary_lines(s_all_gap)

    if start_time_changes:
        print(f"\n=== start_time 열 변경(추정 경계): {start_time_changes}회 ===")

    for fr in file_reports:
        if fr.get("error"):
            print(f"\n[오류] {fr['path']}: {fr['error']}", file=sys.stderr)
            continue
        for sm in fr.get("session_reports", []):
            if sm.get("error"):
                print(
                    f"\n[세션 오류] {fr['path']} L{sm.get('session_preamble_line')}: {sm['error']}",
                    file=sys.stderr,
                )
            elif sm.get("gap_anchor_rows") == 0 and sm.get("rows", 0) > 0:
                print(f"\n[알림] {fr['path']}: {sm.get('note', '갭 앵커 0')}")

    if not all_gap_deltas and not all_ppg_deltas:
        print("\n갭·PPG 모두 샘플 없음.", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
