(function () {
  "use strict";

  const TIME_ONLY = /^(\d{2}):(\d{2}):(\d{2}):(\d{3})$/;
  const DATE_PREFIX = /^\d{4}-\d{2}-\d{2}\s+/;
  const COMPACT_SESSION_TS = /^(\d{2})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2}):(\d{3})$/;
  const POOR_SIGNAL_HR = /^poor\s*signal$/i;
  const FILENAME_YMD_DASH = /(20\d{2})-(\d{2})-(\d{2})/;
  const FILENAME_YMD_COMPACT = /(20\d{2})(\d{2})(\d{2})/;

  function stripCell(s) {
    let t = String(s ?? "").trim();
    if (t.length >= 2 && t[0] === '"' && t[t.length - 1] === '"') {
      t = t.slice(1, -1).replace(/""/g, '"');
    }
    return t.trim();
  }

  function cellBlankOrZero(s) {
    const t = stripCell(s);
    if (t === "") return true;
    const n = parseFloat(String(t).replace(/,/g, ""));
    return !Number.isNaN(n) && n === 0;
  }

  function rowHasMetadata(hr, spo2, temp) {
    const a = stripCell(hr);
    const b = stripCell(spo2);
    const c = stripCell(temp);
    if (cellBlankOrZero(a) && cellBlankOrZero(b) && cellBlankOrZero(c)) return false;
    return true;
  }

  function poorSignalHr(hr) {
    return POOR_SIGNAL_HR.test(stripCell(hr).replace(/_/g, " "));
  }

  function rowIsGapAnchor(hr, spo2, temp) {
    if (poorSignalHr(hr) && cellBlankOrZero(spo2) && cellBlankOrZero(temp)) return false;
    return rowHasMetadata(hr, spo2, temp);
  }

  function classifyRow(hr, spo2, temp) {
    if (poorSignalHr(hr)) return "poor_signal";
    if (rowHasMetadata(hr, spo2, temp)) return "vitals_or_mixed";
    return "ppg_only";
  }

  function kstWallToEpochMs(y, mo, d, h, mi, s, ms) {
    const pad = (n, len) => String(n).padStart(len, "0");
    const iso = `${y}-${pad(mo, 2)}-${pad(d, 2)}T${pad(h, 2)}:${pad(mi, 2)}:${pad(s, 2)}.${pad(ms, 3)}+09:00`;
    return new Date(iso).getTime();
  }

  function kstMidnightEpochMs(y, mo, d) {
    const pad = (n) => String(n).padStart(2, "0");
    const iso = `${y}-${pad(mo)}-${pad(d)}T00:00:00.000+09:00`;
    return new Date(iso).getTime();
  }

  function parseCompactSessionTs(raw) {
    const m = COMPACT_SESSION_TS.exec(stripCell(raw));
    if (!m) return null;
    const yy = +m[1],
      mo = +m[2],
      d = +m[3];
    const h = +m[4],
      mi = +m[5],
      se = +m[6],
      ms = +m[7];
    const year = yy < 100 ? 2000 + yy : yy;
    return { year, mo, d, h, mi, se, ms };
  }

  function timeOnlyToMs(cell) {
    const m = TIME_ONLY.exec(stripCell(cell));
    if (!m) return null;
    const h = +m[1],
      mi = +m[2],
      se = +m[3],
      ms = +m[4];
    return ((h * 60 + mi) * 60 + se) * 1000 + ms;
  }

  function attachEpochMsForSession(sessionStartCompact, timestampCells) {
    const dbg = { session_start: sessionStartCompact, rollovers: 0 };
    const p = parseCompactSessionTs(sessionStartCompact);
    if (!p) return { epochs: timestampCells.map(() => null), dbg: { ...dbg, error: "bad_session_start" } };
    const baseMidnight = kstMidnightEpochMs(p.year, p.mo, p.d);
    const epochs = [];
    let prevTod = null;
    let dayOff = 0;
    for (let i = 0; i < timestampCells.length; i++) {
      const tod = timeOnlyToMs(timestampCells[i]);
      if (tod == null) {
        epochs.push(null);
        continue;
      }
      if (prevTod !== null && tod < prevTod) {
        dayOff++;
        dbg.rollovers++;
      }
      prevTod = tod;
      epochs.push(baseMidnight + dayOff * 86400000 + tod);
    }
    return { epochs, dbg };
  }

  function parseTimestampCell(cell, baseDate) {
    const raw = stripCell(cell);
    if (!raw) return null;
    if (DATE_PREFIX.test(raw)) {
      const sp = raw.split(/\s+/);
      if (sp.length >= 2) {
        const dParts = sp[0].split("-").map(Number);
        let tPart = sp.slice(1).join("").replace(/\s/g, "");
        const m = TIME_ONLY.exec(tPart);
        if (m && dParts.length === 3) {
          const y = dParts[0],
            mo = dParts[1],
            d = dParts[2];
          const h = +m[1],
            mi = +m[2],
            se = +m[3],
            ms = +m[4];
          return kstWallToEpochMs(y, mo, d, h, mi, se, ms);
        }
      }
    }
    const m = TIME_ONLY.exec(raw);
    if (!m) return null;
    const h = +m[1],
      mi = +m[2],
      se = +m[3],
      ms = +m[4];
    const mod = ((h * 60 + mi) * 60 + se) * 1000 + ms;
    if (baseDate) {
      const parts = baseDate.split("-").map(Number);
      if (parts.length === 3 && parts.every((x) => !Number.isNaN(x))) {
        return kstMidnightEpochMs(parts[0], parts[1], parts[2]) + mod;
      }
      return mod;
    }
    return mod;
  }

  function deltaConsecutive(prevMs, nextMs, timeOnly) {
    let d = nextMs - prevMs;
    if (timeOnly && d < 0) d += 86400000;
    return d;
  }

  function detectDelimiter(raw) {
    const t = raw.replace(/^\uFEFF/, "");
    const lineEnd = t.search(/\r?\n/);
    const first = lineEnd === -1 ? t : t.slice(0, lineEnd);
    const tabs = (first.match(/\t/g) || []).length;
    const commas = (first.match(/,/g) || []).length;
    return tabs > commas ? "\t" : ",";
  }

  function parseCSV(text) {
    const delim = detectDelimiter(text);
    const rows = [];
    let row = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      const n = text[i + 1];
      if (inQ) {
        if (c === '"' && n === '"') {
          cur += '"';
          i++;
          continue;
        }
        if (c === '"') {
          inQ = false;
          continue;
        }
        cur += c;
        continue;
      }
      if (c === '"') {
        inQ = true;
        continue;
      }
      if (c === delim) {
        row.push(cur);
        cur = "";
        continue;
      }
      if (c === "\r") continue;
      if (c === "\n") {
        row.push(cur);
        rows.push(row.map(stripCell));
        row = [];
        cur = "";
        continue;
      }
      cur += c;
    }
    if (cur.length || row.length) {
      row.push(cur);
      rows.push(row.map(stripCell));
    }
    return rows;
  }

  function colIndex(headerLower, names) {
    for (let i = 0; i < names.length; i++) {
      const j = headerLower.indexOf(names[i]);
      if (j !== -1) return j;
    }
    return -1;
  }

  function isNewPreamble(hlow) {
    return hlow.indexOf("hub_id") !== -1 && hlow.indexOf("start_time") !== -1 && hlow.indexOf("timestamp") === -1;
  }

  /** hub_mac_address,device_mac_address + 값 1줄 + timestamp 헤더 (Tailing Records 등) */
  function isHubMacPairPreamble(hlow) {
    if (!hlow || !hlow.length || hlow.indexOf("timestamp") !== -1) return false;
    const joined = hlow.join(",");
    return joined.includes("hub_mac") && joined.includes("device_mac");
  }

  function parseSessionsFromRows(rows) {
    const sessions = [];
    let i = 0;
    const n = rows.length;

    while (i < n) {
      const row = rows[i];
      if (!row || !row.some((c) => stripCell(c))) {
        i++;
        continue;
      }
      const hlow = row.map((c) => String(c).trim().toLowerCase());

      if (isNewPreamble(hlow)) {
        const iHub = colIndex(hlow, ["hub_id"]);
        const iDev = colIndex(hlow, ["device_mac"]);
        const iSt = colIndex(hlow, ["start_time"]);
        const iEt = colIndex(hlow, ["end_time"]);
        if (iHub < 0 || iDev < 0 || iSt < 0 || iEt < 0 || i + 2 >= n) {
          i++;
          continue;
        }
        const sessRow = rows[i + 1];
        const hdr2 = rows[i + 2];
        const h2 = hdr2.map((c) => String(c).trim().toLowerCase());
        if (h2.indexOf("timestamp") === -1) {
          i++;
          continue;
        }

        function gv(r, idx) {
          return idx >= 0 && idx < r.length ? stripCell(r[idx]) : "";
        }
        const hub = gv(sessRow, iHub);
        const dev = gv(sessRow, iDev);
        const stRaw = gv(sessRow, iSt);
        const etRaw = gv(sessRow, iEt);

        const data = [];
        let j = i + 3;
        while (j < n) {
          const rj = rows[j];
          if (!rj || !rj.length) {
            j++;
            continue;
          }
          if (stripCell(rj[0]).toLowerCase() === "hub_id" && rj.length <= 6) break;
          data.push({ line: j + 1, row: rj });
          j++;
        }

        sessions.push({
          hub_id: hub,
          device_mac: dev,
          start_time_raw: stRaw,
          end_time_raw: etRaw,
          header_lower: h2,
          data_rows: data,
          preambleLine: i + 1,
        });
        i = j;
        continue;
      }

      if (isHubMacPairPreamble(hlow)) {
        if (i + 2 >= n) {
          i++;
          continue;
        }
        const macRow = rows[i + 1];
        const hdr2 = rows[i + 2];
        const h2 = hdr2.map((c) => String(c).trim().toLowerCase());
        if (h2.indexOf("timestamp") === -1) {
          i++;
          continue;
        }
        const hub = macRow && macRow.length ? stripCell(macRow[0]) : "";
        const dev = macRow && macRow.length > 1 ? stripCell(macRow[1]) : "";
        const data = [];
        let j = i + 3;
        while (j < n) {
          const rj = rows[j];
          if (!rj || !rj.length) {
            j++;
            continue;
          }
          const c0 = stripCell(rj[0]).toLowerCase();
          if ((c0 === "hub_id" || c0 === "hub_mac_address") && rj.length <= 6) break;
          data.push({ line: j + 1, row: rj });
          j++;
        }
        sessions.push({
          hub_id: hub,
          device_mac: dev,
          start_time_raw: "",
          end_time_raw: "",
          header_lower: h2,
          data_rows: data,
          preambleLine: i + 1,
        });
        i = j;
        continue;
      }
      i++;
    }

    if (sessions.length) return sessions;

    for (let idx = 0; idx < rows.length; idx++) {
      const row = rows[idx];
      if (!row || !row.some((c) => stripCell(c))) continue;
      const hlow = row.map((c) => String(c).trim().toLowerCase());
      if (hlow.indexOf("timestamp") !== -1 || hlow.indexOf("time") !== -1) {
        const rest = rows.slice(idx + 1).map((r, k) => ({ line: idx + 2 + k, row: r }));
        return [
          {
            hub_id: "",
            device_mac: "",
            start_time_raw: "",
            end_time_raw: "",
            header_lower: hlow,
            data_rows: rest,
            preambleLine: idx + 1,
          },
        ];
      }
      continue;
    }
    return [];
  }

  function analyzeSession(block, baseDate, legacyTimeOnlyDefault) {
    const gaps = [];
    const meta = {
      hub_id: block.hub_id,
      device_mac: block.device_mac,
      start_time: block.start_time_raw,
      end_time: block.end_time_raw,
      rows: 0,
      metadata_rows_legacy: 0,
      gap_anchor_rows: 0,
      class_ppg_only: 0,
      class_poor_signal: 0,
      class_vitals: 0,
      gaps: 0,
      skipped_parse: 0,
      skipped_stream_break: 0,
      error: null,
      note: null,
      time_debug: {},
    };

    const h = block.header_lower;
    const iTs = colIndex(h, ["timestamp", "time"]);
    const iHr = colIndex(h, ["hr"]);
    const iSpo2 = colIndex(h, ["spo2"]);
    const iTemp = colIndex(h, ["temp"]);
    const iStart = colIndex(h, ["start_time"]);
    const iHub = colIndex(h, ["hub_id"]);
    const iDev = colIndex(h, ["device_mac"]);

    if (iTs < 0) {
      meta.error = "timestamp 열 없음";
      return { gaps, ppgDeltas: [], meta };
    }
    if (iHr < 0 || iSpo2 < 0 || iTemp < 0) {
      meta.error = "필수 열 없음 (hr, spo2, temp)";
      return { gaps, ppgDeltas: [], meta };
    }

    const idxNeed = [iTs, iHr, iSpo2, iTemp];
    if (iHub >= 0) idxNeed.push(iHub);
    if (iDev >= 0) idxNeed.push(iDev);
    let maxIdx = idxNeed[0];
    for (let k = 1; k < idxNeed.length; k++) if (idxNeed[k] > maxIdx) maxIdx = idxNeed[k];

    const tsCells = block.data_rows.map((d) =>
      d.row.length > iTs ? stripCell(d.row[iTs]) : "",
    );

    let prepared = [];
    let timeOnlyMode = legacyTimeOnlyDefault || !baseDate;

    if (block.start_time_raw && parseCompactSessionTs(block.start_time_raw)) {
      const { epochs, dbg } = attachEpochMsForSession(block.start_time_raw, tsCells);
      meta.time_debug = dbg;
      timeOnlyMode = false;
      for (let k = 0; k < block.data_rows.length; k++) {
        const dr = block.data_rows[k];
        meta.rows++;
        const r = dr.row;
        const ep = k < epochs.length ? epochs[k] : null;
        if (r.length > maxIdx) {
          const cls = classifyRow(r[iHr], r[iSpo2], r[iTemp]);
          if (cls === "ppg_only") meta.class_ppg_only++;
          else if (cls === "poor_signal") meta.class_poor_signal++;
          else meta.class_vitals++;
        }
        prepared.push({ line: dr.line, row: r, ep: ep });
      }
    } else {
      const sample = tsCells[0] || "";
      if (DATE_PREFIX.test(sample)) timeOnlyMode = false;
      for (let k = 0; k < block.data_rows.length; k++) {
        const dr = block.data_rows[k];
        meta.rows++;
        const r = dr.row;
        const ep =
          r.length > iTs ? parseTimestampCell(r[iTs], baseDate || null) : null;
        if (r.length > maxIdx) {
          const cls = classifyRow(r[iHr], r[iSpo2], r[iTemp]);
          if (cls === "ppg_only") meta.class_ppg_only++;
          else if (cls === "poor_signal") meta.class_poor_signal++;
          else meta.class_vitals++;
        }
        prepared.push({ line: dr.line, row: r, ep: ep });
      }
    }

    const ppgDeltas = [];
    for (let j = 0; j < prepared.length - 1; j++) {
      const eA = prepared[j].ep;
      const eB = prepared[j + 1].ep;
      if (eA != null && eB != null) ppgDeltas.push(eB - eA);
    }

    for (let j = 0; j < prepared.length - 1; j++) {
      const lnA = prepared[j].line;
      const a = prepared[j].row;
      const b = prepared[j + 1].row;
      const eA = prepared[j].ep;
      const eB = prepared[j + 1].ep;

      if (a.length <= maxIdx || b.length <= maxIdx) continue;

      if (rowHasMetadata(a[iHr], a[iSpo2], a[iTemp])) meta.metadata_rows_legacy++;
      if (!rowIsGapAnchor(a[iHr], a[iSpo2], a[iTemp])) continue;
      meta.gap_anchor_rows++;

      if (iHub >= 0) {
        const ha = a.length > iHub ? stripCell(a[iHub]) : "";
        const hb = b.length > iHub ? stripCell(b[iHub]) : "";
        if (ha && hb && ha !== hb) {
          meta.skipped_stream_break++;
          continue;
        }
      }
      if (iDev >= 0) {
        const da = a.length > iDev ? stripCell(a[iDev]) : "";
        const db = b.length > iDev ? stripCell(b[iDev]) : "";
        if (da && db && da !== db) {
          meta.skipped_stream_break++;
          continue;
        }
      }

      if (eA == null || eB == null) {
        meta.skipped_parse++;
        continue;
      }

      const dTs = deltaConsecutive(eA, eB, timeOnlyMode && !block.start_time_raw);
      let stChanged = false;
      if (iStart >= 0 && a.length > iStart && b.length > iStart) {
        const sa = stripCell(a[iStart]);
        const sb = stripCell(b[iStart]);
        stChanged = !!(sa && sb && sa !== sb);
      }
      gaps.push({ line_meta: lnA, delta_ts_ms: dTs, start_time_changed: stChanged });
      meta.gaps++;
    }

    if (meta.rows && meta.gap_anchor_rows === 0) {
      meta.note =
        "갭 앵커 행 없음. PPG 연속 간격·행 분류(ppg_only / poor_signal / vitals)를 확인하세요.";
    }

    /** 타임스탬프(ep) 해석 가능 + 숫자 HR (Poor Signal 제외) — 그래프용 */
    const hrPoints = [];
    for (let j = 0; j < prepared.length; j++) {
      const p = prepared[j];
      if (p.ep == null) continue;
      const r = p.row;
      if (r.length <= iHr) continue;
      const hrCell = stripCell(r[iHr]);
      if (poorSignalHr(hrCell)) continue;
      const hrN = parseFloat(String(hrCell).replace(/,/g, ""));
      if (!Number.isFinite(hrN) || hrN < 15 || hrN > 350) continue;
      hrPoints.push({ ep: p.ep, hr: hrN, line: p.line });
    }
    hrPoints.sort(function (a, b) {
      return a.ep - b.ep;
    });
    meta.hr_points = hrPoints;

    return { gaps, ppgDeltas, meta };
  }

  function analyzeFileText(text, fileLabel, baseDate) {
    const rows = parseCSV(String(text).replace(/^\uFEFF/, ""));
    const sessions = parseSessionsFromRows(rows);
    if (!sessions.length) {
      return {
        gaps: [],
        ppgDeltas: [],
        meta: {
          path: fileLabel,
          error: "세션/헤더 파싱 실패 (신규 2단 헤더 또는 구형 timestamp 헤더 필요)",
          rows: Math.max(0, rows.length - 1),
        },
        sessions: [],
      };
    }

    const allGaps = [];
    const allPpg = [];
    const sessionMetas = [];
    const effBase = baseDate || "";

    for (let s = 0; s < sessions.length; s++) {
      const block = sessions[s];
      const r = analyzeSession(block, effBase || null, !effBase);
      for (let g = 0; g < r.gaps.length; g++) allGaps.push(r.gaps[g]);
      for (let p = 0; p < r.ppgDeltas.length; p++) allPpg.push(r.ppgDeltas[p]);
      r.meta.session_preamble_line = block.preambleLine;
      sessionMetas.push(r.meta);
    }

    const meta = {
      path: fileLabel,
      sessions: sessions.length,
      rows: sessionMetas.reduce((a, m) => a + (m.rows || 0), 0),
      session_reports: sessionMetas,
    };
    return { gaps: allGaps, ppgDeltas: allPpg, meta, sessions: sessionMetas };
  }

  function summarize(values) {
    if (!values.length) return null;
    const sorted = values.slice().sort(function (a, b) {
      return a - b;
    });
    const n = values.length;
    let sum = 0;
    for (let i = 0; i < n; i++) sum += values[i];
    const mean = sum / n;
    const median = n % 2 === 1 ? sorted[(n - 1) / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    let stdev = 0;
    if (n > 1) {
      let v = 0;
      for (let i = 0; i < n; i++) v += Math.pow(values[i] - mean, 2);
      v /= n - 1;
      stdev = Math.sqrt(v);
    }
    const p95 = sorted[Math.round(0.95 * (n - 1))];
    const p99 = sorted[Math.round(0.99 * (n - 1))];
    return {
      n: n,
      mean_ms: mean,
      median_ms: median,
      stdev_ms: stdev,
      min_ms: sorted[0],
      max_ms: sorted[n - 1],
      p95_ms: p95,
      p99_ms: p99,
    };
  }

  function summarizeClipped(values, maxSec) {
    if (!values.length || !(maxSec > 0)) return null;
    const cap = maxSec * 1000;
    const kept = values.filter(function (x) {
      return x <= cap;
    });
    if (!kept.length) return null;
    const s = summarize(kept);
    s.n_all = values.length;
    s.n_excluded_long = values.length - kept.length;
    s.clip_max_seconds = maxSec;
    return s;
  }

  function formatSummary(s) {
    if (!s) return "—";
    return (
      "n=" +
      s.n +
      " · 평균 " +
      s.mean_ms.toFixed(2) +
      " ms · 중앙 " +
      s.median_ms.toFixed(2) +
      " ms · p95 " +
      s.p95_ms.toFixed(2) +
      " ms · p99 " +
      s.p99_ms.toFixed(2) +
      " ms"
    );
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatKstTimeLabel(epochMs) {
    try {
      return new Intl.DateTimeFormat("ko-KR", {
        timeZone: "Asia/Seoul",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).format(new Date(epochMs));
    } catch {
      return String(epochMs);
    }
  }

  /**
   * @param {HTMLCanvasElement} canvas
   * @param {{ ep: number, hr: number, line: number }[]} points
   */
  function drawHrLineChart(canvas, points) {
    if (!points || points.length < 2) return;
    const wrap = canvas.parentElement;
    const w = Math.max(280, (wrap && wrap.clientWidth) || 640);
    const cssH = 220;
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(cssH * dpr);
    canvas.style.width = w + "px";
    canvas.style.height = cssH + "px";
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);

    const padL = 50;
    const padR = 12;
    const padT = 18;
    const padB = 38;
    const chartW = w - padL - padR;
    const chartH = cssH - padT - padB;

    const hrs = points.map(function (p) {
      return p.hr;
    });
    const eps = points.map(function (p) {
      return p.ep;
    });
    let minEp = eps[0];
    let maxEp = eps[eps.length - 1];
    if (maxEp - minEp < 500) {
      minEp -= 400;
      maxEp += 400;
    }
    let minHr = Math.min.apply(null, hrs);
    let maxHr = Math.max.apply(null, hrs);
    minHr = Math.floor(Math.min(minHr - 4, 45));
    maxHr = Math.ceil(Math.max(maxHr + 4, 125));
    if (maxHr <= minHr) maxHr = minHr + 30;

    ctx.fillStyle = "#0f1419";
    ctx.fillRect(0, 0, w, cssH);

    function xScale(ep) {
      const den = maxEp - minEp || 1;
      return padL + ((ep - minEp) / den) * chartW;
    }
    function yScale(hr) {
      const den = maxHr - minHr || 1;
      return padT + chartH - ((hr - minHr) / den) * chartH;
    }

    ctx.strokeStyle = "#2d3a4f";
    ctx.lineWidth = 1;
    ctx.strokeRect(padL, padT, chartW, chartH);

    const yTicks = 4;
    ctx.font = "11px ui-sans-serif, system-ui, sans-serif";
    for (let t = 0; t <= yTicks; t++) {
      const v = minHr + ((maxHr - minHr) * t) / yTicks;
      const y = padT + chartH - (chartH * t) / yTicks;
      ctx.beginPath();
      ctx.strokeStyle = t === 0 || t === yTicks ? "#3d4f66" : "#243044";
      ctx.moveTo(padL, y);
      ctx.lineTo(padL + chartW, y);
      ctx.stroke();
      ctx.fillStyle = "#8b9cb3";
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ctx.fillText(String(Math.round(v)), padL - 8, y);
    }

    ctx.strokeStyle = "#5b9fd4";
    ctx.lineWidth = 1.75;
    ctx.beginPath();
    for (let i = 0; i < points.length; i++) {
      const x = xScale(points[i].ep);
      const y = yScale(points[i].hr);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();

    if (points.length < 1200) {
      ctx.fillStyle = "rgba(91, 159, 212, 0.45)";
      for (let i = 0; i < points.length; i++) {
        const x = xScale(points[i].ep);
        const y = yScale(points[i].hr);
        ctx.beginPath();
        ctx.arc(x, y, 2.5, 0, Math.PI * 2);
        ctx.fill();
      }
    }

    const xTickCount = Math.min(5, Math.max(2, Math.floor(chartW / 130)));
    ctx.fillStyle = "#8b9cb3";
    ctx.font = "10px ui-sans-serif, system-ui, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (let t = 0; t < xTickCount; t++) {
      const ep = minEp + ((maxEp - minEp) * t) / (xTickCount <= 1 ? 1 : xTickCount - 1);
      const x = xScale(ep);
      ctx.fillText(formatKstTimeLabel(ep), x, padT + chartH + 6);
    }

    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    ctx.fillStyle = "#e7ecf3";
    ctx.font = "600 12px ui-sans-serif, system-ui, sans-serif";
    ctx.fillText("HR (bpm)", 6, 4);
    ctx.font = "10px ui-sans-serif, system-ui, sans-serif";
    ctx.fillStyle = "#6bcb77";
    ctx.fillText("n = " + points.length, 6, 20);
  }

  const drop = document.getElementById("drop");
  const dropHint = document.getElementById("dropHint");
  const fileInput = document.getElementById("file");
  const baseDateEl = document.getElementById("baseDate");
  const clipGapEl = document.getElementById("clipGap");
  const clipPpgEl = document.getElementById("clipPpg");
  const runBtn = document.getElementById("run");
  const out = document.getElementById("out");
  const empty = document.getElementById("empty");
  const overall = document.getElementById("overall");
  const tbody = document.getElementById("tbody");
  const tbodySess = document.getElementById("tbodySess");
  const alerts = document.getElementById("alerts");
  const hrCharts = document.getElementById("hrCharts");
  const hrChartsInner = document.getElementById("hrChartsInner");
  const stChange = document.getElementById("stChange");

  let pending = [];

  function inferDateFromFilename(name) {
    let m = String(name).match(FILENAME_YMD_DASH);
    if (m) return m[1] + "-" + m[2] + "-" + m[3];
    m = String(name).match(FILENAME_YMD_COMPACT);
    if (m) return m[1] + "-" + m[2] + "-" + m[3];
    return null;
  }

  function setPending(files) {
    pending = files;
    runBtn.disabled = !pending.length;
    if (pending.length && !baseDateEl.value) {
      const inf = inferDateFromFilename(pending[0].name);
      if (inf) baseDateEl.value = inf;
    }
    if (pending.length) {
      const names = pending
        .map(function (f) {
          return f.name;
        })
        .join(", ");
      dropHint.innerHTML =
        '<strong style="color:var(--ok)">' +
        pending.length +
        "개 선택됨</strong><br/><span style=\"font-size:0.8rem\">" +
        escapeHtml(names.slice(0, 200)) +
        (names.length > 200 ? "…" : "") +
        "</span>";
    } else {
      dropHint.innerHTML =
        '여러 CSV 선택 가능 · <code>*_vitals.csv</code> 건너뜀 · 상단 <code>hub_id,start_time,…</code> 또는 <code>hub_mac_address,device_mac_address</code>+MAC 1줄';
    }
  }

  drop.addEventListener("click", function () {
    fileInput.click();
  });
  drop.addEventListener("keydown", function (e) {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      fileInput.click();
    }
  });
  drop.addEventListener("dragover", function (e) {
    e.preventDefault();
    drop.classList.add("drag");
  });
  drop.addEventListener("dragleave", function () {
    drop.classList.remove("drag");
  });
  drop.addEventListener("drop", function (e) {
    e.preventDefault();
    drop.classList.remove("drag");
    const list = [];
    for (let i = 0; i < e.dataTransfer.files.length; i++) {
      const f = e.dataTransfer.files[i];
      if (f.name.toLowerCase().endsWith(".csv")) list.push(f);
    }
    if (!list.length) return;
    const jobs = list.map(function (f) {
      return new Promise(function (res) {
        const r = new FileReader();
        r.onload = function () {
          res({ name: f.name, text: String(r.result) });
        };
        r.readAsText(f, "UTF-8");
      });
    });
    Promise.all(jobs).then(setPending);
  });

  fileInput.addEventListener("change", function () {
    const list = Array.prototype.slice.call(fileInput.files);
    if (!list.length) {
      setPending([]);
      return;
    }
    const jobs = list.map(function (f) {
      return new Promise(function (res) {
        const r = new FileReader();
        r.onload = function () {
          res({ name: f.name, text: String(r.result) });
        };
        r.readAsText(f, "UTF-8");
      });
    });
    Promise.all(jobs).then(setPending);
  });

  function renderStatGrid(title, s, colorVar) {
    if (!s)
      return "<p class='err' style='margin:0'>" + (title || "") + " 샘플 없음</p>";
    function fmt(x) {
      return x.toFixed(3);
    }
    const col = colorVar || "var(--muted)";
    return (
      '<h3 style="font-size:0.95rem;margin:1rem 0 0.65rem;color:' +
      col +
      '">' +
      escapeHtml(title) +
      "</h3>" +
      '<div class="stat-grid">' +
      '<div class="stat"><div class="k">샘플 수</div><div class="v">' +
      s.n +
      "</div></div>" +
      '<div class="stat"><div class="k">평균 (ms)</div><div class="v">' +
      fmt(s.mean_ms) +
      "</div></div>" +
      '<div class="stat"><div class="k">중앙 (ms)</div><div class="v">' +
      fmt(s.median_ms) +
      "</div></div>" +
      '<div class="stat"><div class="k">표준편차</div><div class="v">' +
      fmt(s.stdev_ms) +
      "</div></div>" +
      '<div class="stat"><div class="k">최소 / 최대</div><div class="v">' +
      fmt(s.min_ms) +
      " / " +
      fmt(s.max_ms) +
      "</div></div>" +
      '<div class="stat"><div class="k">p95 / p99</div><div class="v">' +
      fmt(s.p95_ms) +
      " / " +
      fmt(s.p99_ms) +
      "</div></div></div>" +
      '<p style="font-size:0.82rem;color:var(--muted);margin:0.75rem 0 0">' +
      formatSummary(s) +
      "</p>"
    );
  }

  runBtn.addEventListener("click", function () {
    empty.style.display = "none";
    out.style.display = "none";
    if (hrCharts) hrCharts.style.display = "none";
    if (hrChartsInner) hrChartsInner.innerHTML = "";
    const manualBase = baseDateEl.value || "";
    const clipParsed = parseFloat(String(clipGapEl.value || "0"), 10);
    const clipSec = !Number.isNaN(clipParsed) && clipParsed > 0 ? clipParsed : 0;
    const useClip = clipSec > 0;
    const clipPpgParsed = parseFloat(String(clipPpgEl && clipPpgEl.value ? clipPpgEl.value : "1"), 10);
    const clipPpgSec = !Number.isNaN(clipPpgParsed) && clipPpgParsed > 0 ? clipPpgParsed : 0;

    const allGapDeltas = [];
    const allPpgDeltas = [];
    let startTimeChanges = 0;
    let streamBreaks = 0;
    const fileReports = [];
    const errLines = [];

    for (let i = 0; i < pending.length; i++) {
      const name = pending[i].name;
      const text = pending[i].text;
      if (name.toLowerCase().indexOf("_vitals") !== -1) continue;
      let effBase = manualBase;
      if (!effBase) effBase = inferDateFromFilename(name) || "";
      const result = analyzeFileText(text, name, effBase);
      const gaps = result.gaps;
      const ppgD = result.ppgDeltas;
      const meta = result.meta;

      if (meta.error) {
        errLines.push(name + ": " + meta.error);
        continue;
      }

      for (let j = 0; j < gaps.length; j++) allGapDeltas.push(gaps[j].delta_ts_ms);
      for (let j = 0; j < ppgD.length; j++) allPpgDeltas.push(ppgD[j]);

      let stc = 0;
      for (let j = 0; j < gaps.length; j++) if (gaps[j].start_time_changed) stc++;
      startTimeChanges += stc;

      const ds = gaps.map(function (g) {
        return g.delta_ts_ms;
      });
      const copy = {
        path: meta.path,
        sessions: meta.sessions,
        rows: meta.rows,
        session_reports: meta.session_reports,
        summary_gap: summarize(ds),
        summary_gap_clipped: useClip ? summarizeClipped(ds, clipSec) : null,
        summary_ppg: summarize(ppgD),
        summary_ppg_clipped: clipPpgSec > 0 ? summarizeClipped(ppgD, clipPpgSec) : null,
        gaps: gaps,
        stc: stc,
      };
      for (let s = 0; s < meta.session_reports.length; s++) {
        streamBreaks += meta.session_reports[s].skipped_stream_break || 0;
      }
      fileReports.push(copy);
    }

    if (errLines.length && !fileReports.length) {
      empty.textContent = errLines.join("\n");
      empty.style.display = "block";
      if (hrCharts) hrCharts.style.display = "none";
      return;
    }

    const sGapAll = summarize(allGapDeltas);
    const sGapClip = useClip ? summarizeClipped(allGapDeltas, clipSec) : null;
    const sPpgAll = summarize(allPpgDeltas);
    const sPpgClip = clipPpgSec > 0 ? summarizeClipped(allPpgDeltas, clipPpgSec) : null;

    let overallHtml = "";
    if (sPpgClip) {
      overallHtml += renderStatGrid(
        "PPG 연속 간격 (≤ " + clipPpgSec + "초, 이상치 제외)",
        sPpgClip,
        "var(--ok)",
      );
    } else if (sPpgAll) {
      overallHtml += renderStatGrid("PPG 연속 간격 (전체)", sPpgAll, "var(--ok)");
    }

    if (sGapClip) {
      overallHtml += renderStatGrid(
        "갭 권장 (메타 앵커→다음 행, ≤ " + clipSec + "초)",
        sGapClip,
        "var(--accent)",
      );
    } else if (useClip && allGapDeltas.length) {
      overallHtml +=
        '<p class="err" style="margin:1rem 0 0">갭: 상한 적용 후 샘플 없음. 최대 간격(초)을 늘리거나 0.</p>';
    }
    overallHtml += renderStatGrid("갭 전체 샘플 (참고)", sGapAll, "var(--muted)");
    overall.innerHTML = overallHtml;

    const noteParts = [];
    if (startTimeChanges)
      noteParts.push("start_time 열 변경(추정 경계): " + startTimeChanges + "회");
    if (streamBreaks)
      noteParts.push("hub/device 불일치로 제외한 메타 행: " + streamBreaks + "건");
    noteParts.push(
      "신규 CSV: 상단 세션(start YYMMDD-HH:mm:ss:SSS) 후 자정 넘김은 자동 일 증가.",
    );
    stChange.innerHTML = noteParts.join("<br>");

    let html = "";
    for (let i = 0; i < fileReports.length; i++) {
      const fr = fileReports[i];
      const sg = fr.summary_gap;
      const sgC = fr.summary_gap_clipped;
      const sp = fr.summary_ppg_clipped || fr.summary_ppg;
      const su = sgC || sg;
      const hasErr = (fr.session_reports || []).some(function (m) {
        return m && m.error;
      });
      if (hasErr) {
        const msg =
          (fr.session_reports &&
            fr.session_reports.find(function (m) {
              return m && m.error;
            })) || {};
        html +=
          "<tr><td>" +
          escapeHtml(fr.path) +
          "</td><td colspan=\"8\" class=\"err\">" +
          escapeHtml(msg.error || "분석 실패") +
          "</td></tr>";
        continue;
      }
      const exLong = sgC ? String(sgC.n_excluded_long) : "—";
      html +=
        "<tr><td>" +
        escapeHtml(fr.path) +
        "</td><td>" +
        (fr.sessions || 1) +
        "</td><td>" +
        fr.rows +
        "</td><td>" +
        (sp ? (sp.median_ms / 1000).toFixed(4) : "—") +
        "</td><td>" +
        (su ? su.n : "—") +
        "</td><td>" +
        (su ? (su.mean_ms / 1000).toFixed(3) : "—") +
        "</td><td>" +
        (su ? (su.median_ms / 1000).toFixed(3) : "—") +
        "</td><td>" +
        exLong +
        "</td><td>" +
        (sg ? sg.n : 0) +
        "</td></tr>";
    }
    tbody.innerHTML = html;

    let sessHtml = "";
    for (let fi = 0; fi < fileReports.length; fi++) {
      const fr = fileReports[fi];
      const reps = fr.session_reports || [];
      for (let si = 0; si < reps.length; si++) {
        const sm = reps[si];
        if (sm.error) {
          sessHtml +=
            "<tr><td>" +
            escapeHtml(fr.path) +
            "</td><td>" +
            sm.session_preamble_line +
            "</td><td colspan=\"6\" class=\"err\">" +
            escapeHtml(sm.error) +
            "</td></tr>";
          continue;
        }
        sessHtml +=
          "<tr><td>" +
          escapeHtml(fr.path) +
          "</td><td>" +
          sm.session_preamble_line +
          "</td><td>" +
          escapeHtml((sm.hub_id || "—").slice(0, 18)) +
          "</td><td>" +
          escapeHtml((sm.device_mac || "—").slice(0, 18)) +
          "</td><td>" +
          sm.class_ppg_only +
          "</td><td>" +
          sm.class_poor_signal +
          "</td><td>" +
          sm.class_vitals +
          "</td><td>" +
          sm.gap_anchor_rows +
          "</td><td title=\"자정 넘김 횟수\">" +
          (sm.time_debug && sm.time_debug.rollovers != null ? sm.time_debug.rollovers : "—") +
          "</td></tr>";
      }
    }
    if (tbodySess) tbodySess.innerHTML = sessHtml;

    let detailBlocks = "";
    for (let i = 0; i < fileReports.length; i++) {
      const fr = fileReports[i];
      if (!fr.gaps || !fr.gaps.length) continue;
      let lines = "";
      const maxShow = Math.min(500, fr.gaps.length);
      for (let j = 0; j < maxShow; j++) {
        const g = fr.gaps[j];
        lines +=
          "줄 " +
          g.line_meta +
          ": Δ " +
          g.delta_ts_ms.toFixed(3) +
          " ms" +
          (g.start_time_changed ? " · start_time 변경" : "") +
          "\n";
      }
      if (fr.gaps.length > 500) lines += "\n… 외 " + (fr.gaps.length - 500) + "건";
      detailBlocks +=
        "<details><summary>" +
        escapeHtml(fr.path) +
        " — 갭 상세 (" +
        fr.gaps.length +
        "건)</summary><pre class=\"gap-list\">" +
        escapeHtml(lines) +
        "</pre></details>";
    }

    let alertHtml = "";
    if (errLines.length) {
      alertHtml +=
        '<p class="err" style="margin-top:1rem">' +
        errLines.map(escapeHtml).join("<br/>") +
        "</p>";
    }
    if (detailBlocks) alertHtml += '<div style="margin-top:1rem">' + detailBlocks + "</div>";
    for (let i = 0; i < fileReports.length; i++) {
      const fr = fileReports[i];
      const reps = fr.session_reports || [];
      for (let j = 0; j < reps.length; j++) {
        const sm = reps[j];
        if (sm.note && sm.rows > 0) {
          alertHtml +=
            '<p class="note">' + escapeHtml(fr.path) + " · 세션 L" + sm.session_preamble_line + ": " + escapeHtml(sm.note) + "</p>";
        }
      }
    }
    alerts.innerHTML = alertHtml;

    if (hrChartsInner && hrCharts) {
      hrChartsInner.innerHTML = "";
      let anyHr = false;
      for (let fi = 0; fi < fileReports.length; fi++) {
        const fr = fileReports[fi];
        const reps = fr.session_reports || [];
        let fileHasChart = false;
        const fileBlock = document.createElement("div");
        fileBlock.className = "hr-file-block";
        const h3 = document.createElement("h3");
        h3.textContent = fr.path;
        fileBlock.appendChild(h3);

        for (let si = 0; si < reps.length; si++) {
          const sm = reps[si];
          if (!sm || sm.error) continue;
          const pts = sm.hr_points;
          if (!pts || pts.length < 2) continue;
          anyHr = true;
          fileHasChart = true;
          const sess = document.createElement("div");
          sess.className = "hr-session";
          const meta = document.createElement("div");
          meta.className = "meta";
          meta.textContent =
            "세션 헤더 줄 L" +
            (sm.session_preamble_line || "?") +
            " · 유효 HR 포인트 " +
            pts.length +
            "개 (Poor Signal·비숫자 제외)";
          sess.appendChild(meta);
          const wrap = document.createElement("div");
          wrap.className = "hr-chart-wrap";
          const cv = document.createElement("canvas");
          wrap.appendChild(cv);
          sess.appendChild(wrap);
          fileBlock.appendChild(sess);
          drawHrLineChart(cv, pts);
        }
        if (fileHasChart) hrChartsInner.appendChild(fileBlock);
      }
      hrCharts.style.display = anyHr ? "block" : "none";
    }

    out.style.display = "block";
  });
})();
