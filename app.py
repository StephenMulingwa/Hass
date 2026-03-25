"""
Hass Petroleum Logistics Monitor
=================================
Architecture
------------
_LIVE_CACHE  (TTL 60 s)   - unit positions/speeds for KPIs + Live tab
_REPORT_CACHE (TTL 300 s) - real geofence report -> outbound/inbound/mileage/TAT

Both caches are refreshed in a background thread so every page load is fast.
The real geofence pipeline mirrors hass_clean.ipynb exactly:
  login -> group units -> exec_report+subrows per unit -> trip pipeline -> computed durations
"""

import io, json, os, re, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from flask import (Flask, redirect, render_template, request,
                   send_file, session, url_for)

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET_KEY", "hass-local-secret-key")

# Vercel (HTTPS reverse proxy): correct URLs in redirects and secure session cookies
if os.getenv("VERCEL"):
    from werkzeug.middleware.proxy_fix import ProxyFix

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

APP_TITLE   = "Hass Petroleum Logistics Monitor"
LOGO_PATH   = Path("img") / "hass_logo.png"
EAT         = timezone(timedelta(hours=3))

WIALON_URL  = os.getenv("WIALON_URL",  "https://hst-api.wialon.com/wialon/ajax.html")
TOKEN       = os.getenv("WIALON_TOKEN","338f417f5f8ae25ba6eb01c878134153FE905CC0A125F5E827810F937A192125F15C8769")
GROUP_ID    = int(os.getenv("WIALON_GROUP_ID",     "29915692"))
RESOURCE_ID = int(os.getenv("WIALON_RESOURCE_ID", "24949343"))
TEMPLATE_ID = int(os.getenv("WIALON_TEMPLATE_ID", "33"))

LIVE_TTL_SEC   = int(os.getenv("LIVE_TTL_SEC",   "60"))
REPORT_TTL_SEC = int(os.getenv("REPORT_TTL_SEC", "300"))
REPORT_DAYS    = int(os.getenv("REPORT_DAYS",     "30"))
# Parallel Wialon sessions (each worker logs in separately; one exec_report at a time per session).
WIALON_REPORT_WORKERS = max(1, min(int(os.getenv("WIALON_REPORT_WORKERS", "4")), 12))
# Optional: block first dashboard load up to N seconds waiting for report cache (default 0 = never block).
REPORT_FIRST_WAIT_SEC = max(0, min(int(os.getenv("REPORT_FIRST_WAIT_SEC", "0")), 120))

ROUTE_STOPS = [
    "Depot Nakuru",
    "Malaba (Border)",
    "Nimule (Border )",
    "Hass Petroleum Juba, Sudan (Destination)",
]
STOP_RANK = {
    "Depot Nakuru":                             0,
    "Malaba (Border)":                          1,
    "Nimule (Border )":                         2,
    "Hass Petroleum Juba, Sudan (Destination)": 3,
}
OUTBOUND_ORDER = list(STOP_RANK.keys())
INBOUND_ORDER  = list(reversed(OUTBOUND_ORDER))
ENDPOINTS      = {0, 3}

# ── CACHES ───────────────────────────────────────────────────────────────────
_lock = threading.Lock()

_LIVE_CACHE   = {"ts": 0.0, "df": None, "eid": None}
_REPORT_CACHE = {"ts": 0.0, "outbound": None, "inbound": None,
                 "mileage": None, "trip_mileage": None, "tat": None,
                 "refreshing": False}
_UPLOAD_REPORT = {"ob": None, "ib": None, "mil": None, "tat": None}

# Raw combined rows from "Run Wialon geofence report" (template table 0, all group units).
LAST_WIALON_GEOFENCE_REPORT: pd.DataFrame | None = None

# ── PURE HELPERS ─────────────────────────────────────────────────────────────

def _df_copy_or_empty(v):
    """Never use `df or pd.DataFrame()` — DataFrame truth value is ambiguous."""
    return v.copy() if isinstance(v, pd.DataFrame) else pd.DataFrame()


def _upload_or_cached(upload_df, cached_df):
    """Prefer uploaded report slice; never `upload_df or cached_df` on DataFrames."""
    return upload_df if upload_df is not None else cached_df


def _eat(val):
    if not val or val in ("----", "-----", None):
        return ""
    s = str(val).strip()
    if re.match(r"^\d+$", s):
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc).astimezone(EAT).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    for fmt in ("%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).astimezone(EAT).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return s

def _apply_eat(df, cols):
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(_eat)
    return df

def _fmt_dur(td):
    if td is None: return ""
    try:
        sec = int(pd.Timedelta(td).total_seconds())
    except Exception:
        return ""
    if sec <= 0: return ""
    d, r  = divmod(sec, 86400)
    h, r2 = divmod(r, 3600)
    m     = round(r2 / 60)
    parts = []
    if d: parts.append(f"{d} Days")
    parts.append(f"{h} Hours")
    parts.append(f"{m} Minutes")
    return " ".join(parts)

def _diff(t_later, t_earlier):
    try:
        a, b = pd.Timestamp(str(t_later)), pd.Timestamp(str(t_earlier))
        if pd.isna(a) or pd.isna(b): return ""
        return _fmt_dur(a - b)
    except Exception:
        return ""

def _sec(s):
    m = re.match(r"(?:(\d+) Days? )?(?:(\d+) Hours? )?(?:(\d+) Minutes?)?", str(s or ""))
    if not m: return 0
    return int(m.group(1) or 0)*86400 + int(m.group(2) or 0)*3600 + int(m.group(3) or 0)*60

def _parse_km(val):
    if not val or val in ("----","-----",None,0): return 0.0
    m = re.search(r"([\d.]+)", str(val))
    return float(m.group(1)) if m else 0.0

# ── WIALON API ────────────────────────────────────────────────────────────────

def _login():
    try:
        r = requests.post(WIALON_URL, params={"svc":"token/login","params":json.dumps({"token":TOKEN})}, timeout=15)
        return r.json().get("eid") or None
    except Exception:
        return None

def _post(eid, svc, params, timeout=30):
    try:
        r = requests.post(WIALON_URL, data={"svc":svc,"params":json.dumps(params),"sid":eid}, timeout=timeout)
        return r.json()
    except Exception:
        return {}

def _group_units(eid):
    grp = _post(eid, "core/search_items", {
        "spec":{"itemsType":"avl_unit_group","propName":"sys_id","propValueMask":str(GROUP_ID),"sortType":"sys_name"},
        "force":1,"flags":1,"from":0,"to":0})
    ids = set((grp.get("items") or [{}])[0].get("u") or []) if grp.get("items") else set()
    if not ids: return []
    units = _post(eid, "core/search_items", {
        "spec":{"itemsType":"avl_unit","propName":"sys_name","propValueMask":"*","sortType":"sys_name"},
        "force":1,"flags":1025,"from":0,"to":0})
    out = []
    for u in (units.get("items") or []):
        uid = u.get("id")
        if uid not in ids: continue
        out.append((int(uid), str(u.get("nm","")).strip() or f"Unit {uid}"))
    return sorted(out, key=lambda x: x[1].lower())


def _split_into_chunks(items: list, n_chunks: int) -> list:
    """Split ``items`` into up to ``n_chunks`` contiguous slices (for parallel workers)."""
    if not items:
        return []
    n = max(1, min(int(n_chunks), len(items)))
    size = (len(items) + n - 1) // n
    return [items[i : i + size] for i in range(0, len(items), size)]


def _wialon_geofence_chunk(chunk: list, from_ts: int, to_ts: int) -> tuple[list, int]:
    """Run geofence template for units in ``chunk`` using one Wialon session."""
    eid = _login()
    if not eid:
        return [], len(chunk)
    frames: list = []
    errs = 0
    for uid, name in chunk:
        try:
            df_sum, _ = _run_report(eid, uid, from_ts, to_ts)
        except Exception:
            errs += 1
            continue
        if df_sum is None or df_sum.empty:
            continue
        d = df_sum.copy()
        d["Vehicle"] = name
        frames.append(d)
    try:
        _post(eid, "report/cleanup_result", {}, timeout=15)
    except Exception:
        pass
    return frames, errs


def _report_pipeline_chunk(chunk: list, from_ts: int, to_ts: int) -> tuple[list, list]:
    """Summary + detail frames for one unit subset (own Wialon session)."""
    eid = _login()
    if not eid:
        return [], []
    summary_frames, detail_frames = [], []
    for uid, name in chunk:
        try:
            df_sum, _ = _run_report(eid, uid, from_ts, to_ts)
        except Exception:
            continue
        if df_sum.empty:
            continue
        df_sum = df_sum.copy()
        df_sum["Vehicle"] = name
        summary_frames.append(df_sum)
        for row_idx in range(len(df_sum)):
            try:
                df_sub = _fetch_subrows(eid, 0, row_idx)
            except Exception:
                continue
            if not df_sub.empty:
                df_sub = df_sub.copy()
                df_sub["Vehicle"] = name
                df_sub["parent_row"] = row_idx
                detail_frames.append(df_sub)
    try:
        _post(eid, "report/cleanup_result", {}, timeout=10)
    except Exception:
        pass
    return summary_frames, detail_frames


# ── LIVE FETCH ────────────────────────────────────────────────────────────────

def _fetch_live():
    eid = _login()
    if not eid: return pd.DataFrame()
    with _lock: _LIVE_CACHE["eid"] = eid

    grp = _post(eid, "core/search_items", {
        "spec":{"itemsType":"avl_unit_group","propName":"sys_id","propValueMask":str(GROUP_ID),"sortType":"sys_name"},
        "force":1,"flags":1,"from":0,"to":0})
    ids = set((grp.get("items") or [{}])[0].get("u") or []) if grp.get("items") else set()
    if not ids: return pd.DataFrame()

    all_units = _post(eid, "core/search_items", {
        "spec":{"itemsType":"avl_unit","propName":"sys_name","propValueMask":"*","sortType":"sys_name"},
        "force":1,"flags":1025,"from":0,"to":0})

    rows = []
    for item in (all_units.get("items") or []):
        uid = item.get("id")
        if uid not in ids: continue
        name = str(item.get("nm","")).strip()
        pos  = item.get("pos") or {}
        lmsg = item.get("lmsg") or {}
        ts   = pos.get("t") or lmsg.get("t")
        last_upd = (datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(EAT).strftime("%Y-%m-%d %H:%M:%S")
                    if ts else "—")
        speed = float(pos.get("s") or (lmsg.get("p") or {}).get("speed") or 0)
        rows.append({"Vehicle":name,"Unit ID":uid,"Last Update":last_upd,
                     "Speed (km/h)":speed,"Status":"🟢 Moving" if speed>0 else "🔴 Stationary"})

    return pd.DataFrame(rows).sort_values("Vehicle").reset_index(drop=True) if rows else pd.DataFrame()

def get_live(force=False):
    with _lock:
        stale = (time.time()-_LIVE_CACHE["ts"]) > LIVE_TTL_SEC or _LIVE_CACHE["df"] is None
    if force or stale:
        df = _fetch_live()
        with _lock:
            if df is not None and not df.empty:
                _LIVE_CACHE.update({"df":df,"ts":time.time()})
    with _lock:
        return _df_copy_or_empty(_LIVE_CACHE["df"])

# ── REPORT PIPELINE ───────────────────────────────────────────────────────────

def _fetch_table(eid, table_index, headers, row_count):
    if row_count == 0: return pd.DataFrame(columns=headers)
    raw = _post(eid,"report/get_result_rows",{"tableIndex":table_index,"indexFrom":0,"indexTo":max(row_count-1,0)},timeout=120)
    if isinstance(raw, dict): return pd.DataFrame(columns=headers)
    rows = [[c.get("t") if isinstance(c,dict) else c for c in r.get("c",[])] for r in raw if isinstance(r,dict)]
    if not rows: return pd.DataFrame(columns=headers)
    mc = min(len(headers),len(rows[0]))
    return pd.DataFrame(rows, columns=headers[:mc])

def _fetch_subrows(eid, table_idx, row_idx):
    res = _post(eid,"report/get_result_subrows",{"tableIndex":table_idx,"rowIndex":row_idx,"indexFrom":0,"indexTo":1000},timeout=60)
    if isinstance(res,dict): return pd.DataFrame()
    rows = [[c.get("t") if isinstance(c,dict) else c for c in r.get("c",[])] for r in res if isinstance(r,dict)]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def _run_report(eid, uid, from_ts, to_ts):
    _post(eid,"report/cleanup_result",{},timeout=15)
    result = _post(eid,"report/exec_report",{
        "reportResourceId":RESOURCE_ID,"reportTemplateId":TEMPLATE_ID,
        "reportObjectId":uid,"reportObjectSecId":0,
        "interval":{"flags":0,"from":from_ts,"to":to_ts}},timeout=180)
    if isinstance(result,dict) and result.get("error"): return pd.DataFrame(),[]
    tables = (result.get("reportResult") or {}).get("tables") or []
    if not tables: return pd.DataFrame(),[]
    meta = tables[0]
    headers = meta.get("header",[])
    nrows = int(meta.get("rows",0) or 0)
    return _fetch_table(eid,0,headers,nrows), tables


def _run_wialon_geofence_raw(from_ts: int, to_ts: int) -> tuple[pd.DataFrame, str]:
    """Execute template report for every unit in GROUP_ID (parallel Wialon sessions)."""
    eid0 = _login()
    if not eid0:
        return pd.DataFrame(), "Wialon login failed."
    units = _group_units(eid0)
    if not units:
        return pd.DataFrame(), "No units found for this Wialon group."

    nw = min(WIALON_REPORT_WORKERS, len(units))
    chunks = _split_into_chunks(units, nw)
    frame_lists: list[pd.DataFrame] = []
    err_count = 0
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futures = [pool.submit(_wialon_geofence_chunk, ch, from_ts, to_ts) for ch in chunks]
        for fut in as_completed(futures):
            try:
                frames, errs = fut.result()
                err_count += errs
                frame_lists.extend(frames)
            except Exception:
                err_count += 1

    if not frame_lists:
        msg = f"No geofence rows in this interval for any of {len(units)} unit(s)."
        if err_count:
            msg += f" ({err_count} unit run(s) failed.)"
        return pd.DataFrame(), msg

    raw = pd.concat(frame_lists, ignore_index=True)
    tcols = [c for c in ("Time in", "Time out") if c in raw.columns]
    if tcols:
        raw = _apply_eat(raw, tcols)
    msg = (
        f"✅ {len(raw):,} row(s) from {len(frame_lists)} vehicle(s) "
        f"(group {GROUP_ID}, template {TEMPLATE_ID}, {len(chunks)} parallel session(s))."
    )
    if err_count:
        msg += f" ({err_count} unit-level error(s).)"
    return raw, msg

# ── TRIP PIPELINE ─────────────────────────────────────────────────────────────

def _split_trips(vdf):
    df = vdf[vdf["Geofence"].isin(STOP_RANK)].sort_values("Time in").reset_index(drop=True)
    trips,cur = [],[]
    for _,row in df.iterrows():
        cur.append(row)
        if STOP_RANK[row["Geofence"]] in ENDPOINTS:
            trips.append(pd.DataFrame(cur).reset_index(drop=True)); cur=[]
    if cur: trips.append(pd.DataFrame(cur).reset_index(drop=True))
    return trips

def _consolidate_start(trips):
    if len(trips)>=2:
        f=trips[0]
        if len(f)==1 and f.iloc[0]["Geofence"] in STOP_RANK and STOP_RANK[f.iloc[0]["Geofence"]] in ENDPOINTS:
            trips[1]=pd.concat([f,trips[1]],ignore_index=True); return trips[1:]
    return trips

def _consolidate_depot(trips):
    if len(trips)<2: return trips
    m=[trips[0]]
    for t in trips[1:]:
        lone = len(t)==1 and t.iloc[0]["Geofence"]=="Depot Nakuru"
        prev = len(m[-1])>0 and m[-1].iloc[-1]["Geofence"]=="Depot Nakuru"
        if lone and prev: m[-1]=pd.concat([m[-1],t],ignore_index=True)
        else: m.append(t)
    return m

def _share_endpoints(trips):
    if len(trips)<2: return trips
    res=[trips[0]]
    for i in range(1,len(trips)):
        prev=res[-1]
        ep=prev[prev["Geofence"].apply(lambda g: STOP_RANK.get(g) in ENDPOINTS)]
        if ep.empty: res.append(trips[i]); continue
        ep_name=ep.iloc[-1]["Geofence"]
        first_occ=prev[prev["Geofence"]==ep_name].iloc[[0]]
        res.append(pd.concat([first_occ,trips[i]],ignore_index=True))
    return res

def _classify(trip_df):
    ranks=[STOP_RANK[g] for g in trip_df["Geofence"] if g in STOP_RANK]
    if not ranks: return "unknown"
    if ranks[-1]==3: return "outbound"
    if ranks[-1]==0: return "inbound"
    return "outbound" if ranks[-1]>=ranks[0] else "inbound"

def _build_row(trip_df, vehicle, route_order):
    row={"Vehicle":vehicle}
    for stop in route_order:
        match=trip_df[trip_df["Geofence"]==stop]
        row[f"{stop} In"]  = match["Time in"].iloc[0]  if not match.empty else ""
        row[f"{stop} Out"] = match["Time out"].iloc[0] if not match.empty else ""

    def g(k): return row.get(k,"") or ""

    if route_order==OUTBOUND_ORDER:
        dep_in=g("Depot Nakuru In");         dep_out=g("Depot Nakuru Out")
        mal_in=g("Malaba (Border) In");       mal_out=g("Malaba (Border) Out")
        nim_in=g("Nimule (Border ) In");      nim_out=g("Nimule (Border ) Out")
        dst_in=g("Hass Petroleum Juba, Sudan (Destination) In")
        dst_out=g("Hass Petroleum Juba, Sudan (Destination) Out")
        t1=_diff(mal_in,dep_out); t2=_diff(nim_in,mal_out); t3=_diff(dst_in,nim_out)
        total=_sec(t1)+_sec(t2)+_sec(t3)
        row["Average Time to Destination"]=_fmt_dur(total) if total else ""
        row["Transit to Malaba"]=t1; row["Transit to Nimule"]=t2; row["Transit to Destination"]=t3
        row["Depot Time Spent"]=_diff(dep_out,dep_in); row["Malaba Time Spent"]=_diff(mal_out,mal_in)
        row["Nimule Time Spent"]=_diff(nim_out,nim_in); row["Juba Time Spent"]=_diff(dst_out,dst_in)
    else:
        dst_in=g("Hass Petroleum Juba, Sudan (Destination) In")
        dst_out=g("Hass Petroleum Juba, Sudan (Destination) Out")
        nim_in=g("Nimule (Border ) In"); nim_out=g("Nimule (Border ) Out")
        mal_in=g("Malaba (Border) In"); mal_out=g("Malaba (Border) Out")
        dep_in=g("Depot Nakuru In");     dep_out=g("Depot Nakuru Out")
        t1=_diff(nim_in,dst_out); t2=_diff(mal_in,nim_out); t3=_diff(dep_in,mal_out)
        total=_sec(t1)+_sec(t2)+_sec(t3)
        row["Average Time to Depot"]=_fmt_dur(total) if total else ""
        row["Transit to Nimule"]=t1; row["Transit to Malaba"]=t2; row["Transit to Depot"]=t3
        row["Juba Time Spent"]=_diff(dst_out,dst_in); row["Nimule Time Spent"]=_diff(nim_out,nim_in)
        row["Malaba Time Spent"]=_diff(mal_out,mal_in); row["Depot Time Spent"]=_diff(dep_out,dep_in)
    return row

def _build_tat(ob,ib):
    if ob.empty or ib.empty: return pd.DataFrame()
    ob2c=[c for c in ["Vehicle","Depot Nakuru Out",
                       "Hass Petroleum Juba, Sudan (Destination) In",
                       "Hass Petroleum Juba, Sudan (Destination) Out"] if c in ob.columns]
    ob2=ob[ob2c].copy(); ob2.columns=["Vehicle","Depot Departure","Juba Arrival","Juba Departure"][:len(ob2c)]
    if "Depot Nakuru In" not in ib.columns: return pd.DataFrame()
    ib2=ib[["Vehicle","Depot Nakuru In"]].copy(); ib2.columns=["Vehicle","Depot Return"]
    tat=ob2.merge(ib2,on="Vehicle",how="outer")
    def td(a,b): return tat.apply(lambda r:_diff(r.get(a,""),r.get(b,"")),axis=1)
    if {"Depot Departure","Juba Arrival"}.issubset(tat.columns):
        tat["Outbound Transit (Depot→Juba)"]=td("Juba Arrival","Depot Departure")
    if {"Juba Arrival","Juba Departure"}.issubset(tat.columns):
        tat["Time at Juba Destination"]=td("Juba Departure","Juba Arrival")
    if {"Juba Departure","Depot Return"}.issubset(tat.columns):
        tat["Inbound Transit (Juba→Depot)"]=td("Depot Return","Juba Departure")
    if {"Depot Departure","Depot Return"}.issubset(tat.columns):
        tat["Full Round-Trip TAT"]=tat.apply(lambda r:_diff(r.get("Depot Return",""),r.get("Depot Departure","")),axis=1)
    return tat.sort_values("Vehicle").reset_index(drop=True)

def _fetch_report_data():
    empty={"outbound":pd.DataFrame(),"inbound":pd.DataFrame(),"mileage":pd.DataFrame(),"trip_mileage":pd.DataFrame(),"tat":pd.DataFrame()}
    eid=_login()
    if not eid: return empty
    units=_group_units(eid)
    if not units: return empty

    now_eat=datetime.now(EAT)
    to_ts=int(now_eat.timestamp())
    from_ts=int((now_eat-timedelta(days=REPORT_DAYS)).timestamp())

    nw = min(WIALON_REPORT_WORKERS, len(units))
    chunks = _split_into_chunks(units, nw)
    summary_frames, detail_frames = [], []
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futs = [pool.submit(_report_pipeline_chunk, ch, from_ts, to_ts) for ch in chunks]
        for fut in as_completed(futs):
            try:
                sf, df = fut.result()
                summary_frames.extend(sf)
                detail_frames.extend(df)
            except Exception:
                pass

    if not summary_frames:
        return empty

    summary_raw=pd.concat(summary_frames,ignore_index=True)
    summary_df=_apply_eat(summary_raw,["Time in","Time out"])

    mil_map={"Initial mileage":"Initial mileage (km)","Final mileage":"Final mileage (km)",
             "Mileage":"Mileage (km)","Mileage (adjusted)":"Adjusted mileage (km)"}
    mil_rows=summary_df[["Vehicle"]].copy()
    for old,new in mil_map.items():
        if old in summary_df.columns: mil_rows[new]=summary_df[old].apply(_parse_km)
    mileage_df=mil_rows.reset_index(drop=True)

    if not detail_frames: return {**empty,"mileage":mileage_df}

    detail_raw=pd.concat(detail_frames,ignore_index=True)
    api_cols=[c for c in summary_df.columns if c!="Vehicle"]
    n,exp=detail_raw.shape[1],len(api_cols)+2
    if n!=exp: return {**empty,"mileage":mileage_df}

    detail_raw.columns=list(api_cols)+["Vehicle","parent_row"]
    detail_df=_apply_eat(detail_raw,["Time in","Time out"])

    ob_rows,ib_rows=[],[]
    for vehicle,group in detail_df.groupby("Vehicle",sort=True):
        trips=_split_trips(group); trips=_consolidate_start(trips)
        trips=_consolidate_depot(trips); trips=_share_endpoints(trips)
        for trip in trips:
            d=_classify(trip)
            if d=="outbound": ob_rows.append(_build_row(trip,vehicle,OUTBOUND_ORDER))
            elif d=="inbound": ib_rows.append(_build_row(trip,vehicle,INBOUND_ORDER))

    ob_df=pd.DataFrame(ob_rows).reset_index(drop=True)
    ib_df=pd.DataFrame(ib_rows).reset_index(drop=True)

    trip_mil_rows=[]
    for vehicle,group in detail_df.groupby("Vehicle",sort=True):
        trips=_split_trips(group); trips=_consolidate_start(trips)
        trips=_consolidate_depot(trips); trips=_share_endpoints(trips)
        for trip in trips:
            d=_classify(trip)
            if d not in ("outbound","inbound"): continue
            valid=trip[trip["Geofence"].isin(STOP_RANK)]
            km=valid["Mileage (adjusted)"].apply(_parse_km).sum() if "Mileage (adjusted)" in valid.columns else 0
            stops=valid["Geofence"].tolist()
            trip_mil_rows.append({"Vehicle":vehicle,"Direction":d,
                "Start stop":stops[0] if stops else "","End stop":stops[-1] if stops else "",
                "Time in":valid["Time in"].min() if not valid.empty else "",
                "Time out":valid["Time out"].max() if not valid.empty else "",
                "Mileage (km)":round(km,2)})

    trip_mil_df=pd.DataFrame(trip_mil_rows).reset_index(drop=True)
    tat_df=_build_tat(ob_df,ib_df)
    return {"outbound":ob_df,"inbound":ib_df,"mileage":mileage_df,"trip_mileage":trip_mil_df,"tat":tat_df}

def _bg_refresh():
    with _lock:
        if _REPORT_CACHE["refreshing"]: return
        _REPORT_CACHE["refreshing"]=True
    try:
        data=_fetch_report_data()
        with _lock: _REPORT_CACHE.update({**data,"ts":time.time(),"refreshing":False})
    except Exception:
        with _lock: _REPORT_CACHE["refreshing"]=False

def get_report(force=False):
    with _lock:
        stale=(time.time()-_REPORT_CACHE["ts"])>REPORT_TTL_SEC
        no_data=_REPORT_CACHE.get("outbound") is None
    if force:
        _bg_refresh()
    elif stale or no_data:
        t = threading.Thread(target=_bg_refresh, daemon=True)
        t.start()
        # Never block the HTTP request for the full fleet report (was up to 60s — felt "frozen").
        if no_data and REPORT_FIRST_WAIT_SEC > 0:
            t.join(timeout=REPORT_FIRST_WAIT_SEC)
    with _lock:
        return {k: _df_copy_or_empty(_REPORT_CACHE.get(k))
                for k in ("outbound","inbound","mileage","trip_mileage","tat")}

# ── UPLOAD PROCESSING ─────────────────────────────────────────────────────────

def _process_upload(df):
    if "Vehicle" not in df.columns: df=df.rename(columns={df.columns[0]:"Vehicle"})
    mil_cols=["Vehicle"]+[c for c in ["Initial mileage (km)","Final mileage (km)","Mileage (km)","Adjusted mileage (km)"] if c in df.columns]
    if "Mileage (km)" not in df.columns and {"Initial mileage (km)","Final mileage (km)"}.issubset(df.columns):
        df["Mileage (km)"]=pd.to_numeric(df["Final mileage (km)"],errors="coerce").fillna(0)-pd.to_numeric(df["Initial mileage (km)"],errors="coerce").fillna(0)
        df["Adjusted mileage (km)"]=(df["Mileage (km)"]*0.97).round(2)
        mil_cols=["Vehicle","Initial mileage (km)","Final mileage (km)","Mileage (km)","Adjusted mileage (km)"]
    mileage=df[[c for c in mil_cols if c in df.columns]].copy()
    cp_cols=[c for c in df.columns if any(s in c for s in ["Depot","Malaba","Nimule","Juba","Destination"])]
    if cp_cols:
        ob=df[["Vehicle"]+cp_cols].copy() if "Vehicle" in df.columns else pd.DataFrame(); ib=pd.DataFrame()
    else:
        ts_col=next((c for c in df.columns if any(k in c.lower() for k in ("snapshot","time","date","update"))),None)
        if ts_col and "Snapshot Time" not in df.columns: df["Snapshot Time"]=df[ts_col]
        if "Mileage (km)" not in df.columns: df["Mileage (km)"]=0
        ob,ib=_synth_ob_ib(df)
    return ob, ib, mileage, _build_tat(ob,ib)

def _synth_ob_ib(df):
    base=df.copy()
    base["_ts"]=pd.to_datetime(base.get("Snapshot Time",pd.Series()),errors="coerce")
    base["_te"]=base["_ts"]+pd.to_timedelta((pd.to_numeric(base.get("Mileage (km)",0),errors="coerce").fillna(0)*2.5).round(),unit="m")
    base["_d_in"]=base["_ts"]-pd.to_timedelta(2,unit="h"); base["_d_out"]=base["_ts"]
    base["_m_in"]=base["_ts"]+pd.to_timedelta(5,unit="h"); base["_m_out"]=base["_m_in"]+pd.to_timedelta(1,unit="h")
    base["_n_in"]=base["_ts"]+pd.to_timedelta(10,unit="h"); base["_n_out"]=base["_n_in"]+pd.to_timedelta(1,unit="h")
    base["_j_in"]=base["_te"]; base["_j_out"]=base["_j_in"]+pd.to_timedelta(8,unit="h")
    ob=pd.DataFrame({"Vehicle":base["Vehicle"],"Depot Nakuru In":base["_d_in"],"Depot Nakuru Out":base["_d_out"],
        "Malaba (Border) In":base["_m_in"],"Malaba (Border) Out":base["_m_out"],
        "Nimule (Border ) In":base["_n_in"],"Nimule (Border ) Out":base["_n_out"],
        "Hass Petroleum Juba, Sudan (Destination) In":base["_j_in"],"Hass Petroleum Juba, Sudan (Destination) Out":base["_j_out"]})
    for col in [c for c in ob.columns if c!="Vehicle"]:
        ob[col]=pd.to_datetime(ob[col],errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    ob["Average Time to Destination"]=(base["_j_in"]-base["_d_out"]).apply(_fmt_dur)
    ob["Transit to Malaba"]=(base["_m_in"]-base["_d_out"]).apply(_fmt_dur)
    ob["Transit to Nimule"]=(base["_n_in"]-base["_m_out"]).apply(_fmt_dur)
    ob["Transit to Destination"]=(base["_j_in"]-base["_n_out"]).apply(_fmt_dur)
    ob["Depot Time Spent"]=(base["_d_out"]-base["_d_in"]).apply(_fmt_dur)
    ob["Malaba Time Spent"]=(base["_m_out"]-base["_m_in"]).apply(_fmt_dur)
    ob["Nimule Time Spent"]=(base["_n_out"]-base["_n_in"]).apply(_fmt_dur)
    ob["Juba Time Spent"]=(base["_j_out"]-base["_j_in"]).apply(_fmt_dur)
    base["_ni"]=base["_j_out"]+pd.to_timedelta(4,unit="h"); base["_no"]=base["_j_out"]+pd.to_timedelta(5,unit="h")
    base["_mi"]=base["_j_out"]+pd.to_timedelta(9,unit="h"); base["_mo"]=base["_j_out"]+pd.to_timedelta(10,unit="h")
    base["_di"]=base["_j_out"]+pd.to_timedelta(16,unit="h"); base["_do"]=base["_j_out"]+pd.to_timedelta(20,unit="h")
    ib=pd.DataFrame({"Vehicle":base["Vehicle"],"Hass Petroleum Juba, Sudan (Destination) In":base["_j_in"],
        "Hass Petroleum Juba, Sudan (Destination) Out":base["_j_out"],
        "Nimule (Border ) In":base["_ni"],"Nimule (Border ) Out":base["_no"],
        "Malaba (Border) In":base["_mi"],"Malaba (Border) Out":base["_mo"],
        "Depot Nakuru In":base["_di"],"Depot Nakuru Out":base["_do"]})
    for col in [c for c in ib.columns if c!="Vehicle"]:
        ib[col]=pd.to_datetime(ib[col],errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    ib["Average Time to Depot"]=(base["_di"]-base["_j_out"]).apply(_fmt_dur)
    ib["Transit to Nimule"]=(base["_ni"]-base["_j_out"]).apply(_fmt_dur)
    ib["Transit to Malaba"]=(base["_mi"]-base["_no"]).apply(_fmt_dur)
    ib["Transit to Depot"]=(base["_di"]-base["_mo"]).apply(_fmt_dur)
    ib["Juba Time Spent"]=(base["_j_out"]-base["_j_in"]).apply(_fmt_dur)
    ib["Nimule Time Spent"]=(base["_no"]-base["_ni"]).apply(_fmt_dur)
    ib["Malaba Time Spent"]=(base["_mo"]-base["_mi"]).apply(_fmt_dur)
    ib["Depot Time Spent"]=(base["_do"]-base["_di"]).apply(_fmt_dur)
    return ob.sort_values("Vehicle"), ib.sort_values("Vehicle")

# ── EXCEL EXPORT ──────────────────────────────────────────────────────────────

def _xlsx(ob,ib,mil,tat):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine="openpyxl") as w:
        for df,name in [(ob,"Outbound"),(ib,"Inbound"),(mil,"Mileage"),(tat,"TAT")]:
            if df is None or df.empty:
                pd.DataFrame({"Note":[f"No data for {name}"]}).to_excel(w,sheet_name=name,index=False)
            else:
                df.to_excel(w,sheet_name=name,index=False)
                ws=w.sheets[name]
                for cc in ws.columns:
                    ml=max((len(str(c.value)) for c in cc if c.value),default=8)
                    ws.column_dimensions[cc[0].column_letter].width=min(ml+4,40)
    buf.seek(0); return buf.read()

# ── HTML TABLE HELPER ─────────────────────────────────────────────────────────

def _html(df,max_rows=200):
    if df is None or df.empty:
        return "<p style='color:var(--text2);padding:16px;'>No data available.</p>"
    out=df.head(max_rows).copy()
    for col in out.select_dtypes(include=["datetimetz","datetime64[ns, UTC]","datetime64[ns]"]).columns:
        try: out[col]=out[col].dt.strftime("%Y-%m-%d %H:%M")
        except Exception: pass
    ts_hints=("In","Out","Update","Arrival","Departure","Return","Time","Start","End")
    for col in out.columns:
        if any(h in col for h in ts_hints):
            out[col]=out[col].apply(lambda v:"—" if str(v).strip() in ("0","","nan","NaT","None") else v)
    return out.to_html(index=False,border=0,classes="")

# ── DATE HELPERS ──────────────────────────────────────────────────────────────

def _now_eat():
    return datetime.now(EAT).strftime("%Y-%m-%d %H:%M:%S EAT")

def _default_range():
    n=datetime.now(EAT)
    return pd.Timestamp(n.replace(hour=0,minute=0,second=0,microsecond=0)), \
           pd.Timestamp(n.replace(hour=23,minute=59,second=59,microsecond=0))

def _parse_local(s):
    if not s: return None
    s=str(s).strip().replace(" ","T")[:19]
    for fmt in ("%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M"):
        try: return pd.Timestamp(datetime.strptime(s,fmt)).tz_localize(EAT)
        except ValueError: continue
    return None

# ── FLASK ROUTES ──────────────────────────────────────────────────────────────

@app.route("/")
def root():
    return redirect(url_for("dashboard") if session.get("auth") else url_for("login"))

@app.route("/login",methods=["GET","POST"])
def login():
    error=None
    if request.method=="POST":
        u=request.form.get("username",""); p=request.form.get("password","")
        if u==os.getenv("HASS_APP_USERNAME","admin") and p==os.getenv("HASS_APP_PASSWORD","Hass@2026"):
            session["auth"]=True; return redirect(url_for("dashboard"))
        error="Invalid username or password."
    return render_template("login.html",error=error,logo_exists=LOGO_PATH.exists())

@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("login"))

@app.route("/logo")
def logo():
    return send_file(LOGO_PATH) if LOGO_PATH.exists() else ("",404)

@app.route("/_stcore/health")
def stcore_health(): return ("ok",200)

@app.route("/_stcore/host-config")
def stcore_host_config(): return {"allowedOrigins":["*"]},200

@app.route("/favicon.ico")
def favicon():
    return send_file(LOGO_PATH,mimetype="image/png") if LOGO_PATH.exists() else ("",204)

@app.route("/refresh",methods=["POST"])
def refresh():
    if not session.get("auth"): return redirect(url_for("login"))
    get_live(force=True); get_report(force=True)
    return redirect(url_for("dashboard"))

@app.route("/download_report",methods=["POST"])
def download_report():
    if not session.get("auth"): return redirect(url_for("login"))
    if _UPLOAD_REPORT["ob"] is not None:
        ob,ib,mil,tat=_UPLOAD_REPORT["ob"],_UPLOAD_REPORT["ib"],_UPLOAD_REPORT["mil"],_UPLOAD_REPORT["tat"]
    else:
        rpt=get_report(); ob,ib,mil,tat=rpt["outbound"],rpt["inbound"],rpt["mileage"],rpt["tat"]
    return send_file(io.BytesIO(_xlsx(ob,ib,mil,tat)),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True, download_name=f"hass_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")

_VALID_MAIN_TABS = frozenset({"live", "report", "outbound", "inbound", "tat", "mileage", "destinations"})
_VALID_REPORT_INNER = frozenset({"rpt-outbound", "rpt-inbound", "rpt-mileage", "rpt-tat", "rpt-wialon"})


@app.route("/dashboard",methods=["GET","POST"])
def dashboard():
    global LAST_WIALON_GEOFENCE_REPORT

    if not session.get("auth"): return redirect(url_for("login"))

    # 1. Fast live data
    live_df=get_live()

    # 2. Live monitor time filter
    lo_def,hi_def=_default_range()
    live_lo=_parse_local(request.args.get("live_from") or request.form.get("live_from")) or lo_def
    live_hi=_parse_local(request.args.get("live_to")   or request.form.get("live_to"))   or hi_def
    if live_lo>live_hi: live_lo,live_hi=live_hi,live_lo

    # Run Wialon geofence (all group units) — slow; then redirect back to Reports → Wialon sub-tab
    if request.method == "POST" and request.form.get("run_wialon_group_report") == "1":
        fu = int(pd.Timestamp(live_lo).tz_convert(timezone.utc).timestamp())
        tu = int(pd.Timestamp(live_hi).tz_convert(timezone.utc).timestamp())
        try:
            df_g, msg = _run_wialon_geofence_raw(fu, tu)
            LAST_WIALON_GEOFENCE_REPORT = df_g
            session["wialon_geofence_msg"] = msg
        except Exception as exc:
            session["wialon_geofence_msg"] = f"⚠️ Wialon report failed: {exc}"
        q_from = live_lo.strftime("%Y-%m-%dT%H:%M")
        q_to = live_hi.strftime("%Y-%m-%dT%H:%M")
        return redirect(url_for(
            "dashboard",
            tab="report",
            inner="rpt-wialon",
            live_from=q_from,
            live_to=q_to,
        ))

    filtered_live=live_df.copy()
    if not live_df.empty and "Last Update" in live_df.columns:
        lu=pd.to_datetime(live_df["Last Update"],errors="coerce")
        try: lu_ts=lu.dt.tz_localize(EAT,ambiguous="infer",nonexistent="shift_forward")
        except Exception: lu_ts=lu
        mask=(lu_ts>=live_lo)&(lu_ts<=live_hi)
        filtered_live=live_df[mask].copy()

    # 3. Real report data
    rpt=get_report()
    outbound_df=rpt["outbound"]; inbound_df=rpt["inbound"]
    mileage_df=rpt["mileage"];   tat_df=rpt["tat"]

    # 4. Upload
    upload_msg=""; report_ob=report_ib=report_mil=report_tat=None
    if request.method=="POST" and "report_file" in request.files:
        f=request.files["report_file"]
        if f and f.filename:
            raw=f.read()
            try:
                uploaded=(pd.read_csv(io.BytesIO(raw)) if f.filename.lower().endswith(".csv") else pd.read_excel(io.BytesIO(raw)))
                report_ob,report_ib,report_mil,report_tat=_process_upload(uploaded)
                _UPLOAD_REPORT.update(ob=report_ob,ib=report_ib,mil=report_mil,tat=report_tat)
                upload_msg=f"✅ Loaded {len(uploaded):,} rows from '{f.filename}'."
            except Exception as exc:
                upload_msg=f"⚠️ Could not read file: {exc}"

    # 5. KPIs
    tracked=len(live_df)
    active=int((live_df["Speed (km/h)"]>0).sum()) if not live_df.empty and "Speed (km/h)" in live_df.columns else 0
    stationary=max(0,tracked-active)
    avg_speed=round(float(live_df["Speed (km/h)"].mean()),1) if not live_df.empty and "Speed (km/h)" in live_df.columns else 0

    # 6. Destinations (last checkpoint per vehicle from outbound)
    dest_df=pd.DataFrame()
    if not outbound_df.empty:
        cp_time_cols=[c for c in outbound_df.columns if c.endswith(" In") or c.endswith(" Out")]
        dest_df=outbound_df[["Vehicle"]].copy()
        def _last_stop(row):
            for col in reversed(cp_time_cols):
                val=row.get(col,"")
                if val and str(val).strip() not in ("","—","0","nan"):
                    return re.sub(r"\s*(In|Out)$","",col).strip()
            return "—"
        dest_df["Last Known Stop"]=outbound_df.apply(_last_stop,axis=1)
        spd_map=dict(zip(live_df.get("Vehicle",[]),live_df.get("Speed (km/h)",[]))) if not live_df.empty else {}
        dest_df["Speed (km/h)"]=dest_df["Vehicle"].map(spd_map).fillna(0)
        dest_df["Status"]=dest_df["Speed (km/h)"].apply(lambda s:"🟢 Moving" if float(s)>0 else "🔴 Stationary")

    # 7. Cache age
    with _lock: rpt_age=int(time.time()-_REPORT_CACHE["ts"])
    cache_label=(f"{rpt_age}s ago" if rpt_age<60 else f"{rpt_age//60}m {rpt_age%60}s ago" if rpt_age<3600 else f"{rpt_age//3600}h ago")

    active_tab = request.args.get("tab", "live")
    if active_tab not in _VALID_MAIN_TABS:
        active_tab = "live"
    active_inner = request.args.get("inner", "") or ""
    if active_inner not in _VALID_REPORT_INNER:
        active_inner = ""
    if active_tab != "report":
        active_inner = ""

    wialon_geofence_msg = session.pop("wialon_geofence_msg", None)
    if wialon_geofence_msg is None:
        wialon_geofence_msg = ""

    _wialon_df = LAST_WIALON_GEOFENCE_REPORT
    wialon_geofence_table = _html(
        _wialon_df if isinstance(_wialon_df, pd.DataFrame) else pd.DataFrame(),
        max_rows=800,
    )

    return render_template("dashboard.html",
        title=APP_TITLE, logo_exists=LOGO_PATH.exists(),
        vehicle_count=tracked, active_count=active, stationary_count=stationary,
        avg_speed=avg_speed, route_stops=len(ROUTE_STOPS),
        last_updated=_now_eat(), cache_label=cache_label,
        wialon_group_id=GROUP_ID, wialon_report_resource_id=RESOURCE_ID, wialon_report_template_id=TEMPLATE_ID,
        live_table=_html(filtered_live),
        outbound_table=_html(outbound_df), inbound_table=_html(inbound_df),
        mileage_table=_html(mileage_df), tat_table=_html(tat_df),
        destinations_table=_html(dest_df),
        destination_options=sorted(dest_df["Last Known Stop"].dropna().unique().tolist()) if not dest_df.empty and "Last Known Stop" in dest_df.columns else [],
        report_table=_html(_upload_or_cached(report_ob, outbound_df)),
        report_ob_table=_html(_upload_or_cached(report_ob, outbound_df)),
        report_ib_table=_html(_upload_or_cached(report_ib, inbound_df)),
        report_mil_table=_html(_upload_or_cached(report_mil, mileage_df)),
        report_tat_table=_html(_upload_or_cached(report_tat, tat_df)),
        wialon_geofence_table=wialon_geofence_table,
        wialon_geofence_msg=wialon_geofence_msg,
        upload_msg=upload_msg,
        live_filter_from=live_lo.strftime("%Y-%m-%dT%H:%M"),
        live_filter_to=live_hi.strftime("%Y-%m-%dT%H:%M"),
        active_tab=active_tab,
        active_inner=active_inner,
    )

# ── STARTUP ───────────────────────────────────────────────────────────────────

def _warmup():
    get_live(force=True)
    threading.Thread(target=lambda: get_report(force=False), daemon=True).start()

if __name__=="__main__":
    _warmup()
    app.run(host="0.0.0.0",port=8501,debug=False,threaded=True)