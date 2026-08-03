"""
Northwind Systems -- synthetic operational dataset.

Generates a multi-year B2B hardware/software business with genuine internal
dynamics: customer acquisition and churn, a sales funnel with heterogeneous
reps, product lifecycle, marketing campaigns, fulfilment with carriers and
lanes, support quality, multi-entity finance, and FX.

The business has a history. Products launch. Prices rise. A discount policy
loosens. A competitor enters a region. A carrier contract is renegotiated.
A support team is reorganised. Those events leave signatures in the data, so
descriptive, diagnostic and predictive questions all have real answers rather
than noise.

Everything is vectorised with numpy, so scale costs seconds rather than hours.

    python generator/generate.py                    # default, ~7.5M rows
    python generator/generate.py --scale 4          # ~30M rows
    python generator/generate.py --scale 0.05       # quick smoke test
    python generator/generate.py --format parquet   # smaller, faster to load

Output lands in seed/ as gzipped CSV by default.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = ROOT / "seed"

# ===========================================================================
# Business calendar
# ===========================================================================

START = date(2023, 1, 1)
END = date(2026, 7, 24)

HOLIDAYS = {
    date(2023, 1, 2), date(2023, 4, 7), date(2023, 5, 29), date(2023, 6, 19),
    date(2023, 7, 4), date(2023, 9, 4), date(2023, 11, 23), date(2023, 12, 25),
    date(2024, 1, 1), date(2024, 3, 29), date(2024, 5, 27), date(2024, 6, 19),
    date(2024, 7, 4), date(2024, 9, 2), date(2024, 11, 28), date(2024, 12, 25),
    date(2025, 1, 1), date(2025, 4, 18), date(2025, 5, 26), date(2025, 6, 19),
    date(2025, 7, 4), date(2025, 9, 1), date(2025, 11, 27), date(2025, 12, 25),
    date(2026, 1, 1), date(2026, 4, 3), date(2026, 5, 25), date(2026, 6, 19),
    date(2026, 7, 3),
}

# ===========================================================================
# Dated business events -- these are what make diagnostics answerable
# ===========================================================================

EV_AURORA_LAUNCH      = date(2023, 6, 1)    # new software line; revenue mix shifts
EV_PRICE_RISE         = date(2024, 2, 1)    # +7% list on Hardware
EV_CRM_MIGRATION      = date(2024, 7, 1)    # account_number keyed by integration after this
EV_SUPPLY_SQUEEZE_A   = date(2024, 7, 15)   # component shortage begins
EV_SUPPLY_SQUEEZE_B   = date(2024, 10, 31)  # ... and ends
EV_DISCOUNT_POLICY    = date(2025, 1, 6)    # approval ceiling raised 20% -> 32%
EV_CARRIER_SWITCH     = date(2025, 9, 1)    # volume moved to a cheaper carrier
EV_EMEA_COMPETITOR    = date(2025, 11, 3)   # new entrant undercuts EMEA pricing
EV_APAC_SUPPORT_REORG = date(2026, 3, 2)    # APAC support consolidated; queues lengthen
EV_JPY_REGRESSION_A   = date(2026, 6, 15)   # OMS release mishandles ISO 4217 minor units
EV_JPY_REGRESSION_B   = date(2026, 6, 23)   # hotfix
EV_APAC_OUTAGE        = date(2026, 6, 17)   # regional order entry unavailable for a day
EV_BULK_REPRICE       = datetime(2026, 6, 11, 2, 0, 0)

# ===========================================================================
# Reference data
# ===========================================================================

COMPANIES = [
    ("US01", "Northwind Systems Inc.",       "USD", "United States"),
    ("CA01", "Northwind Systems Canada ULC", "CAD", "Canada"),
    ("GB01", "Northwind Systems UK Ltd.",    "GBP", "United Kingdom"),
    ("DE01", "Northwind Systems GmbH",       "EUR", "Germany"),
    ("JP01", "Northwind Systems KK",         "JPY", "Japan"),
]
COMPANY_CCY = {c: x for c, _, x, _ in COMPANIES}

REGIONS = ["NA-WEST", "NA-EAST", "CANADA", "EMEA", "UKI", "APAC"]
REGION_WEIGHT = np.array([0.21, 0.25, 0.09, 0.19, 0.09, 0.17])
REGION_COMPANY = {"NA-WEST": "US01", "NA-EAST": "US01", "CANADA": "CA01",
                  "EMEA": "DE01", "UKI": "GB01", "APAC": "JP01"}
REGION_CCY = {"NA-WEST": "USD", "NA-EAST": "USD", "CANADA": "CAD",
              "EMEA": "EUR", "UKI": "GBP", "APAC": "JPY"}

SEGMENTS = ["Enterprise", "Mid-Market", "SMB", "Public Sector"]
SEGMENT_WEIGHT = np.array([0.12, 0.28, 0.51, 0.09])
SEGMENT_ORDER_RATE = {"Enterprise": 34.0, "Mid-Market": 17.0, "SMB": 7.5, "Public Sector": 9.0}
SEGMENT_BASKET = {"Enterprise": 4.4, "Mid-Market": 3.0, "SMB": 1.9, "Public Sector": 3.4}
SEGMENT_CHURN_YR = {"Enterprise": 0.055, "Mid-Market": 0.11, "SMB": 0.26, "Public Sector": 0.07}
SEGMENT_CYCLE_D = {"Enterprise": 118, "Mid-Market": 62, "SMB": 26, "Public Sector": 143}

INDUSTRIES = ["Manufacturing", "Retail", "Technology", "Healthcare", "Logistics",
              "Financial Services", "Energy", "Education", "Public Administration"]

CATEGORIES = ["Hardware", "Software License", "Support Plan",
              "Professional Services", "Consumables"]
CATEGORY_MARGIN = {"Hardware": 0.28, "Software License": 0.86,
                   "Support Plan": 0.71, "Professional Services": 0.42,
                   "Consumables": 0.36}

LEAD_SOURCES = ["Inbound Web", "Partner Referral", "Trade Show", "Outbound SDR",
                "Existing Customer", "Marketing Campaign"]
LOSS_REASONS = ["Price", "Competitor", "No Decision", "Timing", "Feature Gap", "Budget Cut"]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "JPY"]
MINOR_UNITS = {"USD": 2, "EUR": 2, "GBP": 2, "CAD": 2, "JPY": 0}
FX_BASE = {"USD": 1.0, "EUR": 1.0850, "GBP": 1.2680, "CAD": 0.7340, "JPY": 0.006420}

WAREHOUSES = [
    ("WH-SEA", "Kent DC",        "NA-WEST", 47.3809, -122.2348),
    ("WH-DFW", "Grand Prairie DC","NA-WEST", 32.7459, -96.9978),
    ("WH-CMH", "Groveport DC",   "NA-EAST", 39.8814, -82.8846),
    ("WH-EWR", "Edison DC",      "NA-EAST", 40.5187, -74.4121),
    ("WH-YYZ", "Mississauga DC", "CANADA",  43.6777, -79.6248),
    ("WH-FRA", "Raunheim DC",    "EMEA",    50.0119, 8.4489),
    ("WH-LHR", "Slough DC",      "UKI",     51.5105, -0.5950),
    ("WH-NRT", "Narita DC",      "APAC",    35.7720, 140.3929),
]
CARRIERS = [
    ("PREMIER", "Premier Freight Systems", "GROUND", 1.00, 0.951),
    ("VALUEX",  "ValueXpress Logistics",   "GROUND", 0.74, 0.868),
    ("AIRLINK", "AirLink Cargo",           "AIR",    2.35, 0.972),
    ("REGIONAL","Regional Parcel Co-op",   "GROUND", 0.88, 0.912),
    ("OCEANIC", "Oceanic Container Line",  "OCEAN",  0.31, 0.804),
]
SERVICE_LEVELS = [("STANDARD", 5, 1.00), ("EXPEDITED", 2, 1.85), ("ECONOMY", 9, 0.68)]

CASE_TYPES = ["Hardware Fault", "Software Defect", "How-To", "Billing Dispute",
              "Shipping Issue", "Feature Request", "Onboarding"]
CASE_PRIORITIES = ["P1", "P2", "P3", "P4"]

CAMPAIGN_TYPES = ["Email Nurture", "Paid Search", "Field Event", "Webinar",
                  "Content Syndication", "Partner Co-Marketing"]

WORDS_A = ["Aurora", "Vertex", "Halcyon", "Meridian", "Quarry", "Lumen", "Cinder",
           "Pallas", "Onyx", "Tessera", "Corvid", "Beacon", "Slate", "Ardent",
           "Kestrel", "Foundry", "Alloy", "Radian", "Umbra", "Talon"]
WORDS_B = ["Core", "Pro", "Edge", "Max", "Lite", "Node", "Rack", "Grid", "Sync",
           "Vault", "Relay", "Forge", "Prism", "Anchor", "Link"]
CO_A = ["Blue", "Iron", "Summit", "Cedar", "Harbor", "Vantage", "Granite", "Willow",
        "Crescent", "Pioneer", "Northgate", "Silverline", "Redstone", "Copper",
        "Lakeshore", "Brightwater", "Ridgeway", "Foxglove", "Cardinal", "Meridian",
        "Sandpiper", "Thornbury", "Aspen", "Kingsley", "Marbleton"]
CO_B = ["Industries", "Logistics", "Group", "Holdings", "Partners", "Systems",
        "Works", "Labs", "Manufacturing", "Distribution", "Technologies",
        "Solutions", "Health", "Energy", "Retail Group", "Foods", "Aerospace"]
CO_C = ["Inc.", "LLC", "Ltd.", "GmbH", "K.K.", "PLC", "Corp.", "S.A.", "ULC", "Co."]

FIRST = ["Amara", "Jonas", "Priya", "Diego", "Mei", "Tomas", "Nadia", "Rowan",
         "Ines", "Kofi", "Yuki", "Liam", "Farah", "Oscar", "Anaya", "Petra",
         "Emeka", "Sanne", "Rafael", "Leila", "Bjorn", "Chiara", "Hassan", "Noor",
         "Devon", "Marisol", "Aditya", "Greta", "Malik", "Sofia"]
LAST = ["Okonkwo", "Lindqvist", "Raman", "Herrera", "Zhao", "Novak", "Haddad",
        "Whitfield", "Costa", "Mensah", "Tanaka", "Byrne", "Nasser", "Lindgren",
        "Iyer", "Kowalski", "Adeyemi", "Visser", "Moreau", "Rahimi", "Aaltonen",
        "Ferrari", "Baptiste", "Sorensen", "Quinn", "Delgado", "Bhatt", "Muller"]


# ===========================================================================
# Helpers
# ===========================================================================

def map_by(keys: np.ndarray, mapping: dict) -> np.ndarray:
    """Vectorised dict lookup that does not assume the key array is sorted."""
    ks = np.array(sorted(mapping))
    vs = np.array([mapping[k] for k in ks])
    return vs[np.searchsorted(ks, keys)]


def banking_mask(dates: np.ndarray) -> np.ndarray:
    """True where a date is a banking day (weekday and not a market holiday)."""
    dow = pd.DatetimeIndex(dates).dayofweek.to_numpy()
    hol = np.isin(dates, np.array(sorted(HOLIDAYS), dtype="datetime64[D]"))
    return (dow < 5) & ~hol


def rounded(vals: np.ndarray, ccy_codes: np.ndarray) -> np.ndarray:
    """Round to each currency's ISO 4217 minor unit."""
    out = np.empty_like(vals, dtype=np.float64)
    for ccy, places in MINOR_UNITS.items():
        m = ccy_codes == ccy
        if m.any():
            out[m] = np.round(vals[m], places)
    return out


def fmt_money(vals: np.ndarray, ccy_codes: np.ndarray) -> np.ndarray:
    """Round to each currency's minor unit and keep it numeric.

    A JPY amount comes out as a whole number because JPY has no minor unit --
    the ISO 4217 semantics survive without paying for object-dtype columns.
    """
    return rounded(np.asarray(vals, dtype=np.float64), ccy_codes)


def d2s(a: np.ndarray) -> np.ndarray:
    return np.datetime_as_string(a.astype("datetime64[D]"), unit="D")


def ts2s(a: np.ndarray) -> np.ndarray:
    return np.char.replace(np.datetime_as_string(a.astype("datetime64[s]"), unit="s"), "T", " ")


class Timer:
    def __init__(self):
        self.t0 = time.time()
        self.last = self.t0

    def lap(self, label: str, n: int | None = None) -> None:
        now = time.time()
        extra = f"{n:>11,} rows" if n is not None else " " * 16
        print(f"  {label:<26} {extra}   {now - self.last:6.1f}s")
        self.last = now

    def total(self) -> float:
        return time.time() - self.t0


# ===========================================================================
# Generation
# ===========================================================================

def generate(scale: float, fmt: str, out_dir: Path, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    T = Timer()
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, int] = {}

    def emit(name: str, df: pd.DataFrame) -> None:
        if fmt == "parquet":
            df.to_parquet(out_dir / f"{name}.parquet", index=False, compression="zstd")
        elif fmt == "csv":
            df.to_csv(out_dir / f"{name}.csv", index=False)
        else:
            with gzip.open(out_dir / f"{name}.csv.gz", "wt", newline="", encoding="utf-8") as fh:
                df.to_csv(fh, index=False)
        written[name] = len(df)

    # -- calendar ----------------------------------------------------------
    all_days = np.arange(np.datetime64(START, "D"),
                         np.datetime64(END, "D") + np.timedelta64(1, "D"),
                         dtype="datetime64[D]")
    n_days = len(all_days)
    day_idx = np.arange(n_days)
    dow = pd.DatetimeIndex(all_days).dayofweek.to_numpy()
    month = pd.DatetimeIndex(all_days).month.to_numpy()
    dom = pd.DatetimeIndex(all_days).day.to_numpy()
    is_bank = banking_mask(all_days)
    bank_days = all_days[is_bank]

    # -- seasonality -------------------------------------------------------
    # compounding growth, weekday shape, quarter-end push, December surge,
    # northern-hemisphere summer softness
    growth = 1.18 ** (day_idx / 365.25)
    weekday = np.select(
        [dow <= 3, dow == 4, dow == 5, dow == 6],
        [1.00, 0.83, 0.16, 0.11], default=1.0)
    quarter_end = np.where((month % 3 == 0) & (dom >= 16), 1.55, 1.0)
    december = np.where((month == 12) & (dom <= 22), 1.22, 1.0)
    january = np.where((month == 1) & (dom <= 12), 0.72, 1.0)
    summer = np.where(np.isin(month, [7, 8]), 0.87, 1.0)
    holiday_dip = np.where(np.isin(all_days, np.array(sorted(HOLIDAYS), dtype="datetime64[D]")), 0.22, 1.0)
    noise = rng.lognormal(0, 0.085, n_days)
    day_weight = growth * weekday * quarter_end * december * january * summer * holiday_dip * noise
    day_cdf = np.concatenate([[0.0], np.cumsum(day_weight)])
    day_cdf /= day_cdf[-1]

    # =====================================================================
    # Dimensions
    # =====================================================================
    companies = pd.DataFrame(COMPANIES, columns=[
        "company_code", "company_name", "functional_currency", "country"])
    emit("erp_companies", companies)

    warehouses = pd.DataFrame(WAREHOUSES, columns=[
        "warehouse_code", "warehouse_name", "region", "latitude", "longitude"])
    emit("ops_warehouses", warehouses)

    carriers = pd.DataFrame(CARRIERS, columns=[
        "carrier_code", "carrier_name", "mode", "cost_index", "published_otd_rate"])
    emit("ops_carriers", carriers)

    # -- cost centres (some open mid-history, one retires) -----------------
    cc_rows = []
    ccn = 0
    for region, company in REGION_COMPANY.items():
        for fn in ["Sales", "Services", "Support", "Renewals"]:
            ccn += 1
            cc_rows.append((f"CC-{company[:2]}{ccn:03d}", f"{region} {fn}", company, region, fn,
                            f"cc{ccn:03d}@northwind-systems.example",
                            str(START - timedelta(days=int(rng.integers(400, 2200)))), None, True))
    for fn, vf in [("Digital Sales", date(2024, 4, 1)), ("Partner Services", date(2025, 2, 3)),
                   ("Customer Success", date(2025, 10, 1))]:
        ccn += 1
        cc_rows.append((f"CC-DE{ccn:03d}", f"EMEA {fn}", "DE01", "EMEA", fn,
                        f"cc{ccn:03d}@northwind-systems.example", str(vf), None, True))
    cost_centers = pd.DataFrame(cc_rows, columns=[
        "cost_center_code", "cost_center_name", "company_code", "region", "function",
        "owner_email", "valid_from", "valid_to", "is_active"])
    cost_centers.loc[1, ["valid_to", "is_active"]] = ["2024-09-30", False]
    cost_centers.loc[9, ["valid_to", "is_active"]] = ["2025-06-30", False]
    emit("erp_cost_centers", cost_centers)

    gl_accounts = pd.DataFrame([
        ("4000", "Product Revenue", "REVENUE", True),
        ("4010", "License Revenue", "REVENUE", True),
        ("4020", "Services Revenue", "REVENUE", True),
        ("4030", "Support Revenue", "REVENUE", True),
        ("4090", "Revenue Adjustments", "REVENUE", True),
        ("1200", "Accounts Receivable", "ASSET", True),
        ("5000", "Cost of Goods Sold", "EXPENSE", True),
        ("9999", "Suspense", "CLEARING", False),
    ], columns=["gl_account", "gl_name", "account_type", "is_postable"])
    emit("erp_gl_accounts", gl_accounts)

    # -- FX: banking days only, no identity pair, correlated random walk ---
    fx_frames = []
    for ccy in ["EUR", "GBP", "CAD", "JPY"]:
        steps = rng.normal(0, 0.0045, len(bank_days))
        walk = FX_BASE[ccy] * np.exp(np.cumsum(steps) - 0.5 * 0.0045 ** 2 * np.arange(len(bank_days)))
        fx_frames.append(pd.DataFrame({
            "from_currency": ccy, "to_currency": "USD",
            "rate_date": d2s(bank_days), "rate_type": "SPOT",
            "rate": np.round(walk, 8),
            "source_system": "ECB-FEED",
            "loaded_at": np.char.add(d2s(bank_days), " 04:15:00"),
        }))
    fx = pd.concat(fx_frames, ignore_index=True)
    emit("erp_fx_rates", fx)

    # lookup used internally: rate carried forward to the last banking day
    bank_pos = np.searchsorted(all_days, bank_days)
    carry = np.repeat(np.arange(len(bank_days)), np.diff(np.append(bank_pos, n_days)))
    carry = np.concatenate([np.zeros(bank_pos[0], dtype=int), carry])[:n_days]
    fx_daily = {"USD": np.ones(n_days)}
    for ccy in ["EUR", "GBP", "CAD", "JPY"]:
        series = fx.loc[fx.from_currency == ccy, "rate"].to_numpy()
        fx_daily[ccy] = series[carry]
    T.lap("reference + fx", len(fx))

    # =====================================================================
    # Products -- launches, price rises, discontinuations, cost basis
    # =====================================================================
    n_prod = max(60, int(1200 * scale ** 0.45))
    cat_idx = rng.choice(len(CATEGORIES), n_prod, p=[0.31, 0.23, 0.17, 0.12, 0.17])
    cat = np.array(CATEGORIES)[cat_idx]

    launch_off = np.clip(rng.beta(0.7, 3.2, n_prod) * n_days, 0, n_days - 40).astype(int)
    launch_off[rng.random(n_prod) < 0.45] = 0            # most of the catalogue predates the window
    aurora = (cat == "Software License") & (rng.random(n_prod) < 0.30)
    launch_off[aurora] = (np.datetime64(EV_AURORA_LAUNCH) - np.datetime64(START)).astype(int)
    launch_date = all_days[launch_off]

    disc_flag = rng.random(n_prod) < 0.14
    disc_off = np.clip(launch_off + rng.integers(300, 1000, n_prod), 0, n_days - 1)
    disc_flag &= disc_off < n_days - 1

    base_price = np.round(np.exp(rng.normal(6.0, 1.15, n_prod)) + 29, 2)
    base_price = np.clip(base_price, 19.0, 96000.0)
    margin = np.array([CATEGORY_MARGIN[c] for c in cat]) * rng.normal(1.0, 0.09, n_prod)
    margin = np.clip(margin, 0.06, 0.94)
    unit_cost = np.round(base_price * (1 - margin), 2)

    products = pd.DataFrame({
        "product_id": np.arange(1, n_prod + 1),
        "sku": [f"NW-{c[:2].upper()}-{i:05d}" for i, c in zip(range(1, n_prod + 1), cat)],
        "product_name": [f"{WORDS_A[i % len(WORDS_A)]} {WORDS_B[(i * 7) % len(WORDS_B)]} "
                         f"{rng.integers(100, 999)}" for i in range(n_prod)],
        "category": cat,
        "product_family": np.where(aurora, "Aurora", np.where(cat_idx % 2 == 0, "Core", "Extended")),
        "list_price_usd": base_price,
        "standard_cost_usd": unit_cost,
        "launch_date": d2s(launch_date),
        "is_discontinued": disc_flag,
        "discontinued_on": np.where(disc_flag, d2s(all_days[disc_off]), None),
        "updated_at": np.char.add(d2s(launch_date), " 02:00:00"),
    })
    emit("ops_products", products)
    T.lap("products", n_prod)

    # =====================================================================
    # Sales reps -- ramp, ability, discount propensity, attrition
    # =====================================================================
    n_rep = max(12, int(220 * scale ** 0.4))
    rep_hire = all_days[np.clip((rng.beta(1.1, 1.5, n_rep) * n_days).astype(int), 0, n_days - 1)]
    rep_hire[rng.random(n_rep) < 0.45] = all_days[0]
    NAT_D = np.datetime64("NaT", "D")
    rep_left = np.where(rng.random(n_rep) < 0.22,
                        rep_hire + rng.integers(200, 900, n_rep) * np.timedelta64(1, "D"),
                        NAT_D)
    rep_left = np.where(rep_left > all_days[-1], NAT_D, rep_left)
    rep_region = rng.choice(REGIONS, n_rep, p=REGION_WEIGHT)
    rep_ability = np.clip(rng.normal(1.0, 0.21, n_rep), 0.48, 1.75)
    rep_discount_bias = np.clip(rng.normal(1.0, 0.30, n_rep), 0.35, 2.1)

    sales_reps = pd.DataFrame({
        "rep_id": [f"REP-{i:04d}" for i in range(1, n_rep + 1)],
        "rep_name": [f"{FIRST[i % len(FIRST)]} {LAST[(i * 5) % len(LAST)]}" for i in range(n_rep)],
        "region": rep_region,
        "hire_date": d2s(rep_hire),
        "departure_date": np.where(np.isnat(rep_left), None,
                                   d2s(np.where(np.isnat(rep_left), rep_hire, rep_left))),
        "quota_usd_annual": np.round(np.where(
            np.isin(rep_region, ["EMEA", "APAC"]), 1_150_000, 1_400_000) * rng.normal(1, .12, n_rep), -3),
        "manager_id": [f"MGR-{(i % max(3, n_rep // 8)) + 1:03d}" for i in range(n_rep)],
    })
    emit("crm_sales_reps", sales_reps)

    # =====================================================================
    # Marketing campaigns
    # =====================================================================
    n_camp = max(8, int(150 * scale ** 0.35))
    camp_start_off = np.sort(rng.integers(0, n_days - 30, n_camp))
    camp_len = rng.integers(14, 120, n_camp)
    campaigns = pd.DataFrame({
        "campaign_id": [f"CMP-{i:04d}" for i in range(1, n_camp + 1)],
        "campaign_name": [f"{CAMPAIGN_TYPES[i % len(CAMPAIGN_TYPES)]} "
                          f"{str(all_days[camp_start_off[i]])[:7]}" for i in range(n_camp)],
        "campaign_type": [CAMPAIGN_TYPES[i % len(CAMPAIGN_TYPES)] for i in range(n_camp)],
        "target_region": rng.choice(REGIONS + ["GLOBAL"], n_camp),
        "target_segment": rng.choice(SEGMENTS + ["ALL"], n_camp),
        "start_date": d2s(all_days[camp_start_off]),
        "end_date": d2s(all_days[np.clip(camp_start_off + camp_len, 0, n_days - 1)]),
        "budget_usd": np.round(np.exp(rng.normal(10.6, 0.85, n_camp)), -2),
        "channel_owner": [f"{FIRST[(i * 3) % len(FIRST)]} {LAST[(i * 11) % len(LAST)]}" for i in range(n_camp)],
    })
    emit("crm_campaigns", campaigns)
    T.lap("reps + campaigns", n_rep + n_camp)

    # =====================================================================
    # Customers -- acquisition, latent health, churn hazard
    # =====================================================================
    n_cust = max(200, int(75_000 * scale ** 0.62))
    acq_off = np.clip((rng.beta(1.35, 1.9, n_cust) * n_days).astype(int), 0, n_days - 1)
    acq_off[rng.random(n_cust) < 0.30] = 0               # book of business existing at t0
    seg = rng.choice(SEGMENTS, n_cust, p=SEGMENT_WEIGHT)
    region = rng.choice(REGIONS, n_cust, p=REGION_WEIGHT)
    health = np.clip(rng.beta(4.6, 2.1, n_cust), 0.05, 1.0)

    churn_yr = np.array([SEGMENT_CHURN_YR[s] for s in seg]) * (1.75 - 0.95 * health)
    # EMEA churn rises after the competitor enters; APAC after the support reorg
    life_days = rng.exponential(365.25 / np.clip(churn_yr, 0.005, 0.9))
    churn_off = acq_off + life_days.astype(int)
    emea_hit = (region == "EMEA") & (churn_off > (np.datetime64(EV_EMEA_COMPETITOR) - np.datetime64(START)).astype(int))
    churn_off[emea_hit] -= (rng.random(emea_hit.sum()) * 260).astype(int)
    apac_hit = (region == "APAC") & (churn_off > (np.datetime64(EV_APAC_SUPPORT_REORG) - np.datetime64(START)).astype(int))
    churn_off[apac_hit] -= (rng.random(apac_hit.sum()) * 190).astype(int)
    churn_off = np.maximum(churn_off, acq_off + 20)
    active_end = np.minimum(churn_off, n_days - 1)
    churned = churn_off < n_days - 1

    company_code = np.array([REGION_COMPANY[r] for r in region])
    canonical_acct = np.char.add("ACC-", np.char.mod("%06d", np.arange(1, n_cust + 1)))

    # account_number was keyed by hand before the CRM migration
    pre_mig = all_days[acq_off] < np.datetime64(EV_CRM_MIGRATION)
    variant = rng.integers(0, 5, n_cust)
    acct_ops = canonical_acct.copy().astype(object)
    idx = np.where(pre_mig)[0]
    for i in idx:
        c = canonical_acct[i]
        acct_ops[i] = [c.replace("-", " "), c.lower(), f"ACC-{i + 1}",
                       f" {c}", f"{c} "][variant[i]]

    cust_created = all_days[acq_off].astype("datetime64[s]") + (
        rng.integers(8 * 3600, 19 * 3600, n_cust)) * np.timedelta64(1, "s")

    customers = pd.DataFrame({
        "customer_id": np.arange(1, n_cust + 1),
        "account_number": acct_ops,
        "customer_name": [f"{CO_A[i % len(CO_A)]}{CO_B[(i * 13) % len(CO_B)][:0]}"
                          f"{CO_A[(i * 7) % len(CO_A)] if i % 9 == 0 else ''} "
                          f"{CO_B[(i * 3) % len(CO_B)]} {CO_C[(i * 5) % len(CO_C)]}"
                          for i in range(n_cust)],
        "segment": seg,
        "industry": rng.choice(INDUSTRIES, n_cust),
        "region": region,
        "company_code": company_code,
        "country": map_by(region, {"NA-WEST": "United States", "NA-EAST": "United States",
                                   "CANADA": "Canada", "EMEA": "Germany",
                                   "UKI": "United Kingdom", "APAC": "Japan"}),
        "employee_count": np.where(seg == "Enterprise", rng.integers(5000, 90000, n_cust),
                          np.where(seg == "Mid-Market", rng.integers(300, 5000, n_cust),
                          np.where(seg == "SMB", rng.integers(5, 300, n_cust),
                                   rng.integers(150, 30000, n_cust)))),
        "credit_limit_usd": np.round(np.exp(rng.normal(11.4, 1.0, n_cust)), 2),
        "payment_terms_days": rng.choice([15, 30, 45, 60, 90], n_cust, p=[.08, .52, .22, .13, .05]),
        "is_active": ~churned,
        "created_at": ts2s(cust_created),
        "updated_at": ts2s(cust_created + rng.integers(0, 300 * 86400, n_cust) * np.timedelta64(1, "s")),
    })
    emit("ops_customers", customers)
    T.lap("customers", n_cust)

    # =====================================================================
    # CRM accounts (same entities, CRM's view)
    # =====================================================================
    crm_acct_id = np.char.add("001", np.char.mod("%015d", np.arange(1, n_cust + 1)))
    crm_mod = cust_created + rng.integers(0, 900 * 86400, n_cust) * np.timedelta64(1, "s")
    crm_mod = np.minimum(crm_mod, np.datetime64(END).astype("datetime64[s]"))
    account_owner_ids = sales_reps["rep_id"].to_numpy()[rng.integers(0, n_rep, n_cust)]
    account_created_dates = np.char.add(np.char.replace(ts2s(cust_created), " ", "T"), ".000+0000")
    account_modified_dates = np.char.add(np.char.replace(ts2s(crm_mod), " ", "T"), ".000+0000")
    accounts = pd.DataFrame({
        "Id": crm_acct_id,
        "Name": customers["customer_name"],
        "AccountNumber": canonical_acct,
        "Industry": customers["industry"],
        "Type": rng.choice(["Customer - Direct", "Customer - Channel", "Partner"],
                           n_cust, p=[.68, .27, .05]),
        "BillingCountry": customers["country"],
        "AnnualRevenue": np.round(customers["employee_count"].to_numpy() *
                                  rng.normal(185_000, 42_000, n_cust), -3),
        "NumberOfEmployees": customers["employee_count"],
        "OwnerId": account_owner_ids,
        "CreatedById": account_owner_ids,
        "LastModifiedById": account_owner_ids,
        "CreatedDate": account_created_dates,
        "LastModifiedDate": account_modified_dates,
        "SystemModstamp": account_modified_dates,
        "IsDeleted": False,
    })

    # =====================================================================
    # Orders -- inverse-CDF sampling of seasonal dates inside each lifetime
    # =====================================================================
    tenure_yr = (active_end - acq_off) / 365.25
    lam = np.array([SEGMENT_ORDER_RATE[s] for s in seg]) * health * tenure_yr * rng.normal(1, .22, n_cust)
    n_orders_cust = rng.poisson(np.clip(lam, 0.05, None))
    total_orders = int(n_orders_cust.sum())

    cust_of_order = np.repeat(np.arange(n_cust), n_orders_cust)
    lo = day_cdf[acq_off][cust_of_order]
    hi = day_cdf[np.minimum(active_end + 1, n_days)][cust_of_order]
    u = lo + rng.random(total_orders) * np.maximum(hi - lo, 1e-12)
    o_day = np.clip(np.searchsorted(day_cdf, u) - 1, 0, n_days - 1)

    order = np.argsort(o_day, kind="stable")
    o_day = o_day[order]
    cust_of_order = cust_of_order[order]
    order_id = np.arange(1, total_orders + 1, dtype=np.int64)
    o_date = all_days[o_day]
    o_region = region[cust_of_order]
    o_seg = seg[cust_of_order]
    o_ccy = map_by(o_region, REGION_CCY)
    cross_border = rng.random(total_orders) < 0.17
    o_ccy = np.where(cross_border, "USD", o_ccy)

    o_status = rng.choice(["INVOICED", "SHIPPED", "PENDING", "CANCELLED"],
                          total_orders, p=[0.62, 0.27, 0.075, 0.035])
    recent = o_day > n_days - 12
    o_status = np.where(recent & (rng.random(total_orders) < 0.55), "PENDING", o_status)

    o_channel = rng.choice(["DIRECT", "PARTNER", "ECOMM"], total_orders, p=[.55, .24, .21])
    o_created = o_date.astype("datetime64[s]") + rng.integers(7 * 3600, 21 * 3600, total_orders) * np.timedelta64(1, "s")
    apac_out = (o_region == "APAC") & (o_date == np.datetime64(EV_APAC_OUTAGE))
    o_created = np.where(apac_out, o_created + np.timedelta64(26, "h"), o_created)
    o_updated = o_created + rng.integers(0, 72 * 3600, total_orders) * np.timedelta64(1, "s")

    T.lap("orders scaffold", total_orders)

    # =====================================================================
    # Opportunities -- funnel with real win/loss dynamics
    # =====================================================================
    # every DIRECT/PARTNER order traces to a won opportunity; ECOMM does not
    has_opp = (o_channel != "ECOMM") & (rng.random(total_orders) < 0.94)
    n_won = int(has_opp.sum())
    n_lost = int(n_won * 1.05)
    n_open = int(n_won * 0.22)
    n_opp = n_won + n_lost + n_open

    opp_id = np.char.add("006", np.char.mod("%015d", np.arange(1, n_opp + 1)))
    won_close_day = o_day[has_opp]
    lost_open_day = np.clip(np.searchsorted(day_cdf, rng.random(n_lost + n_open)) - 1, 0, n_days - 1)
    opp_close_day = np.concatenate([won_close_day, lost_open_day])
    opp_cust = np.concatenate([cust_of_order[has_opp],
                               rng.integers(0, n_cust, n_lost + n_open)])
    opp_seg = seg[opp_cust]
    opp_region = region[opp_cust]

    cycle = np.array([SEGMENT_CYCLE_D[s] for s in opp_seg])
    age = np.clip(rng.gamma(4.0, cycle / 4.0), 5, 500).astype(int)
    opp_create_day = np.clip(opp_close_day - age, 0, n_days - 1)

    rep_of_opp = rng.integers(0, n_rep, n_opp)
    rep_tenure_d = (all_days[opp_close_day] - rep_hire[rep_of_opp]).astype("timedelta64[D]").astype(int)
    ramp = np.clip(rep_tenure_d / 180.0, 0.25, 1.0)

    stage = np.empty(n_opp, dtype=object)
    stage[:n_won] = "Closed Won"
    stage[n_won:n_won + n_lost] = "Closed Lost"
    stage[n_won + n_lost:] = rng.choice(
        ["Prospecting", "Qualification", "Proposal", "Negotiation"], n_open, p=[.31, .27, .24, .18])

    # discount: policy ceiling rises in 2025, reps differ, EMEA discounts harder after the entrant
    ceiling = np.where(all_days[opp_close_day] >= np.datetime64(EV_DISCOUNT_POLICY), 32.0, 20.0)
    disc = np.clip(rng.gamma(2.0, 4.2, n_opp) * rep_discount_bias[rep_of_opp], 0, ceiling)
    emea_late = (opp_region == "EMEA") & (all_days[opp_close_day] >= np.datetime64(EV_EMEA_COMPETITOR))
    disc[emea_late] = np.clip(disc[emea_late] * 1.35, 0, ceiling[emea_late])
    disc = np.round(disc, 1)

    opp_amount = np.round(np.exp(rng.normal(
        np.select([opp_seg == "Enterprise", opp_seg == "Mid-Market", opp_seg == "SMB"],
                  [11.6, 10.3, 9.0], default=10.6), 0.85)), 2)

    lead_src = rng.choice(LEAD_SOURCES, n_opp, p=[.22, .17, .11, .19, .21, .10])
    camp_of_opp = np.where(lead_src == "Marketing Campaign",
                           rng.integers(0, n_camp, n_opp), -1)

    loss_reason = np.where(stage == "Closed Lost",
                           rng.choice(LOSS_REASONS, n_opp, p=[.27, .24, .19, .13, .11, .06]), None)
    loss_reason = np.where((stage == "Closed Lost") & emea_late & (rng.random(n_opp) < 0.45),
                           "Competitor", loss_reason)
    loss_reason = np.where(stage == "Closed Lost", loss_reason, None)

    opp_created_ts = all_days[opp_create_day].astype("datetime64[s]") + \
        rng.integers(8 * 3600, 18 * 3600, n_opp) * np.timedelta64(1, "s")
    opp_mod_ts = np.minimum(
        all_days[opp_close_day].astype("datetime64[s]") + rng.integers(0, 30 * 86400, n_opp) * np.timedelta64(1, "s"),
        np.datetime64(END).astype("datetime64[s]"))

    opportunity_owner_ids = sales_reps["rep_id"].to_numpy()[rep_of_opp]
    opportunity_created_dates = np.char.add(np.char.replace(ts2s(opp_created_ts), " ", "T"), ".000+0000")
    opportunity_modified_dates = np.char.add(np.char.replace(ts2s(opp_mod_ts), " ", "T"), ".000+0000")
    opportunities = pd.DataFrame({
        "Id": opp_id,
        "AccountId": crm_acct_id[opp_cust],
        "Name": np.char.add(np.char.add(
            customers["customer_name"].to_numpy()[opp_cust].astype(str), " - "),
            rng.choice(["Renewal", "New Business", "Upsell", "Expansion", "Refresh"], n_opp)),
        "StageName": stage,
        "Amount": opp_amount,
        "CurrencyIsoCode": map_by(opp_region, REGION_CCY),
        "CloseDate": d2s(all_days[opp_close_day]),
        "Probability": np.select(
            [stage == "Closed Won", stage == "Closed Lost", stage == "Negotiation",
             stage == "Proposal", stage == "Qualification"],
            [100, 0, 75, 50, 25], default=10),
        "LeadSource": lead_src,
        "CampaignId": np.where(camp_of_opp >= 0,
                               campaigns["campaign_id"].to_numpy()[np.maximum(camp_of_opp, 0)], None),
        "DiscountPercent": disc,
        "LossReason": loss_reason,
        "SalesCycleDays": (all_days[opp_close_day] - all_days[opp_create_day]).astype("timedelta64[D]").astype(int),
        "OwnerId": opportunity_owner_ids,
        "CreatedById": opportunity_owner_ids,
        "LastModifiedById": opportunity_owner_ids,
        "CreatedDate": opportunity_created_dates,
        "LastModifiedDate": opportunity_modified_dates,
        "SystemModstamp": opportunity_modified_dates,
        "IsDeleted": False,
    })
    # a cleanup project soft-deleted a slice of duplicates in the CRM
    del_idx = rng.choice(n_opp, int(n_opp * 0.014), replace=False)
    opportunities.loc[del_idx, "IsDeleted"] = True

    o_opp_ref = np.full(total_orders, None, dtype=object)
    o_opp_ref[has_opp] = opp_id[:n_won]
    o_discount = np.zeros(total_orders)
    o_discount[has_opp] = disc[:n_won]
    o_rep = np.full(total_orders, None, dtype=object)
    o_rep[has_opp] = sales_reps["rep_id"].to_numpy()[rep_of_opp[:n_won]]

    emit("crm_accounts", accounts)
    emit("crm_opportunities", opportunities)
    del accounts, opportunities, opp_created_ts, opp_mod_ts, opp_amount, loss_reason
    T.lap("crm accounts + opps", n_opp + n_cust)

    orders = pd.DataFrame({
        "order_id": order_id,
        "customer_id": (cust_of_order + 1).astype(np.int64),
        "opportunity_ref": o_opp_ref,
        "rep_id": o_rep,
        "order_date": d2s(o_date),
        "requested_delivery_date": d2s(o_date + rng.integers(3, 25, total_orders) * np.timedelta64(1, "D")),
        "currency_code": o_ccy,
        "status": o_status,
        "sales_channel": o_channel,
        "order_discount_pct": np.round(o_discount, 1),
        "po_number": np.where(rng.random(total_orders) < 0.72,
                              np.char.add("PO-", np.char.mod("%09d", rng.integers(1, 10 ** 9, total_orders))),
                              None),
        "created_at": ts2s(o_created),
        "updated_at": ts2s(o_updated),
    })
    emit("ops_orders", orders)
    del o_updated
    T.lap("orders", total_orders)

    # =====================================================================
    # Order lines
    # =====================================================================
    basket = np.array([SEGMENT_BASKET[s] for s in o_seg])
    n_lines_order = 1 + rng.poisson(np.maximum(basket - 1, 0.2))
    total_lines = int(n_lines_order.sum())

    line_order_pos = np.repeat(np.arange(total_orders), n_lines_order)
    l_day = o_day[line_order_pos]

    # only products already launched can be sold; older products are better established
    launch_sorted = np.sort(launch_off)
    prod_by_launch = np.argsort(launch_off, kind="stable")
    n_avail = np.searchsorted(launch_sorted, l_day, side="right")
    n_avail = np.maximum(n_avail, 1)
    pick = (rng.beta(1.0, 1.6, total_lines) * n_avail).astype(int)
    pick = np.minimum(pick, n_avail - 1)
    l_prod = prod_by_launch[pick]

    l_qty = 1 + rng.poisson(np.where(cat[l_prod] == "Consumables", 9.0, 2.2))
    price_usd = base_price[l_prod].copy()
    hw = cat[l_prod] == "Hardware"
    price_usd = np.where(hw & (all_days[l_day] >= np.datetime64(EV_PRICE_RISE)),
                         price_usd * 1.07, price_usd)

    l_ccy = o_ccy[line_order_pos]
    fx_vec = np.empty(total_lines)
    for ccy in CURRENCIES:
        m = l_ccy == ccy
        if m.any():
            fx_vec[m] = fx_daily[ccy][l_day[m]]
    unit_price = price_usd / fx_vec

    jpy_bug = (l_ccy == "JPY") & (all_days[l_day] >= np.datetime64(EV_JPY_REGRESSION_A)) \
        & (all_days[l_day] <= np.datetime64(EV_JPY_REGRESSION_B))
    unit_price = np.where(jpy_bug, unit_price * 100, unit_price)

    l_disc = np.round(np.clip(o_discount[line_order_pos] + rng.normal(0, 1.4, total_lines), 0, 45), 2)
    unit_price = rounded(unit_price, l_ccy)
    line_amount = rounded(unit_price * l_qty * (1 - l_disc / 100), l_ccy)

    l_updated = o_created[line_order_pos] + rng.integers(0, 48 * 3600, total_lines) * np.timedelta64(1, "s")
    reprice = (all_days[l_day] < np.datetime64(EV_BULK_REPRICE.date())) & (rng.random(total_lines) < 0.22)
    l_updated = np.where(reprice, np.datetime64(EV_BULK_REPRICE), l_updated)

    lines = pd.DataFrame({
        "order_id": order_id[line_order_pos],
        "product_id": (l_prod + 1).astype(np.int64),
        "quantity": l_qty.astype(np.int16),
        "unit_price": fmt_money(unit_price, l_ccy),
        "discount_pct": np.round(l_disc, 2).astype(np.float32),
        "line_amount": fmt_money(line_amount, l_ccy),
        "unit_cost_usd": unit_cost[l_prod].astype(np.float32),
        "updated_at": ts2s(l_updated),
    })
    lines = lines.drop_duplicates(subset=["order_id", "product_id"], keep="first")
    lines.insert(0, "order_line_id", np.arange(1, len(lines) + 1, dtype=np.int64))
    kept = lines.index.to_numpy()
    emit("ops_order_lines", lines)

    order_total = np.bincount(line_order_pos[kept], weights=line_amount[kept], minlength=total_orders)
    del lines, unit_price, line_amount, l_disc, l_updated, price_usd, fx_vec
    T.lap("order lines", int(len(kept)))

    # =====================================================================
    # Shipments -- carriers, lanes, promised vs actual, freight cost
    # =====================================================================
    shipworthy = np.isin(o_status, ["SHIPPED", "INVOICED"])
    sh_idx = np.where(shipworthy)[0]
    n_ship = len(sh_idx)

    wh_region = np.array([w[2] for w in WAREHOUSES])
    wh_codes = np.array([w[0] for w in WAREHOUSES])
    sh_region = o_region[sh_idx]
    wh_for_region = {}
    for r in REGIONS:
        pool = np.where(wh_region == r)[0]
        wh_for_region[r] = wh_codes[pool] if len(pool) else wh_codes
    sh_wh = np.empty(n_ship, dtype=object)
    for r in REGIONS:
        m = sh_region == r
        if m.any():
            sh_wh[m] = rng.choice(wh_for_region[r], m.sum())

    sl = rng.choice(len(SERVICE_LEVELS), n_ship, p=[.66, .19, .15])
    sl_name = np.array([s[0] for s in SERVICE_LEVELS])[sl]
    sl_days = np.array([s[1] for s in SERVICE_LEVELS])[sl]
    sl_mult = np.array([s[2] for s in SERVICE_LEVELS])[sl]

    ship_day = o_day[sh_idx] + rng.integers(0, 4, n_ship)
    ship_day = np.minimum(ship_day, n_days - 1)
    # the 2024 component shortage stretched hardware lead times
    squeeze = (all_days[ship_day] >= np.datetime64(EV_SUPPLY_SQUEEZE_A)) & \
              (all_days[ship_day] <= np.datetime64(EV_SUPPLY_SQUEEZE_B))
    ship_day = np.minimum(ship_day + np.where(squeeze, rng.integers(2, 9, n_ship), 0), n_days - 1)

    # carrier mix shifts after the 2025 contract renegotiation
    carrier_codes = np.array([c[0] for c in CARRIERS])
    pre_mix = [.44, .12, .13, .24, .07]
    post_mix = [.16, .46, .11, .20, .07]
    post = all_days[ship_day] >= np.datetime64(EV_CARRIER_SWITCH)
    sh_car = np.where(post,
                      rng.choice(carrier_codes, n_ship, p=post_mix),
                      rng.choice(carrier_codes, n_ship, p=pre_mix))

    car_cost = dict((c[0], c[3]) for c in CARRIERS)
    car_otd = dict((c[0], c[4]) for c in CARRIERS)
    cost_idx = np.array([car_cost[c] for c in sh_car])
    otd_base = np.array([car_otd[c] for c in sh_car])

    distance = np.round(np.exp(rng.normal(6.4, 0.75, n_ship)), 1)
    distance = np.where(np.isin(sh_region, ["APAC", "EMEA"]), distance * 1.25, distance)
    weight = np.round(np.exp(rng.normal(2.6, 1.05, n_ship)), 2)

    promised = ship_day + sl_days + (distance / 900).astype(int)
    # winter weather in NA-EAST, peak-season congestion in December
    winter = np.isin(pd.DatetimeIndex(all_days[ship_day]).month.to_numpy(), [1, 2, 12])
    otd_p = otd_base * np.where(winter & (sh_region == "NA-EAST"), 0.88, 1.0)
    otd_p *= np.where(pd.DatetimeIndex(all_days[ship_day]).month.to_numpy() == 12, 0.93, 1.0)
    otd_p *= np.where(squeeze, 0.94, 1.0)
    on_time = rng.random(n_ship) < np.clip(otd_p, 0.4, 0.995)
    delay = np.where(on_time, -rng.integers(0, 2, n_ship), rng.integers(1, 9, n_ship))
    delivered = np.clip(promised + delay, ship_day + 1, n_days - 1)
    delivered_known = all_days[delivered] <= np.datetime64(END)

    freight = np.round(
        (6.4 + distance * 0.0135 + weight * 0.42) * cost_idx * sl_mult *
        np.where(all_days[ship_day] >= np.datetime64(EV_CARRIER_SWITCH), 0.94, 1.0) *
        rng.normal(1, 0.11, n_ship), 2)

    shipments = pd.DataFrame({
        "shipment_id": np.arange(1, n_ship + 1, dtype=np.int64),
        "order_id": order_id[sh_idx],
        "warehouse_code": sh_wh,
        "carrier_code": sh_car,
        "service_level": sl_name,
        "ship_date": d2s(all_days[ship_day]),
        "promised_delivery_date": d2s(all_days[np.minimum(promised, n_days - 1)]),
        "delivered_date": np.where(delivered_known, d2s(all_days[delivered]), None),
        "package_count": (1 + rng.poisson(1.1, n_ship)).astype(np.int16),
        "gross_weight_kg": weight.astype(np.float32),
        "distance_km": distance.astype(np.float32),
        "freight_cost_usd": np.maximum(freight, 3.0).astype(np.float32),
        "tracking_number": np.char.add("1Z", np.char.mod("%012d", rng.integers(1, 10 ** 12, n_ship))),
    })
    emit("ops_shipments", shipments)
    del shipments, distance, weight, freight, promised, delivered
    T.lap("shipments", n_ship)

    # =====================================================================
    # Support cases -- quality varies by region and over time
    # =====================================================================
    case_rate = np.clip((1.6 - health) * np.array([
        {"Enterprise": 7.0, "Mid-Market": 3.4, "SMB": 1.5, "Public Sector": 3.0}[s] for s in seg]) *
        np.maximum(tenure_yr, 0.05), 0.02, None)
    n_case_cust = rng.poisson(case_rate)
    n_cases = int(n_case_cust.sum())
    case_cust = np.repeat(np.arange(n_cust), n_case_cust)
    clo = day_cdf[acq_off][case_cust]
    chi = day_cdf[np.minimum(active_end + 1, n_days)][case_cust]
    c_day = np.clip(np.searchsorted(day_cdf, clo + rng.random(n_cases) * np.maximum(chi - clo, 1e-12)) - 1,
                    0, n_days - 1)
    c_region = region[case_cust]
    c_pri = rng.choice(CASE_PRIORITIES, n_cases, p=[.07, .21, .48, .24])
    base_hours = np.select([c_pri == "P1", c_pri == "P2", c_pri == "P3"],
                           [4.0, 14.0, 42.0], default=76.0)
    apac_reorg = (c_region == "APAC") & (all_days[c_day] >= np.datetime64(EV_APAC_SUPPORT_REORG))
    res_hours = np.round(rng.gamma(2.4, base_hours / 2.4) * np.where(apac_reorg, 2.15, 1.0), 2)
    csat = np.clip(np.round(5.4 - 0.9 * np.log1p(res_hours / 24) + rng.normal(0, 0.55, n_cases)), 1, 5)
    still_open = rng.random(n_cases) < 0.045
    # CSAT is an integer 1-5 and is missing whenever the customer did not respond.
    # A nullable Int16 keeps it out of float land, so it serialises as "4" not "4.0".
    no_response = still_open | (rng.random(n_cases) < 0.42)
    csat_out = pd.array(csat.astype(np.int16), dtype="Int16")
    csat_out[no_response] = pd.NA

    cases = pd.DataFrame({
        "case_id": np.arange(1, n_cases + 1, dtype=np.int64),
        "customer_id": (case_cust + 1).astype(np.int64),
        "case_type": rng.choice(CASE_TYPES, n_cases),
        "priority": c_pri,
        "channel": rng.choice(["Email", "Phone", "Portal", "Chat"], n_cases, p=[.34, .22, .31, .13]),
        "opened_at": ts2s(all_days[c_day].astype("datetime64[s]") +
                          rng.integers(0, 86400, n_cases) * np.timedelta64(1, "s")),
        "resolution_hours": np.where(still_open, np.nan, res_hours).astype(np.float32),
        "status": np.where(still_open, "Open", rng.choice(["Resolved", "Closed"], n_cases, p=[.35, .65])),
        "csat_score": csat_out,
        "assigned_region": c_region,
    })
    emit("ops_support_cases", cases)
    del cases
    T.lap("support cases", n_cases)

    # =====================================================================
    # ERP revenue postings
    # =====================================================================
    postable = np.isin(o_status, ["INVOICED", "SHIPPED"]) & (order_total > 0)
    p_idx = np.where(postable)[0]
    n_inv = len(p_idx)

    p_day = o_day[p_idx]
    p_comp = company_code[cust_of_order[p_idx]]
    p_doc_ccy = o_ccy[p_idx]
    p_comp_ccy = np.array([COMPANY_CCY[c] for c in p_comp])

    fx_doc = np.empty(n_inv)
    fx_comp = np.empty(n_inv)
    for ccy in CURRENCIES:
        m = p_doc_ccy == ccy
        if m.any():
            fx_doc[m] = fx_daily[ccy][p_day[m]]
        m2 = p_comp_ccy == ccy
        if m2.any():
            fx_comp[m2] = fx_daily[ccy][p_day[m2]]

    amt_doc = rounded(order_total[p_idx], p_doc_ccy)
    amt_comp = rounded(amt_doc * fx_doc / fx_comp, p_comp_ccy)

    # cost centre valid as of the posting date, within the right entity
    cc_codes = cost_centers["cost_center_code"].to_numpy()
    cc_comp = cost_centers["company_code"].to_numpy()
    cc_from = cost_centers["valid_from"].to_numpy().astype("datetime64[D]")
    cc_to = np.where(cost_centers["valid_to"].to_numpy() == "", np.datetime64("2999-01-01"),
                     cost_centers["valid_to"].to_numpy()).astype("datetime64[D]")
    p_cc = np.empty(n_inv, dtype=object)
    for comp in COMPANY_CCY:
        m = p_comp == comp
        if not m.any():
            continue
        pool = np.where(cc_comp == comp)[0]
        days_here = all_days[p_day[m]]
        choice = rng.integers(0, len(pool), m.sum())
        sel = pool[choice]
        bad = (cc_from[sel] > days_here) | (cc_to[sel] <= days_here)
        for _ in range(6):
            if not bad.any():
                break
            choice[bad] = rng.integers(0, len(pool), bad.sum())
            sel = pool[choice]
            bad = (cc_from[sel] > days_here) | (cc_to[sel] <= days_here)
        p_cc[m] = cc_codes[sel]

    gl_pick = rng.choice(["4000", "4010", "4020", "4030"], n_inv, p=[.44, .27, .17, .12])

    # posted_at: same day normally, later during month-end close, occasionally backdated
    month_arr = pd.DatetimeIndex(all_days[p_day]).month.to_numpy()
    dom_arr = pd.DatetimeIndex(all_days[p_day]).day.to_numpy()
    eom = dom_arr >= 27
    lag = np.where(eom & (rng.random(n_inv) < 0.6), rng.integers(1, 6, n_inv), 0)
    lag = np.where(rng.random(n_inv) < 0.012, rng.integers(10, 34, n_inv), lag)
    posted_day = np.minimum(p_day + lag, n_days - 1)
    posted_ts = all_days[posted_day].astype("datetime64[s]") + \
        rng.integers(3 * 3600, 23 * 3600, n_inv) * np.timedelta64(1, "s")

    doc_seq = np.arange(1, n_inv + 1)
    inv = pd.DataFrame({
        "posting_id": np.arange(1, n_inv + 1, dtype=np.int64),
        "document_number": np.char.add(np.char.add(p_comp.astype(str), "-"),
                                       np.char.mod("%09d", doc_seq)),
        "document_type": "INV",
        "company_code": p_comp,
        "order_ref": pd.array(order_id[p_idx], dtype="Int64"),
        "gl_account": gl_pick,
        "cost_center_code": p_cc,
        "posting_date": d2s(all_days[p_day]),
        "fiscal_period": np.char.add(np.char.add(
            np.char.mod("%d", pd.DatetimeIndex(all_days[p_day]).year.to_numpy()), "-"),
            np.char.mod("%02d", month_arr)),
        "document_currency": p_doc_ccy,
        "amount_doc": fmt_money(amt_doc, p_doc_ccy),
        "company_currency": p_comp_ccy,
        "amount_company": fmt_money(amt_comp, p_comp_ccy),
        "reverses_posting_id": pd.array([pd.NA] * n_inv, dtype="Int64"),
        "posted_at": ts2s(posted_ts),
    })

    # credit notes reverse a slice of invoices, usually in a later period
    crn_sel = rng.choice(n_inv, int(n_inv * 0.027), replace=False)
    crn_day = np.minimum(p_day[crn_sel] + rng.integers(4, 40, len(crn_sel)), n_days - 1)
    crn_posted = all_days[crn_day].astype("datetime64[s]") + \
        rng.integers(6 * 3600, 21 * 3600, len(crn_sel)) * np.timedelta64(1, "s")
    crn = pd.DataFrame({
        "posting_id": np.arange(n_inv + 1, n_inv + 1 + len(crn_sel), dtype=np.int64),
        "document_number": np.char.add(np.char.add(p_comp[crn_sel].astype(str), "-"),
                                       np.char.mod("%09d", np.arange(n_inv + 1, n_inv + 1 + len(crn_sel)))),
        "document_type": "CRN",
        "company_code": p_comp[crn_sel],
        "order_ref": pd.array(order_id[p_idx][crn_sel], dtype="Int64"),
        "gl_account": gl_pick[crn_sel],
        "cost_center_code": p_cc[crn_sel],
        "posting_date": d2s(all_days[crn_day]),
        "fiscal_period": np.char.add(np.char.add(
            np.char.mod("%d", pd.DatetimeIndex(all_days[crn_day]).year.to_numpy()), "-"),
            np.char.mod("%02d", pd.DatetimeIndex(all_days[crn_day]).month.to_numpy())),
        "document_currency": p_doc_ccy[crn_sel],
        "amount_doc": fmt_money(-amt_doc[crn_sel], p_doc_ccy[crn_sel]),
        "company_currency": p_comp_ccy[crn_sel],
        "amount_company": fmt_money(-amt_comp[crn_sel], p_comp_ccy[crn_sel]),
        "reverses_posting_id": pd.array((crn_sel + 1).astype(np.int64), dtype="Int64"),
        "posted_at": ts2s(crn_posted),
    })

    # period-level adjustments: no order, no cost centre
    periods = sorted({(d.year, d.month) for d in pd.DatetimeIndex(all_days).date})
    adj_rows = []
    pid = n_inv + len(crn_sel)
    for (yy, mm) in periods:
        pend = max(d for d in all_days if pd.Timestamp(d).year == yy and pd.Timestamp(d).month == mm)
        for comp in COMPANY_CCY:
            if rng.random() < 0.55:
                continue
            pid += 1
            ccy = COMPANY_CCY[comp]
            amt = round(float(rng.normal(0, 55_000)), MINOR_UNITS[ccy])
            if amt == 0:
                amt = 1250.0
            adj_rows.append({
                "posting_id": pid,
                "document_number": f"{comp}-{pid:09d}",
                "document_type": "ADJ", "company_code": comp, "order_ref": pd.NA,
                "gl_account": "4090", "cost_center_code": None,
                "posting_date": str(pd.Timestamp(pend).date()),
                "fiscal_period": f"{yy}-{mm:02d}",
                "document_currency": ccy, "amount_doc": float(amt),
                "company_currency": ccy, "amount_company": float(amt),
                "reverses_posting_id": pd.NA,
                "posted_at": str(pd.Timestamp(pend).date() + timedelta(days=2)) + " 09:30:00",
            })
    adj = pd.DataFrame(adj_rows)
    if len(adj):
        adj["order_ref"] = pd.array([pd.NA] * len(adj), dtype="Int64")
        adj["reverses_posting_id"] = pd.array([pd.NA] * len(adj), dtype="Int64")
    postings = pd.concat([inv, crn, adj], ignore_index=True)
    # Pin every column's type once, so concatenating frames built different ways
    # cannot leave an object column that CSV would tolerate and parquet rejects.
    for col in ("order_ref", "reverses_posting_id"):
        postings[col] = postings[col].astype("Int64")
    for col in ("posting_id",):
        postings[col] = postings[col].astype("int64")
    for col in ("amount_doc", "amount_company"):
        postings[col] = postings[col].astype("float64")
    emit("erp_revenue_postings", postings)
    del inv, crn, postings
    T.lap("erp postings", n_inv + len(crn_sel) + len(adj_rows))

    total_rows = sum(written.values())
    print(f"\n  {'TOTAL':<26} {total_rows:>11,} rows   {T.total():6.1f}s")
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", type=float, default=1.0,
                    help="1.0 gives roughly 7-8M rows; scales super-linearly on facts")
    ap.add_argument("--format", choices=["csv.gz", "csv", "parquet"], default="csv.gz")
    ap.add_argument("--out", type=Path, default=SEED_DIR)
    ap.add_argument("--seed", type=int, default=20260728)
    args = ap.parse_args()

    print(f"Northwind Systems  |  {START} .. {END}  |  scale={args.scale}  format={args.format}\n")
    written = generate(args.scale, args.format, args.out, args.seed)
    (args.out / "_manifest.json").write_text(json.dumps(
        {"generated_at": datetime.now(timezone.utc).isoformat(), "scale": args.scale,
         "seed": args.seed, "window": [str(START), str(END)], "row_counts": written},
        indent=2), encoding="utf-8")
    print(f"\nWrote {len(written)} datasets to {args.out}")


if __name__ == "__main__":
    main()
