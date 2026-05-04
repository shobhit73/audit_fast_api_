"""Paycom - Prior Payroll Setup Helper (MCP core).

Mirror of core/adp/prior_payroll_setup_helper.py for Paycom files.
Given a Paycom Prior Payroll Register (long format) plus a Paycom Scheduled
Deductions report, emits a 3-tab Excel workbook answering:

  Tab 1 - What to Set Up in Uzio (Earnings | Contributions | Deductions)
  Tab 2 - Pre-Tax vs Post-Tax (read straight from the Tax Treatment column
          of the Scheduled Deductions report - no algorithm needed, Paycom
          tells us directly via 'B - S125 Pre-Tax', 'H - FICA/FUTA/SUTA
          Taxable Only (401k)', 'A - After Tax Deduction')
  Tab 3 - Bonus Verdict (FLSA discretionary vs non-discretionary).
          Strategy A+C:
            (C) If the Prior Payroll Register has both 'OT' (plain overtime)
                AND 'WOT' (Paycom's FLSA-weighted overtime) lines for the
                same employee+period, compare them. WOT materially higher
                than OT means Paycom internally rolled a bonus into the
                regular rate => non-discretionary.
            (A) If the file lacks the OT-vs-WOT differential (or has no
                bonus codes), return 'indeterminate' with a clear note
                asking the user to supply a Payroll Register Detail with
                hours, OR confirm explicitly which kind of bonus it is.
"""

from __future__ import annotations
import io
import re

import pandas as pd


# ---------- helpers ----------

def _num(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)) and not pd.isna(v):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "")
    if s in ("", "-", "nan", "NaT", "None"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _read_either(content: bytes, filename: str) -> pd.DataFrame:
    """Read .xlsx/.csv into a DataFrame, dtype default (numbers stay numeric)."""
    name = (filename or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    return pd.read_excel(io.BytesIO(content))


# ---------- Tab 1: What to Set Up ----------

CONTRIB_PATTERN = re.compile(
    r"\b(401[Kk]?|403[Bb]?|457|ROTH|HSA|FSA|RETIREMENT)\b"
)


def build_earnings_catalog(prior_df: pd.DataFrame) -> list[dict]:
    """Distinct (Type Code, Type Description) where Code Description == 'Earnings'."""
    if "Code Description" not in prior_df.columns:
        return []
    earn = prior_df[prior_df["Code Description"].astype(str).str.strip() == "Earnings"]
    rows = []
    seen = set()
    for _, r in earn.iterrows():
        tc = str(r.get("Type Code", "")).strip()
        td = str(r.get("Type Description", "")).strip()
        key = (tc, td)
        if not tc or key in seen:
            continue
        seen.add(key)
        amt = earn[(earn["Type Code"] == tc) & (earn["Type Description"] == td)]["Amount"].apply(_num)
        rows.append({
            "Type Code": tc,
            "Type Description": td,
            "Total $": round(float(amt.sum()), 2),
            "Employees": int(len(amt[amt != 0])),
        })
    return rows


def build_taxes_discovered(prior_df: pd.DataFrame) -> list[dict]:
    """Distinct (Type Code, Type Description) where Code Description == 'W/H Taxes'."""
    if "Code Description" not in prior_df.columns:
        return []
    tax = prior_df[prior_df["Code Description"].astype(str).str.strip() == "W/H Taxes"]
    rows = []
    seen = set()
    for _, r in tax.iterrows():
        tc = str(r.get("Type Code", "")).strip()
        td = str(r.get("Type Description", "")).strip()
        key = (tc, td)
        if not tc or key in seen:
            continue
        seen.add(key)
        amt = tax[(tax["Type Code"] == tc) & (tax["Type Description"] == td)]["Amount"].apply(_num)
        rows.append({
            "Type Code": tc,
            "Type Description": td,
            "Total $": round(float(amt.sum()), 2),
            "Employees": int(len(amt[amt != 0])),
        })
    return rows


def split_contribs_deductions(scheduled_df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Distinct (Deduction Code, Deduction Desc) from Scheduled Deductions.
    Split into contributions vs deductions by name pattern.
    Returns (contributions, deductions)."""
    if "Deduction Code" not in scheduled_df.columns:
        return [], []
    rows = []
    seen = set()
    for _, r in scheduled_df.iterrows():
        dc = str(r.get("Deduction Code", "")).strip()
        dd = str(r.get("Deduction Desc", "")).strip()
        key = (dc, dd)
        if not dc or key in seen:
            continue
        seen.add(key)
        rows.append({
            "Deduction Code": dc,
            "Deduction Desc": dd,
            "Setup Count": int(((scheduled_df["Deduction Code"] == dc)
                                & (scheduled_df["Deduction Desc"] == dd)).sum()),
        })

    contribs, deds = [], []
    for r in rows:
        u = (r["Deduction Code"] + " " + r["Deduction Desc"]).upper()
        if CONTRIB_PATTERN.search(u):
            contribs.append(r)
        else:
            deds.append(r)
    return contribs, deds


# ---------- Tab 2: Pre/Post-Tax (read from Tax Treatment column) ----------

def classify_pre_post_tax(scheduled_df: pd.DataFrame) -> list[dict]:
    """Map each Deduction Code to a verdict using the Tax Treatment column.

    Paycom values seen in the wild:
      'B - S125 Pre-Tax'                           -> PRE-TAX, Section 125
      'H - FICA/FUTA/SUTA Taxable Only (401k)'    -> PRE-TAX, 401k traditional
      'A - After Tax Deduction'                    -> POST-TAX
      '' / NaN                                     -> unknown (review)
    """
    if "Deduction Code" not in scheduled_df.columns or "Tax Treatment" not in scheduled_df.columns:
        return []
    rows = []
    grouped = scheduled_df.groupby(["Deduction Code", "Deduction Desc"], dropna=False)
    for (dc, dd), grp in grouped:
        treatments = grp["Tax Treatment"].dropna().astype(str).str.strip().unique().tolist()
        # Pick the most common; warn if multiple
        if not treatments:
            verdict, flavor, why = "unknown", "", "Tax Treatment column was blank for every row of this deduction."
        else:
            primary = grp["Tax Treatment"].dropna().astype(str).str.strip().mode()
            tt = primary.iloc[0] if not primary.empty else treatments[0]
            tt_upper = tt.upper()
            if tt_upper.startswith("B"):
                verdict, flavor = "PRE-TAX", "Section 125"
                why = f"Tax Treatment '{tt}' = Section 125 cafeteria plan (reduces FIT, FICA, Medicare, and state-income taxable wages)."
            elif tt_upper.startswith("H"):
                verdict, flavor = "PRE-TAX", "401k traditional"
                why = f"Tax Treatment '{tt}' = traditional 401(k) (reduces FIT and SIT but NOT FICA/Medicare)."
            elif tt_upper.startswith("A"):
                verdict, flavor = "POST-TAX", ""
                why = f"Tax Treatment '{tt}' = post-tax deduction (does not reduce taxable wages)."
            else:
                verdict, flavor = "unknown", "review"
                why = f"Tax Treatment '{tt}' is not a recognized Paycom code -- please review manually."
            if len(treatments) > 1:
                why += f"  (Multiple distinct Tax Treatments seen: {treatments}; using the most common.)"
        rows.append({
            "Code": str(dc).strip(),
            "Description": str(dd).strip() if dd is not None else "",
            "Verdict": verdict,
            "Flavor": flavor,
            "Why": why,
        })
    return rows


# ---------- Tab 3: Bonus FLSA verdict (Strategy A+C) ----------

BONUS_RE = re.compile(r"\b(BONUS|BNS|BND|BNH|BN[0-9]?|NA[0-9])\b", re.IGNORECASE)


def classify_bonus(prior_df: pd.DataFrame) -> dict:
    """A+C strategy:
      C - If both 'OT' (plain) and 'WOT' (weighted) lines exist for the same
          employee in the same pay period AND they differ materially
          (>0.5%), Paycom rolled a bonus into the regular rate
          => non-discretionary.
      A - Otherwise indeterminate; tell the user to supply hours data.
    """
    if "Code Description" not in prior_df.columns or "Type Code" not in prior_df.columns:
        return {
            "verdict": "indeterminate",
            "reason": "Prior Payroll Register is missing Code Description / Type Code columns.",
            "bonus_codes_found": [],
            "samples": [],
        }
    earn = prior_df[prior_df["Code Description"].astype(str).str.strip() == "Earnings"]

    # Find bonus-shaped Type Codes anywhere in the Earnings rows.
    bonus_codes = sorted({
        str(r["Type Code"]).strip()
        for _, r in earn.iterrows()
        if BONUS_RE.search(f"{r.get('Type Code', '')} {r.get('Type Description', '')}".upper())
    })

    # Find OT vs WOT pairs (Strategy C).
    ot_codes = ["OT", "OVT", "OVR"]            # plain overtime variants
    wot_codes = ["WOT"]                          # weighted overtime
    has_ot = any(c in earn["Type Code"].astype(str).unique() for c in ot_codes)
    has_wot = any(c in earn["Type Code"].astype(str).unique() for c in wot_codes)

    if not bonus_codes:
        return {
            "verdict": "no_bonus_in_file",
            "reason": ("No bonus codes found in the Prior Payroll Register. "
                       "(Looked for Type Codes containing BONUS / BNS / BND / BNH / BN# / NA#.) "
                       "If a bonus exists outside this pay period, supply that file too."),
            "bonus_codes_found": [],
            "ot_present": has_ot,
            "wot_present": has_wot,
            "samples": [],
        }

    # Strategy C requires BOTH plain OT and WOT lines to compare.
    if not (has_ot and has_wot):
        # Cannot run C; fall to A.
        msg_parts = []
        if has_wot and not has_ot:
            msg_parts.append("File contains only Paycom's WOT (weighted overtime) lines; "
                             "the plain-OT comparison line is absent so the WOT-vs-OT differential "
                             "test cannot run.")
        elif has_ot and not has_wot:
            msg_parts.append("File contains only plain-OT lines; the WOT (weighted overtime) "
                             "comparison is absent.")
        else:
            msg_parts.append("File contains neither OT nor WOT lines.")
        msg_parts.append(
            "To classify the bonus, supply a Paycom Payroll Register Detail report (which "
            "exposes Reg Hours / OT Hours columns) so the standard FLSA test can run, OR confirm "
            "the bonus type with the implementer directly."
        )
        return {
            "verdict": "indeterminate",
            "reason": " ".join(msg_parts),
            "bonus_codes_found": bonus_codes,
            "ot_present": has_ot,
            "wot_present": has_wot,
            "samples": [],
        }

    # Strategy C: compare OT and WOT amounts per employee.
    pivot = earn.pivot_table(
        index="EE Code", columns="Type Code", values="Amount",
        aggfunc=lambda s: float(sum(_num(v) for v in s)), fill_value=0.0,
    )
    samples = []
    differential_rows = 0
    matching_rows = 0
    rate_tol_pct = 0.005
    for eid, row in pivot.iterrows():
        ot_amt = sum(_num(row[c]) for c in ot_codes if c in row.index)
        wot_amt = sum(_num(row[c]) for c in wot_codes if c in row.index)
        bonus_amt = sum(_num(row[c]) for c in bonus_codes if c in row.index)
        if ot_amt <= 0 or wot_amt <= 0 or bonus_amt <= 0:
            continue
        diff_pct = (wot_amt - ot_amt) / ot_amt if ot_amt > 0 else 0.0
        if diff_pct > rate_tol_pct:
            differential_rows += 1
        else:
            matching_rows += 1
        if len(samples) < 5:
            samples.append({
                "employee": str(eid),
                "plain_ot_amount": round(ot_amt, 2),
                "weighted_ot_amount": round(wot_amt, 2),
                "differential_pct": round(diff_pct * 100, 3),
                "bonus_amount": round(bonus_amt, 2),
                "row_verdict": "non_discretionary" if diff_pct > rate_tol_pct else "discretionary",
            })

    rows_tested = differential_rows + matching_rows
    if rows_tested == 0:
        return {
            "verdict": "indeterminate",
            "reason": ("Bonus codes were found but no employee in this pay period had both "
                       "OT, WOT, and a bonus amount in the same row. Cannot run the "
                       "WOT-vs-OT differential test."),
            "bonus_codes_found": bonus_codes,
            "ot_present": has_ot,
            "wot_present": has_wot,
            "samples": [],
        }
    if differential_rows > 0:
        return {
            "verdict": "non_discretionary",
            "reason": (f"{differential_rows} of {rows_tested} employees show Paycom's WOT "
                       f"(weighted overtime) materially higher than plain OT. Paycom rolls "
                       f"non-discretionary bonuses into the regular rate before computing the "
                       f"weighted OT, so any positive WOT-vs-OT gap means the bonus is "
                       f"non-discretionary under FLSA."),
            "bonus_codes_found": bonus_codes,
            "rows_tested": rows_tested,
            "differential_rows": differential_rows,
            "matching_rows": matching_rows,
            "samples": samples,
        }
    return {
        "verdict": "discretionary",
        "reason": (f"All {rows_tested} tested employees show WOT == plain OT (no weighted "
                   f"adjustment). Paycom did NOT roll the bonus into the regular rate, so the "
                   f"bonus is discretionary."),
        "bonus_codes_found": bonus_codes,
        "rows_tested": rows_tested,
        "differential_rows": 0,
        "matching_rows": matching_rows,
        "samples": samples,
    }


# ---------- xlsx writer (3-tab simplified) ----------

def _pick_bonus_example(bonus_info: dict):
    samples = bonus_info.get("samples", [])
    if not samples:
        return None
    verdict = bonus_info["verdict"]
    if verdict == "non_discretionary":
        cands = [s for s in samples if s["row_verdict"] == "non_discretionary"]
        return max(cands, key=lambda s: s["differential_pct"]) if cands else samples[0]
    if verdict == "discretionary":
        cands = [s for s in samples if s["row_verdict"] == "discretionary"]
        return min(cands, key=lambda s: abs(s["differential_pct"])) if cands else samples[0]
    return samples[0]


def build_simplified_xlsx_bytes(results: dict) -> bytes:
    """Three-tab simplified xlsx, mirrors the ADP setup_helper output."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        wb = writer.book
        header_fmt = wb.add_format({
            "bold": True, "bg_color": "#1F4E78", "font_color": "white",
            "border": 1, "align": "left", "valign": "vcenter",
        })
        wrap_fmt = wb.add_format({"valign": "top", "text_wrap": True})
        v_pre = wb.add_format({"bold": True, "bg_color": "#C6EFCE",
                               "font_color": "#006100", "align": "center", "valign": "vcenter"})
        v_post = wb.add_format({"bold": True, "bg_color": "#FFC7CE",
                                "font_color": "#9C0006", "align": "center", "valign": "vcenter"})
        v_nondisc = wb.add_format({"bold": True, "bg_color": "#FFC7CE",
                                   "font_color": "#9C0006", "align": "left",
                                   "valign": "vcenter", "font_size": 14})
        v_disc = wb.add_format({"bold": True, "bg_color": "#C6EFCE",
                                "font_color": "#006100", "align": "left",
                                "valign": "vcenter", "font_size": 14})

        # Tab 1: What to Set Up
        earn = [r["Type Code"] + " - " + r["Type Description"] for r in results["Earnings_Codes"]]
        contrib = [r["Deduction Code"] + " - " + r["Deduction Desc"] for r in results["Contributions"]]
        ded = [r["Deduction Code"] + " - " + r["Deduction Desc"] for r in results["Deductions"]]
        max_n = max(len(earn), len(contrib), len(ded), 1)
        rows1 = [{
            "Earnings": earn[i] if i < len(earn) else "",
            "Contributions": contrib[i] if i < len(contrib) else "",
            "Deductions": ded[i] if i < len(ded) else "",
        } for i in range(max_n)]
        df1 = pd.DataFrame(rows1)
        df1.to_excel(writer, sheet_name="1. What to Set Up", index=False)
        ws1 = writer.sheets["1. What to Set Up"]
        ws1.set_column("A:A", 38); ws1.set_column("B:B", 32); ws1.set_column("C:C", 38)
        for i, c in enumerate(df1.columns):
            ws1.write(0, i, c, header_fmt)
        ws1.set_row(0, 24)

        # Tab 2: Pre/Post-Tax
        rows2 = []
        for r in results["Pre_Post_Tax"]:
            rows2.append({
                "Code": r["Code"],
                "Description": r["Description"],
                "Verdict": r["Verdict"],
                "Flavor": r["Flavor"],
                "Why": r["Why"],
            })
        if not rows2:
            rows2 = [{"Code": "(none)", "Description": "", "Verdict": "",
                      "Flavor": "", "Why": "Scheduled Deductions report had no rows."}]
        df2 = pd.DataFrame(rows2)
        df2.to_excel(writer, sheet_name="2. Pre-Tax vs Post-Tax", index=False)
        ws2 = writer.sheets["2. Pre-Tax vs Post-Tax"]
        ws2.set_column("A:A", 14); ws2.set_column("B:B", 30)
        ws2.set_column("C:C", 11); ws2.set_column("D:D", 20)
        ws2.set_column("E:E", 90, wrap_fmt)
        for i, c in enumerate(df2.columns):
            ws2.write(0, i, c, header_fmt)
        ws2.set_row(0, 24)
        for ri, r in enumerate(rows2, start=1):
            v = r["Verdict"]
            if v == "PRE-TAX":
                ws2.write(ri, 2, "PRE-TAX", v_pre)
            elif v == "POST-TAX":
                ws2.write(ri, 2, "POST-TAX", v_post)
            ws2.set_row(ri, 30)

        # Tab 3: Bonus Verdict
        bonus = results["Bonus"]
        sample = _pick_bonus_example(bonus)
        verdict_label = bonus["verdict"].upper().replace("_", "-")
        rows3 = [
            ("Verdict", verdict_label),
            ("Reason", bonus["reason"]),
            ("Bonus codes detected", ", ".join(bonus.get("bonus_codes_found", [])) or "(none)"),
        ]
        if "rows_tested" in bonus:
            rows3 += [
                ("Employees tested", bonus.get("rows_tested", 0)),
                ("    of which non-discretionary (WOT > OT)", bonus.get("differential_rows", 0)),
                ("    of which discretionary (WOT == OT)", bonus.get("matching_rows", 0)),
            ]
        if sample:
            rows3 += [
                ("", ""),
                ("---- Example employee that proves the verdict ----", ""),
                ("Employee", sample["employee"]),
                ("Plain OT amount (Paycom 'OT')", f"${sample['plain_ot_amount']:,}"),
                ("Weighted OT amount (Paycom 'WOT', FLSA-corrected)", f"${sample['weighted_ot_amount']:,}"),
                ("Differential (%)", f"{sample['differential_pct']}%"),
                ("Bonus amount in this period", f"${sample['bonus_amount']:,}"),
                ("", ""),
                ("Plain-English explanation",
                    "WOT > OT => Paycom rolled the bonus into the regular rate before "
                    "calculating the weighted OT. Per FLSA, that means the bonus is "
                    "NON-DISCRETIONARY."
                    if bonus["verdict"] == "non_discretionary" else
                    "WOT matches plain OT exactly => Paycom did NOT roll the bonus into the "
                    "regular rate => bonus is DISCRETIONARY."
                    if bonus["verdict"] == "discretionary" else
                    bonus["reason"]),
            ]
        df3 = pd.DataFrame(rows3, columns=["Field", "Value"])
        df3.to_excel(writer, sheet_name="3. Bonus Verdict", index=False)
        ws3 = writer.sheets["3. Bonus Verdict"]
        ws3.set_column("A:A", 50); ws3.set_column("B:B", 80, wrap_fmt)
        for i, c in enumerate(df3.columns):
            ws3.write(0, i, c, header_fmt)
        ws3.set_row(0, 24)
        if bonus["verdict"] == "non_discretionary":
            ws3.write(1, 1, verdict_label, v_nondisc)
        elif bonus["verdict"] == "discretionary":
            ws3.write(1, 1, verdict_label, v_disc)
        ws3.set_row(1, 28)

    return buf.getvalue()


# ---------- orchestrator ----------

def run_paycom_prior_payroll_setup_helper(
    prior_payroll_content: bytes,
    prior_payroll_filename: str,
    scheduled_deductions_content: bytes,
    scheduled_deductions_filename: str,
):
    """Returns (results_dict, xlsx_bytes).

    results_dict has keys: Earnings_Codes, Contributions, Deductions,
    Taxes_Discovered, Pre_Post_Tax, Bonus.
    """
    prior_df = _read_either(prior_payroll_content, prior_payroll_filename)
    sched_df = _read_either(scheduled_deductions_content, scheduled_deductions_filename)

    earnings = build_earnings_catalog(prior_df)
    taxes = build_taxes_discovered(prior_df)
    contributions, deductions = split_contribs_deductions(sched_df)
    pre_post = classify_pre_post_tax(sched_df)
    bonus = classify_bonus(prior_df)

    summary = [
        {"Metric": "Prior Payroll Register rows", "Value": int(len(prior_df))},
        {"Metric": "Scheduled Deductions rows", "Value": int(len(sched_df))},
        {"Metric": "Distinct earnings codes", "Value": len(earnings)},
        {"Metric": "Distinct contribution codes", "Value": len(contributions)},
        {"Metric": "Distinct deduction codes", "Value": len(deductions)},
        {"Metric": "Distinct tax codes", "Value": len(taxes)},
        {"Metric": "Bonus verdict", "Value": bonus["verdict"]},
    ]

    results = {
        "Summary": summary,
        "Earnings_Codes": earnings,
        "Contributions": contributions,
        "Deductions": deductions,
        "Taxes_Discovered": taxes,
        "Pre_Post_Tax": pre_post,
        "Bonus": bonus,
    }
    xlsx_bytes = build_simplified_xlsx_bytes(results)
    return results, xlsx_bytes
