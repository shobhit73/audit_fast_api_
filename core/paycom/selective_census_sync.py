"""Paycom Selective Census Sync (MCP core).

Pure-Python port of the Streamlit `apps/paycom/census_generator.py`'s
`render_selective_census_generator` entry point. Updates only the requested
columns in a pre-filled Uzio Census Template (.xlsm) using values from a
fresh Paycom census export.

Identical shape to core/adp/selective_census_sync; only the source-side
field map differs (PAYCOM_FIELD_MAP).
"""

import io
import pandas as pd

from utils.audit_utils import (
    norm_colname, normalize_id, UZIO_RAW_MAPPING,
    read_uzio_template_df, extract_mappings_from_uzio,
    selective_update_uzio, inject_into_uzio_template,
)
from core.paycom.census_audit import PAYCOM_FIELD_MAP


def _read_paycom_source(content, filename):
    """Read a Paycom census file. Mirrors Streamlit `preprocess_paycom_file`."""
    name = (filename or "").lower()
    if name.endswith(".csv"):
        try:
            df = pd.read_csv(io.BytesIO(content), dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="latin1")
    else:
        df = pd.read_excel(io.BytesIO(content), dtype=str)

    df.columns = [norm_colname(c) for c in df.columns]

    resolved_field_map = {}
    for std_name, vendor_cols in PAYCOM_FIELD_MAP.items():
        if isinstance(vendor_cols, str):
            vendor_cols = [vendor_cols]
        for vc in vendor_cols:
            norm_vc = norm_colname(vc)
            if norm_vc in df.columns:
                resolved_field_map[std_name] = norm_vc
                break
        else:
            resolved_field_map[std_name] = norm_colname(vendor_cols[0])
    return df, resolved_field_map


def discover_mappings(paycom_content, paycom_filename, uzio_template_content):
    df_source, resolved_field_map = _read_paycom_source(paycom_content, paycom_filename)
    df_template = read_uzio_template_df(io.BytesIO(uzio_template_content))
    if df_template is None:
        return {
            "error": "Could not read 'Employee Details' sheet from Uzio template.",
            "job_seeds": {}, "loc_seeds": {},
            "unique_jobs": [], "unique_locs": [],
        }
    job_seeds, loc_seeds = extract_mappings_from_uzio(df_source, df_template, resolved_field_map)

    src_job_col = resolved_field_map.get("Job Title")
    src_loc_col = resolved_field_map.get("Work Location")
    unique_jobs = sorted({str(j).strip() for j in df_source[src_job_col].dropna().unique()}) if src_job_col and src_job_col in df_source.columns else []
    unique_locs = sorted({str(l).strip() for l in df_source[src_loc_col].dropna().unique()}) if src_loc_col and src_loc_col in df_source.columns else []
    return {
        "job_seeds": job_seeds,
        "loc_seeds": loc_seeds,
        "unique_jobs": unique_jobs,
        "unique_locs": unique_locs,
        "field_map": resolved_field_map,
    }


def run_paycom_selective_census_sync(
    paycom_content,
    paycom_filename,
    uzio_template_content,
    selected_uzio_cols,
    job_title_mapping=None,
    work_location_mapping=None,
    fix_options=None,
):
    df_source, resolved_field_map = _read_paycom_source(paycom_content, paycom_filename)
    df_template = read_uzio_template_df(io.BytesIO(uzio_template_content))
    if df_template is None:
        raise ValueError("Could not read 'Employee Details' sheet from Uzio template.")

    df_uzio, summary_text, changes_df = selective_update_uzio(
        df_source, df_template, selected_uzio_cols, resolved_field_map,
        fix_options=fix_options or {},
    )

    src_job_col = resolved_field_map.get("Job Title")
    src_loc_col = resolved_field_map.get("Work Location")

    def _apply(map_dict, src_col, target_col):
        if map_dict is None or not src_col or src_col not in df_source.columns:
            return
        seeded = map_dict
        if not seeded:
            j_seed, l_seed = extract_mappings_from_uzio(df_source, df_template, resolved_field_map)
            seeded = j_seed if target_col == "Job Title" else l_seed
        if not seeded:
            return
        template_id_col = "Employee ID*" if "Employee ID*" in df_uzio.columns else "Employee ID"
        src_id_col = resolved_field_map.get("Employee ID")
        if not src_id_col or src_id_col not in df_source.columns:
            return
        src_lookup = dict(zip(
            df_source[src_id_col].apply(normalize_id),
            df_source[src_col].astype(str).str.strip(),
        ))
        for idx, row in df_uzio.iterrows():
            eid = normalize_id(row.get(template_id_col, ""))
            if not eid:
                continue
            src_v = src_lookup.get(eid)
            if src_v and src_v in seeded:
                df_uzio.at[idx, target_col] = seeded[src_v]

    _apply(job_title_mapping, src_job_col, "Job Title")
    _apply(work_location_mapping, src_loc_col, "Work Location")

    wb = inject_into_uzio_template(df_uzio, io.BytesIO(uzio_template_content))
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    summary = {
        "summary_text": summary_text,
        "rows_changed": int(len(changes_df)) if not changes_df.empty else 0,
        "preview_changes": changes_df.head(50).to_dict("records") if not changes_df.empty else [],
        "selected_columns": list(selected_uzio_cols),
        "fix_options": fix_options or {},
        "job_title_overrides": len(job_title_mapping) if isinstance(job_title_mapping, dict) else None,
        "work_location_overrides": len(work_location_mapping) if isinstance(work_location_mapping, dict) else None,
    }
    return out.getvalue(), summary
