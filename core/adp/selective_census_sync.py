"""ADP Selective Census Sync (MCP core).

Pure-Python port of the Streamlit `apps/adp/census_generator.py`'s
`render_selective_census_generator` entry point. Updates only the requested
columns in a pre-filled Uzio Census Template (.xlsm) using values from a
fresh ADP census export -- everything else in the template is left untouched.

Caller flow:
  1. Run `discover_mappings(adp_content, adp_filename, uzio_template_content)`
     to inspect the template and seed the Job Title / Work Location mapping
     suggestions. (Optional -- only useful if the caller wants to review or
     tweak the mappings before applying.)
  2. Run `run_adp_selective_census_sync(...)` with the source file, the
     template, the list of Uzio columns to sync, optional Job Title /
     Work Location overrides, and any auto-fix toggles.

Returns the modified template as .xlsm bytes plus a summary dict.
"""

import io
import pandas as pd
import openpyxl

from utils.audit_utils import (
    norm_colname, normalize_id, UZIO_RAW_MAPPING,
    read_uzio_template_df, extract_mappings_from_uzio,
    selective_update_uzio, inject_into_uzio_template,
)
from core.adp.census_audit import ADP_FIELD_MAP


def _read_adp_source(content, filename):
    """Read an ADP census file and return (df, resolved_field_map).

    Mirrors Streamlit `preprocess_adp_file`: column headers are normalized via
    norm_colname, then ADP_FIELD_MAP is resolved against the normalized headers.
    """
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
    for std_name, vendor_cols in ADP_FIELD_MAP.items():
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


def discover_mappings(adp_content, adp_filename, uzio_template_content):
    """Return the seed Job Title / Work Location mappings discovered by walking
    the existing Uzio template, plus the unique source values needing review.
    """
    df_source, resolved_field_map = _read_adp_source(adp_content, adp_filename)
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


def run_adp_selective_census_sync(
    adp_content,
    adp_filename,
    uzio_template_content,
    selected_uzio_cols,
    job_title_mapping=None,
    work_location_mapping=None,
    fix_options=None,
):
    """End-to-end pipeline. Returns (xlsm_bytes, summary_dict).

    selected_uzio_cols: list of Uzio template column headers to overwrite
                       (must be keys of UZIO_RAW_MAPPING).
    job_title_mapping / work_location_mapping: optional dicts mapping source
                       value -> Uzio value. Pass None to skip syncing those
                       columns; pass {} to seed entirely from the existing
                       Uzio template (via extract_mappings_from_uzio).
    fix_options: optional dict of toggles forwarded to selective_update_uzio.
    """
    df_source, resolved_field_map = _read_adp_source(adp_content, adp_filename)
    df_template = read_uzio_template_df(io.BytesIO(uzio_template_content))
    if df_template is None:
        raise ValueError("Could not read 'Employee Details' sheet from Uzio template.")

    df_uzio, summary_text, changes_df = selective_update_uzio(
        df_source, df_template, selected_uzio_cols, resolved_field_map,
        fix_options=fix_options or {},
    )

    # Apply Job Title / Work Location overrides on the working DataFrame
    src_job_col = resolved_field_map.get("Job Title")
    src_loc_col = resolved_field_map.get("Work Location")

    if job_title_mapping is not None and src_job_col and src_job_col in df_source.columns:
        # Re-extract row-by-row using df_source ordering (the template df_uzio is keyed differently)
        seeded = {}
        if not job_title_mapping:
            j_seed, _ = extract_mappings_from_uzio(df_source, df_template, resolved_field_map)
            seeded = j_seed
        else:
            seeded = job_title_mapping
        if seeded:
            # Map source job titles to Uzio job titles
            template_id_col = "Employee ID*" if "Employee ID*" in df_uzio.columns else "Employee ID"
            src_id_col = resolved_field_map.get("Employee ID")
            if src_id_col and src_id_col in df_source.columns:
                src_job_lookup = dict(zip(
                    df_source[src_id_col].apply(normalize_id),
                    df_source[src_job_col].astype(str).str.strip(),
                ))
                for idx, row in df_uzio.iterrows():
                    eid = normalize_id(row.get(template_id_col, ""))
                    if not eid:
                        continue
                    src_job = src_job_lookup.get(eid)
                    if src_job and src_job in seeded:
                        df_uzio.at[idx, "Job Title"] = seeded[src_job]

    if work_location_mapping is not None and src_loc_col and src_loc_col in df_source.columns:
        seeded = {}
        if not work_location_mapping:
            _, l_seed = extract_mappings_from_uzio(df_source, df_template, resolved_field_map)
            seeded = l_seed
        else:
            seeded = work_location_mapping
        if seeded:
            template_id_col = "Employee ID*" if "Employee ID*" in df_uzio.columns else "Employee ID"
            src_id_col = resolved_field_map.get("Employee ID")
            if src_id_col and src_id_col in df_source.columns:
                src_loc_lookup = dict(zip(
                    df_source[src_id_col].apply(normalize_id),
                    df_source[src_loc_col].astype(str).str.strip(),
                ))
                for idx, row in df_uzio.iterrows():
                    eid = normalize_id(row.get(template_id_col, ""))
                    if not eid:
                        continue
                    src_loc = src_loc_lookup.get(eid)
                    if src_loc and src_loc in seeded:
                        df_uzio.at[idx, "Work Location"] = seeded[src_loc]

    # Inject the updated DataFrame back into the .xlsm template
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
