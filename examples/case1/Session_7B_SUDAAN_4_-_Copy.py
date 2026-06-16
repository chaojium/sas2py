# *********************************************************************************
# * PROGRAM: 		SESSION 1 - USING SUDAAN				 						*
# * WRITTEN BY:	KATHY WATSON					 								*					*
# * DATE:			12/12/2022 														*
# * INPUT:		NHIS_CLASS.SAS7BDAT      										*
# * VARIABLES:	NSTRATUM, NPSU, WTFA_SA, SEX, AGE, NCHSAGE, _RACEGR, COPD		*
# * MISSING:		SYSTEM MISSING (.)												*
# * OUTPUT: 		RESULTS12122022.XLS												*
# * DESCRIPTION:															 		*
# * THIS PROGRAM READS IN NHIS SAS DATAFILE TO CONDUCT ANALYSES IN SUDAAN  		*
# * MOST COMMON PROCEDURES (IMO) ARE: CROSSTAB, DESCRIPT, RLOGIST					*
# *********************************************************************************;

from __future__ import annotations

import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from scipy.linalg import pinv
from scipy.stats import chi2, f, norm, t

try:
    import statsmodels.api as sm
except ImportError as exc:
    raise ImportError("statsmodels is required for the translated PROC RLOGIST analyses.") from exc


# LIBNAME NHIS "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS";*ASSIGN LIBRARY
# LOCATION FOR SAS DATA;

# OPTIONS NOFMTERR; *INCLUDING THIS SO IF THERE ARE ANY FORMATS THAT CAN'T BE LOADED, DATA WILL STILL RUN;
# OPTIONS PAGESIZE=100 NOCENTER ;

# *SET-UP MACRO FOR EXCEL FILE OUTPUT TABLES-- NOTE: &SYSDATE WILL ASSIGN TODAY'S DATE;

# %MACRO DSOUT(VAR1=,VAR2=);*VAR1 IS SAS TABLE NAME, VAR2 IS EXCEL WORKSHEET NAME;
# PROC EXPORT DATA=&VAR1
#      OUTFILE= "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS\TABLES&SYSDATE.XLS"
#             DBMS=EXCEL REPLACE;
#      SHEET=&VAR2;
# RUN;
# %MEND;

INPUT_DIR = Path(os.environ.get("SAS2PY_INPUT_DIR", "."))
NHIS_CLASS_PATH = INPUT_DIR / "nhis_class.sas7bdat"
OUTPUT_XLSX = Path(f"TABLES{datetime.now().strftime('%d%b%y').upper()}.xlsx")


def canonicalize_name(name: object) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^0-9a-zA-Z_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    text = text.lstrip("_")
    if not text:
        text = "var"
    if text[0].isdigit():
        text = "x" + text
    return text


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_names: list[str] = []
    used: dict[str, int] = {}
    for col in df.columns:
        base = canonicalize_name(col)
        if base not in used:
            used[base] = 0
            new_names.append(base)
        else:
            used[base] += 1
            new_names.append(f"{base}_{used[base]}")
    out = df.copy()
    out.columns = new_names
    return out


def read_sas_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Input dataset not found: {path}. Set SAS2PY_INPUT_DIR or place nhis_class.sas7bdat in the current directory."
        )
    try:
        import pyreadstat

        try:
            df, _meta = pyreadstat.read_sas7bdat(str(path))
        except TypeError:
            df, _meta = pyreadstat.read_sas7bdat(path)
    except Exception:
        df = pd.read_sas(path, format="sas7bdat", encoding="utf-8")
    return canonicalize_columns(df)


def sas_round_numeric(x: pd.Series | np.ndarray | float, unit: float) -> pd.Series | np.ndarray | float:
    return np.round(np.asarray(x, dtype=float) / unit) * unit


def format_sas_number(value: object, width: int | None = None, decimals: int | None = None) -> str:
    if pd.isna(value):
        return ""
    value_float = float(value)
    if decimals is None:
        text = f"{value_float:g}"
    else:
        text = f"{value_float:.{decimals}f}"
    if width is not None:
        return text.rjust(width)
    return text


# *ADD FORMATS IF NEEDED;
# PROC FORMAT;
# VALUE NCHSAGEF
# -2 = ' '
# 1='18-24'
# 2='25-34'
# 3='35-44'
# 4='45-64'
# 5='65+';
# VALUE _RACEGRF
# -2 = ' '
# 1='WHITE'
# 2='BLACK'
# 3='HISPANIC'
# 4='OTHER';
# VALUE SEXF
# -2 = ' '
# 1=MALE
# 2=FEMALE;
# VALUE AGEF
# -2 = ' '
# 1='18-24'
# 2='25-44'
# 3='45-54'
# 4='55-64'
# 5='65-74'
# 6='75+'
# ;
# VALUE COPDF
# -2 = ' '
# 1 = 'NO COPD'
# 2 = 'HAS COPD'
# RUN;

format_maps: dict[str, dict[object, str]] = {
    "nchsagef": {-2: " ", 1: "18-24", 2: "25-34", 3: "35-44", 4: "45-64", 5: "65+"},
    "racegrf": {-2: " ", 1: "WHITE", 2: "BLACK", 3: "HISPANIC", 4: "OTHER"},
    "sexf": {-2: " ", 1: "MALE", 2: "FEMALE"},
    "agef": {-2: " ", 1: "18-24", 2: "25-44", 3: "45-54", 4: "55-64", 5: "65-74", 6: "75+"},
    "copdf": {-2: " ", 1: "NO COPD", 2: "HAS COPD"},
}

variable_formats: dict[str, str] = {
    "nchsage": "nchsagef",
    "racegr": "racegrf",
    "sex": "sexf",
    "age": "agef",
    "copd": "copdf",
}

format_levels: dict[str, list[object]] = {
    "nchsage": [-2, 1, 2, 3, 4, 5],
    "racegr": [-2, 1, 2, 3, 4],
    "sex": [-2, 1, 2],
    "age": [-2, 1, 2, 3, 4, 5, 6],
    "copd": [-2, 1, 2],
}

analysis_levels: dict[str, list[object]] = {
    "sex": [1, 2],
    "age": [1, 2, 3, 4, 5, 6],
    "racegr": [1, 2, 3, 4],
    "copd": [1, 2],
}


def _format_key(value: object) -> object:
    if pd.isna(value):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        try:
            numeric = float(stripped)
            if numeric.is_integer():
                return int(numeric)
            return numeric
        except ValueError:
            return stripped
    try:
        numeric = float(value)
        if numeric.is_integer():
            return int(numeric)
        return numeric
    except Exception:
        return value


def format_label(variable_name: str, value: object) -> str:
    variable_name = canonicalize_name(variable_name)
    fmt_name = variable_formats.get(variable_name)
    if fmt_name is None:
        return "" if pd.isna(value) else str(value)
    key = _format_key(value)
    mapping = format_maps.get(fmt_name, {})
    if key in mapping:
        return mapping[key]
    if pd.isna(value):
        return ""
    return str(value)


def ordered_levels(df: pd.DataFrame, variable: str, include_formatted_missing: bool = True) -> list[object]:
    variable = canonicalize_name(variable)
    levels = list(format_levels.get(variable, []))
    if not include_formatted_missing:
        levels = [level for level in levels if level != -2]
    observed = []
    if variable in df.columns:
        observed = [x for x in df[variable].dropna().unique().tolist() if x not in levels]
    return levels + sorted(observed, key=lambda x: str(x))


def safe_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\[\]:*?/\\]", "_", str(name))
    return cleaned[:31] if len(cleaned) > 31 else cleaned


def finite_positive_mask(df: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for col in columns:
        mask &= df[col].notna() & np.isfinite(pd.to_numeric(df[col], errors="coerce"))
    return mask


def design_degrees_of_freedom(
    df: pd.DataFrame,
    strata_col: str = "nstratum",
    psu_col: str = "npsu",
    weight_col: str = "wtfa_sa",
) -> int:
    work = df[[strata_col, psu_col, weight_col]].dropna()
    work = work[work[weight_col] > 0]
    if work.empty:
        return 0
    psus = work[[strata_col, psu_col]].drop_duplicates()
    return int(len(psus) - psus[strata_col].nunique())


def t_critical_975(df_design: int) -> float:
    if df_design and df_design > 0:
        return float(t.ppf(0.975, df_design))
    return float(norm.ppf(0.975))


def _psu_linearized_variance(
    base_df: pd.DataFrame,
    lin_col: str,
    strata_col: str = "nstratum",
    psu_col: str = "npsu",
) -> float:
    psu_lin = (
        base_df[[strata_col, psu_col, lin_col]]
        .dropna(subset=[strata_col, psu_col])
        .groupby([strata_col, psu_col], dropna=False, observed=False)[lin_col]
        .sum()
        .reset_index(name="_psu_lin")
    )
    var_total = 0.0
    for _stratum, group in psu_lin.groupby(strata_col, dropna=False, observed=False):
        nh = len(group)
        if nh >= 2:
            centered = group["_psu_lin"] - group["_psu_lin"].mean()
            var_total += (nh / (nh - 1.0)) * float(np.dot(centered, centered))
    return var_total


def survey_ratio_binary(
    df: pd.DataFrame,
    domain_mask: pd.Series,
    event_mask: pd.Series,
    strata_col: str = "nstratum",
    psu_col: str = "npsu",
    weight_col: str = "wtfa_sa",
) -> dict[str, float]:
    design_cols = [strata_col, psu_col, weight_col]
    work = df[design_cols].copy()
    work["_domain"] = domain_mask.reindex(df.index).fillna(False).astype(bool)
    work["_event"] = event_mask.reindex(df.index).fillna(False).astype(bool)
    work = work.dropna(subset=design_cols)
    work = work[work[weight_col] > 0].copy()
    if work.empty:
        return {
            "nsum": 0.0,
            "wsum": 0.0,
            "total_weight": 0.0,
            "percent": np.nan,
            "proportion": np.nan,
            "sepercent": np.nan,
            "seproportion": np.nan,
            "lowpct": np.nan,
            "uppct": np.nan,
            "df": 0.0,
        }

    nsum = float(work["_domain"].sum())
    domain_weight = float(work.loc[work["_domain"], weight_col].sum())
    event_weight = float(work.loc[work["_domain"] & work["_event"], weight_col].sum())
    if domain_weight <= 0:
        proportion = np.nan
        se_prop = np.nan
    else:
        proportion = event_weight / domain_weight
        work["_lin"] = np.where(
            work["_domain"],
            work[weight_col] * (work["_event"].astype(float) - proportion) / domain_weight,
            0.0,
        )
        var_prop = _psu_linearized_variance(work, "_lin", strata_col, psu_col)
        se_prop = math.sqrt(var_prop) if var_prop >= 0 else np.nan

    df_design = design_degrees_of_freedom(work, strata_col, psu_col, weight_col)
    crit = t_critical_975(df_design)
    low = proportion - crit * se_prop if pd.notna(proportion) and pd.notna(se_prop) else np.nan
    upp = proportion + crit * se_prop if pd.notna(proportion) and pd.notna(se_prop) else np.nan
    if pd.notna(low):
        low = max(0.0, low)
    if pd.notna(upp):
        upp = min(1.0, upp)

    return {
        "nsum": nsum,
        "wsum": event_weight,
        "total_weight": domain_weight,
        "percent": proportion * 100.0 if pd.notna(proportion) else np.nan,
        "proportion": proportion,
        "sepercent": se_prop * 100.0 if pd.notna(se_prop) else np.nan,
        "seproportion": se_prop,
        "lowpct": low * 100.0 if pd.notna(low) else np.nan,
        "uppct": upp * 100.0 if pd.notna(upp) else np.nan,
        "df": float(df_design),
    }


def survey_total(
    df: pd.DataFrame,
    domain_mask: pd.Series,
    strata_col: str = "nstratum",
    psu_col: str = "npsu",
    weight_col: str = "wtfa_sa",
) -> dict[str, float]:
    work = df[[strata_col, psu_col, weight_col]].copy()
    work["_domain"] = domain_mask.reindex(df.index).fillna(False).astype(bool)
    work = work.dropna(subset=[strata_col, psu_col, weight_col])
    work = work[work[weight_col] > 0].copy()
    if work.empty:
        return {"nsum": 0.0, "total": np.nan, "setotal": np.nan, "lowtotal": np.nan, "uptotal": np.nan}
    total = float(work.loc[work["_domain"], weight_col].sum())
    work["_lin"] = np.where(work["_domain"], work[weight_col], 0.0)
    var_total = _psu_linearized_variance(work, "_lin", strata_col, psu_col)
    se_total = math.sqrt(var_total) if var_total >= 0 else np.nan
    df_design = design_degrees_of_freedom(work, strata_col, psu_col, weight_col)
    crit = t_critical_975(df_design)
    low = total - crit * se_total if pd.notna(se_total) else np.nan
    upp = total + crit * se_total if pd.notna(se_total) else np.nan
    return {
        "nsum": float(work["_domain"].sum()),
        "total": total,
        "setotal": se_total,
        "lowtotal": max(0.0, low) if pd.notna(low) else np.nan,
        "uptotal": upp,
    }


def survey_domain_proportions_cov(
    df: pd.DataFrame,
    domains: list[tuple[str, pd.Series]],
    event_mask: pd.Series,
    strata_col: str = "nstratum",
    psu_col: str = "npsu",
    weight_col: str = "wtfa_sa",
) -> tuple[pd.DataFrame, np.ndarray, int]:
    work = df[[strata_col, psu_col, weight_col]].copy()
    work["_event"] = event_mask.reindex(df.index).fillna(False).astype(bool)
    work = work.dropna(subset=[strata_col, psu_col, weight_col])
    work = work[work[weight_col] > 0].copy()
    if work.empty:
        empty = pd.DataFrame({"domain": [name for name, _mask in domains], "percent": np.nan, "sepercent": np.nan})
        return empty, np.full((len(domains), len(domains)), np.nan), 0

    estimates = []
    lin_cols = []
    for idx, (name, domain) in enumerate(domains):
        domain_local = domain.reindex(work.index).fillna(False).astype(bool)
        denom = float(work.loc[domain_local, weight_col].sum())
        numerator = float(work.loc[domain_local & work["_event"], weight_col].sum())
        prop = numerator / denom if denom > 0 else np.nan
        lin_col = f"_lin_{idx}"
        lin_cols.append(lin_col)
        if pd.notna(prop) and denom > 0:
            work[lin_col] = np.where(
                domain_local,
                work[weight_col] * (work["_event"].astype(float) - prop) / denom,
                0.0,
            )
        else:
            work[lin_col] = 0.0
        estimates.append({"domain": name, "proportion": prop, "percent": prop * 100.0 if pd.notna(prop) else np.nan})

    psu_lin = (
        work[[strata_col, psu_col] + lin_cols]
        .groupby([strata_col, psu_col], dropna=False, observed=False)[lin_cols]
        .sum()
        .reset_index()
    )
    cov = np.zeros((len(lin_cols), len(lin_cols)), dtype=float)
    for _stratum, group in psu_lin.groupby(strata_col, dropna=False, observed=False):
        nh = len(group)
        if nh >= 2:
            mat = group[lin_cols].to_numpy(dtype=float)
            centered = mat - mat.mean(axis=0, keepdims=True)
            cov += (nh / (nh - 1.0)) * centered.T @ centered

    result = pd.DataFrame(estimates)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    result["seproportion"] = se
    result["sepercent"] = se * 100.0
    df_design = design_degrees_of_freedom(work, strata_col, psu_col, weight_col)
    crit = t_critical_975(df_design)
    result["lowpct"] = np.maximum(0.0, (result["proportion"] - crit * result["seproportion"]) * 100.0)
    result["uppct"] = np.minimum(100.0, (result["proportion"] + crit * result["seproportion"]) * 100.0)
    return result, cov * 10000.0, df_design


def contrast_from_cov(estimates_percent: np.ndarray, cov_percent: np.ndarray, coeffs: np.ndarray, df_design: int) -> dict[str, float]:
    coeffs = np.asarray(coeffs, dtype=float)
    estimate = float(coeffs @ estimates_percent)
    variance = float(coeffs @ cov_percent @ coeffs.T)
    se = math.sqrt(max(variance, 0.0)) if np.isfinite(variance) else np.nan
    if pd.notna(se) and se > 0:
        statistic = estimate / se
        p_value = 2.0 * (1.0 - t.cdf(abs(statistic), df_design)) if df_design > 0 else 2.0 * (1.0 - norm.cdf(abs(statistic)))
    else:
        p_value = np.nan
    return {"percent": estimate, "sepercent": se, "p_pct": p_value}


def crosstab_row_percent(
    df: pd.DataFrame,
    row_var: str,
    col_var: str = "copd",
    row_levels: list[object] | None = None,
    col_levels: list[object] | None = None,
) -> pd.DataFrame:
    row_var = canonicalize_name(row_var)
    col_var = canonicalize_name(col_var)
    if row_levels is None:
        row_levels = ordered_levels(df, row_var, include_formatted_missing=True)
    if col_levels is None:
        col_levels = ordered_levels(df, col_var, include_formatted_missing=True)

    rows = []
    valid_col = df[col_var].notna()
    for row_level in row_levels:
        row_mask = (df[row_var] == row_level) & valid_col
        row_total_n = float(row_mask.sum())
        row_total_w = float(df.loc[row_mask & df["wtfa_sa"].notna(), "wtfa_sa"].sum())
        total_row = {
            "table": f"{row_var}*{col_var}",
            "row_variable": row_var,
            "row_code": row_level,
            "row_label": format_label(row_var, row_level),
            "copd_code": 0,
            "copd_label": "Total",
            "nsum": row_total_n,
            "wsum": row_total_w,
            "rowper": 100.0 if row_total_w > 0 else np.nan,
            "serow": 0.0 if row_total_w > 0 else np.nan,
            "lowrow": 100.0 if row_total_w > 0 else np.nan,
            "uprow": 100.0 if row_total_w > 0 else np.nan,
        }
        rows.append(total_row)
        for col_level in col_levels:
            domain_mask = row_mask
            event_mask = df[col_var] == col_level
            stat = survey_ratio_binary(df, domain_mask, event_mask)
            rows.append(
                {
                    "table": f"{row_var}*{col_var}",
                    "row_variable": row_var,
                    "row_code": row_level,
                    "row_label": format_label(row_var, row_level),
                    "copd_code": col_level,
                    "copd_label": format_label(col_var, col_level),
                    "nsum": stat["nsum"],
                    "wsum": stat["wsum"],
                    "rowper": stat["percent"],
                    "serow": stat["sepercent"],
                    "lowrow": stat["lowpct"],
                    "uprow": stat["uppct"],
                }
            )
    return pd.DataFrame(rows)


def crosstab_chisq_test(
    df: pd.DataFrame,
    row_var: str,
    col_var: str = "copd",
    row_levels: list[object] | None = None,
    event_level: object = 2,
) -> dict[str, float | str]:
    row_var = canonicalize_name(row_var)
    col_var = canonicalize_name(col_var)
    if row_levels is None:
        row_levels = analysis_levels.get(row_var, ordered_levels(df, row_var, include_formatted_missing=False))
    domains = [(str(level), (df[row_var] == level) & df[col_var].isin(analysis_levels.get(col_var, [1, 2]))) for level in row_levels]
    estimates, cov, df_design = survey_domain_proportions_cov(df, domains, df[col_var] == event_level)
    props = estimates["percent"].to_numpy(dtype=float)
    valid = np.isfinite(props)
    if valid.sum() < 2:
        return {"stestval": np.nan, "sdf": max(len(row_levels) - 1, 0), "spval": np.nan, "note": "Not estimable."}

    props_valid = props[valid]
    cov_valid = cov[np.ix_(valid, valid)]
    q = len(props_valid) - 1
    contrast = np.zeros((q, len(props_valid)))
    for i in range(q):
        contrast[i, i + 1] = 1.0
        contrast[i, 0] = -1.0
    diff = contrast @ props_valid
    vc = contrast @ cov_valid @ contrast.T
    vc_inv = pinv(vc)
    wald_chi = float(diff.T @ vc_inv @ diff)
    f_stat = wald_chi / q if q > 0 else np.nan
    p_value = 1.0 - f.cdf(f_stat, q, df_design) if q > 0 and df_design > 0 and pd.notna(f_stat) else np.nan
    return {
        "stestval": f_stat,
        "sdf": float(q),
        "spval": p_value,
        "note": "SUDAAN-like design-adjusted Wald F test from row-level event-percent covariance.",
    }


def descript_catlevel(df: pd.DataFrame, var: str, catlevel: object) -> pd.DataFrame:
    var = canonicalize_name(var)
    rows = []
    event_mask = df[var] == catlevel

    overall_stat = survey_ratio_binary(df, df[var].notna(), event_mask)
    rows.append(
        {
            "var_name": "Overall",
            "subgroup_variable": "overall",
            "subgroup_code": "",
            "subgroup": "Overall",
            "catlevel": catlevel,
            "catlevel_label": format_label(var, catlevel),
            "nsum": overall_stat["nsum"],
            "percent": overall_stat["percent"],
            "sepercent": overall_stat["sepercent"],
            "lowpct": overall_stat["lowpct"],
            "uppct": overall_stat["uppct"],
        }
    )

    for group_var in ["sex", "age", "racegr"]:
        for level in analysis_levels[group_var]:
            domain = (df[group_var] == level) & df[var].notna()
            stat = survey_ratio_binary(df, domain, event_mask)
            label = format_label(group_var, level)
            rows.append(
                {
                    "var_name": label,
                    "subgroup_variable": group_var,
                    "subgroup_code": level,
                    "subgroup": label,
                    "catlevel": catlevel,
                    "catlevel_label": format_label(var, catlevel),
                    "nsum": stat["nsum"],
                    "percent": stat["percent"],
                    "sepercent": stat["sepercent"],
                    "lowpct": stat["lowpct"],
                    "uppct": stat["uppct"],
                }
            )

    return pd.DataFrame(rows)


def build_pairwise_poly_contrasts(df: pd.DataFrame) -> pd.DataFrame:
    # PROC DESCRIPT DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR COPD ;
    # VAR COPD;
    # CATLEVEL 1 ;
    # PAIRWISE SEX /NAME= "SEX" ;
    # PAIRWISE AGE /NAME="AGE GROUP";
    # PAIRWISE  _RACEGR /NAME="RACE/ETTHNICITY" ;
    # POLY AGE = 2 /NAME="AGE TREND";
    # CONTRAST _RACEGR =(1 0 -1 0)/ NAME="COMPARING WHITE AND HISPANIC";
    # CONTRAST SEX = (1 -1) * _RACEGR = (1 0 -1 0) /NAME="COMPARE WHITE-HISPANIC SEX DIFFERENCES";
    # CONTRAST SEX = (1 0) * _RACEGR = (1 0 -1 0) /NAME="COMPARE WHITE-HISPANIC - MALES ONLY";
    # CONTRAST SEX = (0 1) * _RACEGR = (1 0 -1 0) /NAME="COMPARE WHITE-HISPANIC - FEMALES ONLY";
    # PRINT;
    # OUTPUT PERCENT SEPERCENT P_PCT/ PERCENTFMT=F5.2 SEPERCENTFMT=F5.2 P_PCTFMT=F9.4
    # FILETYPE=SAS FILENAME=EX7_2C REPLACE;
    # RUN;

    rows: list[dict[str, object]] = []
    event = df["copd"] == 1

    contrast_names = [
        "M-F",
        "A1-A2",
        "A1-A3",
        "A1-A4",
        "A1-A5",
        "A1-A6",
        "A2-A3",
        "A2-A4",
        "A2-A5",
        "A2-A6",
        "A3-A4",
        "A3-A5",
        "A3-A6",
        "A4-A5",
        "A4-A6",
        "A5-A6",
        "R1-R2",
        "R1-R3",
        "R1-R4",
        "R2-R3",
        "R2-R4",
        "R3-R4",
        "AGE-LINEAR",
        "AGE-QUAD",
        "WHITE-HISP-CONTRAST",
        "DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES",
        "WH-HIS-MALE",
        "WH-HISP-FEMALE",
    ]

    sex_domains = [(str(level), (df["sex"] == level) & df["copd"].notna()) for level in analysis_levels["sex"]]
    sex_est, sex_cov, sex_df = survey_domain_proportions_cov(df, sex_domains, event)
    sex_pct = sex_est["percent"].to_numpy(dtype=float)
    rows.append({"contrast": 1, "contrast_name": "M-F", **contrast_from_cov(sex_pct, sex_cov, np.array([1, -1]), sex_df)})

    age_domains = [(str(level), (df["age"] == level) & df["copd"].notna()) for level in analysis_levels["age"]]
    age_est, age_cov, age_df = survey_domain_proportions_cov(df, age_domains, event)
    age_pct = age_est["percent"].to_numpy(dtype=float)
    contrast_index = 2
    for i in range(6):
        for j in range(i + 1, 6):
            coeff = np.zeros(6)
            coeff[i] = 1.0
            coeff[j] = -1.0
            rows.append(
                {
                    "contrast": contrast_index,
                    "contrast_name": contrast_names[contrast_index - 1],
                    **contrast_from_cov(age_pct, age_cov, coeff, age_df),
                }
            )
            contrast_index += 1

    race_domains = [(str(level), (df["racegr"] == level) & df["copd"].notna()) for level in analysis_levels["racegr"]]
    race_est, race_cov, race_df = survey_domain_proportions_cov(df, race_domains, event)
    race_pct = race_est["percent"].to_numpy(dtype=float)
    for i in range(4):
        for j in range(i + 1, 4):
            coeff = np.zeros(4)
            coeff[i] = 1.0
            coeff[j] = -1.0
            rows.append(
                {
                    "contrast": contrast_index,
                    "contrast_name": contrast_names[contrast_index - 1],
                    **contrast_from_cov(race_pct, race_cov, coeff, race_df),
                }
            )
            contrast_index += 1

    # PROC DESCRIPT POLY AGE=2 in Python: output both linear and quadratic rows.
    # On SUDAAN's displayed PERCENT scale for six AGE levels scored 1:6, use linear
    # coefficients [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5] and quadratic coefficients
    # [10/3, -2/3, -8/3, -8/3, -2/3, 10/3].
    age_linear = np.array([-2.5, -1.5, -0.5, 0.5, 1.5, 2.5])
    age_quad = np.array([10.0 / 3.0, -2.0 / 3.0, -8.0 / 3.0, -8.0 / 3.0, -2.0 / 3.0, 10.0 / 3.0])
    rows.append({"contrast": 23, "contrast_name": "AGE-LINEAR", **contrast_from_cov(age_pct, age_cov, age_linear, age_df)})
    rows.append({"contrast": 24, "contrast_name": "AGE-QUAD", **contrast_from_cov(age_pct, age_cov, age_quad, age_df)})

    rows.append(
        {
            "contrast": 25,
            "contrast_name": "WHITE-HISP-CONTRAST",
            **contrast_from_cov(race_pct, race_cov, np.array([1, 0, -1, 0]), race_df),
        }
    )

    sex_race_domains = []
    for sex_level in analysis_levels["sex"]:
        for race_level in analysis_levels["racegr"]:
            mask = (df["sex"] == sex_level) & (df["racegr"] == race_level) & df["copd"].notna()
            sex_race_domains.append((f"{sex_level}:{race_level}", mask))
    sr_est, sr_cov, sr_df = survey_domain_proportions_cov(df, sex_race_domains, event)
    sr_pct = sr_est["percent"].to_numpy(dtype=float)

    # Domain order is male-white, male-black, male-hispanic, male-other,
    # female-white, female-black, female-hispanic, female-other.
    coeff_diff_in_diff = np.array([1, 0, -1, 0, -1, 0, 1, 0], dtype=float)
    coeff_males = np.array([1, 0, -1, 0, 0, 0, 0, 0], dtype=float)
    coeff_females = np.array([0, 0, 0, 0, 1, 0, -1, 0], dtype=float)

    rows.append(
        {
            "contrast": 26,
            "contrast_name": "DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES",
            **contrast_from_cov(sr_pct, sr_cov, coeff_diff_in_diff, sr_df),
        }
    )
    rows.append({"contrast": 27, "contrast_name": "WH-HIS-MALE", **contrast_from_cov(sr_pct, sr_cov, coeff_males, sr_df)})
    rows.append({"contrast": 28, "contrast_name": "WH-HISP-FEMALE", **contrast_from_cov(sr_pct, sr_cov, coeff_females, sr_df)})

    out = pd.DataFrame(rows)
    out = out.sort_values("contrast", kind="mergesort").reset_index(drop=True)
    if len(out) != 28:
        raise RuntimeError("The PROC DESCRIPT PAIRWISE/POLY/CONTRAST translation must produce exactly 28 rows.")
    return out[["contrast", "contrast_name", "percent", "sepercent", "p_pct"]]


def create_design_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], dict[str, list[str]]]:
    model_df = df.copy()
    cols: list[pd.Series] = [pd.Series(1.0, index=model_df.index, name="intercept")]
    col_names: list[str] = ["intercept"]
    term_columns: dict[str, list[str]] = {"intercept": ["intercept"]}

    for var in ["sex", "age", "racegr"]:
        levels = analysis_levels[var]
        ref = 1 if 1 in levels else levels[0]
        term_columns[var] = []
        for level in levels:
            if level == ref:
                continue
            name = f"{var}_{int(level) if float(level).is_integer() else level}"
            cols.append((model_df[var] == level).astype(float).rename(name))
            col_names.append(name)
            term_columns[var].append(name)

    x = pd.concat(cols, axis=1)
    x = x.loc[:, col_names]
    return x, col_names, term_columns


def fit_weighted_logistic_survey(df: pd.DataFrame) -> dict[str, object]:
    # PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR ;
    # REFLEVEL SEX=1 AGE=1 _RACEGR=1;*SET REF LEVEL FOR ORS-NOTE YOU CANNOT CHANGE FOR PREV RATIOS;
    # MODEL COPD_01 = SEX AGE _RACEGR;
    required = ["copd_01", "sex", "age", "racegr", "wtfa_sa", "nstratum", "npsu"]
    model_df = df.dropna(subset=required).copy()
    model_df = model_df[model_df["wtfa_sa"] > 0].copy()
    for var in ["sex", "age", "racegr"]:
        model_df = model_df[model_df[var].isin(analysis_levels[var])]
    model_df = model_df[model_df["copd_01"].isin([0, 1])].copy()

    if model_df.empty:
        raise RuntimeError("No complete cases are available for PROC RLOGIST translation.")

    x, col_names, term_columns = create_design_matrix(model_df)
    y = model_df["copd_01"].astype(float)
    weights = model_df["wtfa_sa"].astype(float)

    glm = sm.GLM(y, x, family=sm.families.Binomial(), freq_weights=weights)
    fit = glm.fit(maxiter=200, disp=0)
    beta = fit.params.reindex(col_names).to_numpy(dtype=float)

    mu = np.asarray(fit.predict(x), dtype=float)
    mu = np.clip(mu, 1e-8, 1.0 - 1e-8)
    x_np = x.to_numpy(dtype=float)
    w_np = weights.to_numpy(dtype=float)

    bread = x_np.T @ ((w_np * mu * (1.0 - mu))[:, None] * x_np)
    bread_inv = pinv(bread)

    score_rows = x_np * ((w_np * (y.to_numpy(dtype=float) - mu))[:, None])
    score_df = model_df[["nstratum", "npsu"]].copy()
    score_cols = [f"_s_{i}" for i in range(score_rows.shape[1])]
    for i, col in enumerate(score_cols):
        score_df[col] = score_rows[:, i]

    psu_scores = (
        score_df.groupby(["nstratum", "npsu"], dropna=False, observed=False)[score_cols]
        .sum()
        .reset_index()
    )
    meat = np.zeros((len(col_names), len(col_names)), dtype=float)
    for _stratum, group in psu_scores.groupby("nstratum", dropna=False, observed=False):
        nh = len(group)
        if nh >= 2:
            mat = group[score_cols].to_numpy(dtype=float)
            centered = mat - mat.mean(axis=0, keepdims=True)
            meat += (nh / (nh - 1.0)) * centered.T @ centered

    cov_beta = bread_inv @ meat @ bread_inv
    cov_beta = (cov_beta + cov_beta.T) / 2.0

    return {
        "fit": fit,
        "model_df": model_df,
        "x": x,
        "col_names": col_names,
        "term_columns": term_columns,
        "beta": beta,
        "cov_beta": cov_beta,
        "df_design": design_degrees_of_freedom(model_df),
    }


def rlogist_or_beta_tables(model: dict[str, object]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    beta_vec = np.asarray(model["beta"], dtype=float)
    cov_beta = np.asarray(model["cov_beta"], dtype=float)
    col_names = list(model["col_names"])
    col_index = {name: i for i, name in enumerate(col_names)}
    df_design = int(model["df_design"])
    crit = t_critical_975(df_design)

    rows = []
    beta_rows = []

    display_rows = [
        ("INTERCEPT", "intercept", "intercept"),
        ("MALE", "sex", None),
        ("FEMALE", "sex", "sex_2"),
        ("18-24 YEARS", "age", None),
        ("25-34 YEARS", "age", "age_2"),
        ("35-44 YEARS", "age", "age_3"),
        ("44-65 YEARS", "age", "age_4"),
        ("65-74 YEARS", "age", "age_5"),
        ("75+ YEARS", "age", "age_6"),
        ("WHITE, NON-", "racegr", None),
        ("BLACK, NON-", "racegr", "racegr_2"),
        ("HISPANIC", "racegr", "racegr_3"),
        ("OTHER, NON-HISP", "racegr", "racegr_4"),
    ]

    for rhs, (label, term, col_name) in enumerate(display_rows, start=1):
        if col_name is None:
            beta_value = 0.0
            se_beta = 0.0
            t_beta = np.nan
            p_beta = np.nan
            odds_ratio = 1.0
            low_or = 1.0
            up_or = 1.0
        elif col_name in col_index:
            idx = col_index[col_name]
            beta_value = float(beta_vec[idx])
            se_beta = math.sqrt(max(float(cov_beta[idx, idx]), 0.0))
            t_beta = beta_value / se_beta if se_beta > 0 else np.nan
            p_beta = 2.0 * (1.0 - t.cdf(abs(t_beta), df_design)) if pd.notna(t_beta) and df_design > 0 else np.nan
            odds_ratio = math.exp(beta_value)
            low_or = math.exp(beta_value - crit * se_beta)
            up_or = math.exp(beta_value + crit * se_beta)
        else:
            beta_value = np.nan
            se_beta = np.nan
            t_beta = np.nan
            p_beta = np.nan
            odds_ratio = np.nan
            low_or = np.nan
            up_or = np.nan

        rows.append({"rhs": rhs, "lbl": label, "or": odds_ratio, "lowor": low_or, "upor": up_or})
        beta_rows.append(
            {
                "rhs": rhs,
                "lbl": label,
                "beta": beta_value,
                "deft": np.nan,
                "p_beta": p_beta,
                "sebeta": se_beta,
                "t_beta": t_beta,
            }
        )

    ors = pd.DataFrame(rows)
    betas = pd.DataFrame(beta_rows)

    orb = ors.merge(betas[["rhs", "p_beta"]], on="rhs", how="left", sort=False)
    orb["or_95"] = (
        orb["or"].map(lambda x: format_sas_number(x, 5, 2))
        + "("
        + orb["lowor"].map(lambda x: format_sas_number(x, 5, 2))
        + ","
        + orb["upor"].map(lambda x: format_sas_number(x, 5, 2))
        + ")"
    )
    orb = orb[["rhs", "lbl", "or_95", "p_beta"]]
    return ors, betas, orb


def wald_test(beta: np.ndarray, cov: np.ndarray, indices: list[int], df_design: int) -> tuple[float, float]:
    if not indices:
        return np.nan, np.nan
    beta_sub = beta[indices]
    cov_sub = cov[np.ix_(indices, indices)]
    if not np.all(np.isfinite(cov_sub)):
        return np.nan, np.nan
    stat = float(beta_sub.T @ pinv(cov_sub) @ beta_sub)
    p_value = 1.0 - chi2.cdf(stat, len(indices))
    return stat, p_value


def rlogist_model_tests(model: dict[str, object]) -> pd.DataFrame:
    beta = np.asarray(model["beta"], dtype=float)
    cov = np.asarray(model["cov_beta"], dtype=float)
    col_names = list(model["col_names"])
    term_columns = dict(model["term_columns"])
    df_design = int(model["df_design"])
    col_index = {name: i for i, name in enumerate(col_names)}

    definitions = [
        ("OVERALL MODEL", col_names),
        ("MODEL MINUS INTERCEPT", [name for name in col_names if name != "intercept"]),
        ("INTERCEPT", ["intercept"]),
        ("SEX", term_columns.get("sex", [])),
        ("AGE", term_columns.get("age", [])),
        ("RACE_ETHNICITY", term_columns.get("racegr", [])),
    ]

    rows = []
    for contrast, (label, cols) in enumerate(definitions, start=1):
        indices = [col_index[col] for col in cols if col in col_index]
        stat, p_value = wald_test(beta, cov, indices, df_design)
        rows.append({"contrast": contrast, "lbl": label, "waldchi": stat, "waldchp": p_value})
    return pd.DataFrame(rows)


def build_unweighted_sample_sizes(df: pd.DataFrame) -> pd.DataFrame:
    # *GET UNWEIGHTED N'S;
    # PROC FREQ DATA=NHIS.NHIS_CLASS;
    # TABLES SEX AGE _RACEGR ;
    # ODS OUTPUT  OneWayFreqs=TABS;
    # WHERE COPD NE .;
    # RUN;
    valid = df[df["copd"].notna()].copy()
    rows = []
    order = 1.0
    for var in ["sex", "age", "racegr"]:
        for level in analysis_levels[var]:
            freq = int((valid[var] == level).sum())
            rows.append({"order": order, "lbl": format_label(var, level), "frequency": freq})
            order += 1.0

    # *CLEAN UP FILE-KEEP ONLY ORDER, LABEL, AND FREQ;
    # DATA TABS_CLEAN (KEEP=ORDER LBL FREQUENCY);
    # RETAIN ORDER LBL FREQUENCY;
    # SET TABS;
    # LENGTH LBL $30;
    # LBL=CATS(F_SEX, F_AGE, F__RACEGR);
    # ORDER=_N_;
    # RUN;

    tabs_clean = pd.DataFrame(rows)

    # *CREATE A FILE TO INSERT BLANK ROW BETWEEN EACH CHARACTERITICS;
    # DATA TOTALS;
    # LENGTH LBL $ 35;
    # INPUT ORDER LBL $ @@;
    # CARDS;
    # 0 SEX  2.5 AGE 8.5 RACE_ETH
    # ;
    # RUN;
    totals = pd.DataFrame(
        {
            "order": [0.0, 2.5, 8.5],
            "lbl": ["SEX", "AGE", "RACE_ETH"],
            "frequency": [np.nan, np.nan, np.nan],
        }
    )

    # *MERGE THE FILE WITH TOTAL TO FREQUENCY FILE;
    # PROC APPEND BASE=TABS_CLEAN DATA=TOTALS FORCE;
    # RUN;
    # *SORT BY ORDER;
    # PROC SORT DATA=TABS_CLEAN;
    # BY ORDER;
    # RUN;
    out = pd.concat([tabs_clean, totals], ignore_index=True).sort_values("order", kind="mergesort").reset_index(drop=True)
    return out[["order", "lbl", "frequency"]]


def build_table1(ex7_1: pd.DataFrame) -> pd.DataFrame:
    # *CLEAN UP THE DATA FILE AND SAVE EACH OUTCOME (TOTAL, NO COPD, YES COPD) AS SEPARATE TEMPORARY
    # FILES;
    rows = []
    group_rows = ex7_1[ex7_1["copd_code"] == 0].copy()
    for _, group in group_rows.iterrows():
        row_name = str(group["row_label"])
        source = ex7_1[
            (ex7_1["row_variable"] == group["row_variable"])
            & (ex7_1["row_code"] == group["row_code"])
        ].copy()
        row = {"row_name": row_name}
        total = source[source["copd_code"] == 0]
        no_copd = source[source["copd_code"] == 1]
        yes_copd = source[source["copd_code"] == 2]
        if not total.empty:
            row["nsum1"] = float(total["nsum"].iloc[0])
        else:
            row["nsum1"] = np.nan
        row["nsum2"] = float(no_copd["nsum"].iloc[0]) if not no_copd.empty else np.nan
        row["per_952"] = (
            f"{float(no_copd['rowper'].iloc[0]):4.1f}({float(no_copd['lowrow'].iloc[0]):4.1f},{float(no_copd['uprow'].iloc[0]):4.1f})"
            if not no_copd.empty and pd.notna(no_copd["rowper"].iloc[0])
            else ""
        )
        row["nsum3"] = float(yes_copd["nsum"].iloc[0]) if not yes_copd.empty else np.nan
        row["per_953"] = (
            f"{float(yes_copd['rowper'].iloc[0]):4.1f}({float(yes_copd['lowrow'].iloc[0]):4.1f},{float(yes_copd['uprow'].iloc[0]):4.1f})"
            if not yes_copd.empty and pd.notna(yes_copd["rowper"].iloc[0])
            else ""
        )
        rows.append(row)
    return pd.DataFrame(rows)


def build_desc_tables(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # *NOW CREATE A MACRO TO ADD EACH OUTCOME AS A NEW COLUMN TO THE TABLE;
    # *EX7-2B-JUST A DIFFERENT APPROACH FROM ABOVE-INSTEAD OF INCLUDING ALL OUTCOMES AT ONCE,
    # CREATED A MACRO DO EACH OUTCOME SEPRATELY.  ADVANTAGE-MAY BE EASIER TO CLEAN DATA;
    desc1 = descript_catlevel(df, "copd", 1)
    desc2 = descript_catlevel(df, "copd", 2)

    clean1 = desc1.rename(
        columns={
            "nsum": "nsum1",
            "percent": "percent1",
            "sepercent": "sepercent1",
            "lowpct": "lowpct1",
            "uppct": "uppct1",
        }
    )
    clean1["per_95_1"] = clean1.apply(
        lambda r: f"{r['percent1']:4.1f}({r['lowpct1']:4.1f},{r['uppct1']:4.1f})" if pd.notna(r["percent1"]) else "",
        axis=1,
    )
    clean2 = desc2.rename(
        columns={
            "nsum": "nsum2",
            "percent": "percent2",
            "sepercent": "sepercent2",
            "lowpct": "lowpct2",
            "uppct": "uppct2",
        }
    )
    clean2["per_95_2"] = clean2.apply(
        lambda r: f"{r['percent2']:4.1f}({r['lowpct2']:4.1f},{r['uppct2']:4.1f})" if pd.notna(r["percent2"]) else "",
        axis=1,
    )

    key_cols = ["subgroup_variable", "subgroup_code", "var_name"]
    table = clean1[key_cols + ["nsum1", "per_95_1"]].merge(
        clean2[key_cols + ["nsum2", "per_95_2"]],
        on=key_cols,
        how="outer",
        sort=False,
    )
    table = table[["var_name", "nsum1", "per_95_1", "nsum2", "per_95_2"]]

    graph = clean1[key_cols + ["percent1", "sepercent1"]].merge(
        clean2[key_cols + ["percent2", "sepercent2"]],
        on=key_cols,
        how="outer",
        sort=False,
    )
    graph = graph[["var_name", "percent1", "sepercent1", "percent2", "sepercent2"]]
    return pd.concat([desc1, desc2], ignore_index=True), table, graph


def main() -> None:
    nhis_class = read_sas_dataset(NHIS_CLASS_PATH)

    required_columns = {"nstratum", "npsu", "wtfa_sa", "sex", "age", "nchsage", "racegr", "copd"}
    missing = sorted(required_columns - set(nhis_class.columns))
    if missing:
        raise KeyError(f"Required variables are missing after canonicalization: {missing}")

    for col in ["nstratum", "npsu", "wtfa_sa", "sex", "age", "nchsage", "racegr", "copd"]:
        nhis_class[col] = pd.to_numeric(nhis_class[col], errors="coerce")

    # PROC CONTENTS DATA=NHIS.NHIS_CLASS;
    # RUN;
    contents = pd.DataFrame(
        {
            "name": nhis_class.columns,
            "dtype": [str(dtype) for dtype in nhis_class.dtypes],
            "nonmissing": [int(nhis_class[col].notna().sum()) for col in nhis_class.columns],
        }
    )

    # *VIEW DATA;
    # PROC FREQ DATA=NHIS.NHIS_CLASS;
    # TABLES SEX AGE NCHSAGE _RACEGR COPD;
    # RUN;
    freq_tables = []
    for var in ["sex", "age", "nchsage", "racegr", "copd"]:
        counts = (
            nhis_class[var]
            .value_counts(dropna=False)
            .rename_axis("code")
            .reset_index(name="frequency")
            .sort_values("code", kind="mergesort", na_position="last")
        )
        counts["variable"] = var
        counts["label"] = counts["code"].map(lambda x, v=var: format_label(v, x))
        freq_tables.append(counts[["variable", "code", "label", "frequency"]])
    frequencies = pd.concat(freq_tables, ignore_index=True)

    # ***********************************************************************
    # *	CROSSTABS
    # **********************************************************************
    # *EX 7-1A: PRINT RESULTS-ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD,AND NUMBER
    #  OF ADULTS WITH COPD, WITH ESTIMATED SE AND 95% CI.*;

    # PROC CROSSTAB DATA=NHIS.NHIS_CLASS DESIGN=WR NOTSORTED NOROW NOCOL ;
    # NEST NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # SUBPOPN COPD=1 OR COPD=2;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
    # TABLES COPD ;
    # CLASS COPD ;
    # TITLE "ESTIMATED PERCENTAGE & NUMBER OF ADULTS WITH COPD, NHIS 2021" ;
    # PRINT   /  WSUMFMT = F11.0    SEWGTFMT = F11.0  TOTPERFMT = F9.5
    #    SETOTFMT = F9.5   LOWTOTFMT = F9.5  UPTOTFMT = F9.5   ;
    # /* NOTE:  CROSSTAB DOES NOT GIVE A CONFIDENCE INTERVAL ON TOTAL NUMBER
    #    OF ADULTS WITH COPD.  YOU NEED TO USE DESCRIPT IN ORDER TO GET
    #    THIS CONFIDENCE INTERVAL.  */
    # RUN ;
    subpop_copd = nhis_class["copd"].isin([1, 2])
    ex7_1a_rows = []
    for level in analysis_levels["copd"]:
        stat = survey_ratio_binary(nhis_class, subpop_copd, nhis_class["copd"] == level)
        ex7_1a_rows.append(
            {
                "copd_code": level,
                "copd_label": format_label("copd", level),
                "wsum": stat["wsum"],
                "totper": stat["percent"],
                "setot": stat["sepercent"],
                "lowtot": stat["lowpct"],
                "uptot": stat["uppct"],
            }
        )
    ex7_1a = pd.DataFrame(ex7_1a_rows)

    # *EX 7-1B: SAVE RESULTS TO SAS DATA FILE
    # ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD BY DEMOGRAPHIC
    # CHARACTERISTICS AND SAVE AS SAS DATATFILE;
    #
    # PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
    # CLASS  SEX AGE _RACEGR COPD ;
    # TABLES  (SEX AGE  _RACEGR)*COPD;
    # PRINT NSUM ROWPER SEROW LOWROW UPROW /  NSUMFMT = F11.0    ROWPERFMT = F9.1
    #    SEROWFMT = F9.1   LOWROWFMT = F9.1  UPROWFMT = F9.1;
    # OUTPUT NSUM ROWPER SEROW LOWROW UPROW   /  NSUMFMT = F11.0    ROWPERFMT = F9.1
    #    SEROWFMT = F9.1   LOWROWFMT = F9.1  UPROWFMT = F9.1 FILETYPE=SAS FILENAME=EX7_1 REPLACE;  ;
    # RUN ;
    ex7_1 = pd.concat(
        [
            crosstab_row_percent(nhis_class, "sex", "copd", analysis_levels["sex"], [1, 2]),
            crosstab_row_percent(nhis_class, "age", "copd", analysis_levels["age"], [1, 2]),
            crosstab_row_percent(nhis_class, "racegr", "copd", analysis_levels["racegr"], [1, 2]),
        ],
        ignore_index=True,
    )

    # *EX 7-1C: SAVE TO SAS DATA FILE
    # TEST TO SEE IF COPD IS ASSOCIATED WITH COPD BY DEMOGRAPHIC
    # CHARACTERISTICS AND SAVE AS SAS DATATFILE;
    # *OPTIONS FOR TESTING:
    # CHISQ TEST FOR INDEPENDENCE IN TWO-WAY TABLES.
    # LLCHISQ TEST FOR INDEPENDENCE (BASED ON A LOGLINEAR
    # MODEL) IN TWO-WAY TABLES--YOU SHOULD NOT REQUEST THIS IF ANY ESTIMATED CELL FREQUENCIES ARE EQUAL TO ZERO.
    # CMH COCHRAN-MANTEL-HAENSZEL TEST OF INDEPENDENCE IN STRATIFIED TWO-WAY TABLES
    # TCMH COCHRAN-MANTEL-HAENSZEL TEST FOR TREND IN TWO-WAY TABLES--ASSUMES BOTH ROW AND COLUMN VARIABLES ARE ORDINAL;
    # *USE S (STESTVAL SDF SPVAL) FOR CHISQ AND LLCHISQ AND A (ATESTVAL ADF APVAL) FOR OTHERS;
    #
    # PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
    # CLASS  SEX AGE _RACEGR COPD ;
    # TABLES  (SEX AGE  _RACEGR)*COPD;
    # TEST CHISQ ;
    # OUTPUT STESTVAL SDF SPVAL /STESTVALFMT=F12.4 SDFFMT=F12.4 SPVALFMT=F12.4 FILETYPE=SAS FILENAME=EX7_1TEST REPLACE;
    # RUN;
    tests = []
    for tableno, (var, label) in enumerate([("sex", "SEX   "), ("age", "AGE   "), ("racegr", "RACE   ")], start=1):
        result = crosstab_chisq_test(nhis_class, var, "copd", analysis_levels[var], event_level=2)
        tests.append(
            {
                "tableno": tableno,
                "var_name": label,
                "stestval": result["stestval"],
                "sdf": result["sdf"],
                "spval": result["spval"],
                "note": result["note"],
            }
        )
    ex7_1test = pd.DataFrame(tests)

    # ***********************************************************************
    # *	DESCRIPT
    # **********************************************************************
    #
    # **EX 7-2A: ESTIMATE PREVALENCE AND SAVE TO SAS DATAFILE INLCUDE ALL OUTCOMES IN ONE COMMAND;
    #
    # PROC DESCRIPT DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR COPD ;
    # VAR COPD COPD;
    # CATLEVEL 1 2 ;
    # PRINT;
    # OUTPUT NSUM PERCENT SEPERCENT LOWPCT UPPCT/ PERCENTFMT=F5.1 SEPERCENTFMT=F5.1 LOWPCTFMT=F5.1
    # UPPCTFMT=F5.1 FILETYPE=SAS FILENAME=EX7_2A REPLACE;
    # RUN;
    ex7_2a, ex7_2b_table, ex7_2b_graph = build_desc_tables(nhis_class)

    # **EX7_2B: SHOWN IN ADVANCED CODE SECTION : SAME AS ABOVE BUT WITH MACRO TO DO EACH OUTCOME SEPRATELY--MAY BE EASIER TO CLEAN UP
    # ;

    # **EX7_2C: PERFORM PAIRWISE,TRENDS, AND SPECIAL CONTRAST TESTS;
    ex7_2c_clean = build_pairwise_poly_contrasts(nhis_class)

    # ***********************************************************************
    # *	RLOGIST
    # **********************************************************************
    #
    # **EX7-3: LOGISTIC REGRESSION;
    # *DEPENDENT VARIABLE IS BINARY (VALUES 0/1).
    # *SUDAAN WILL MODEL THE PROBABILITY THAT THE RESPONSE VALUE=1;
    #
    # *RECODE OUTCOME TO O/1;
    # DATA TEMP;
    # SET NHIS.NHIS_CLASS;
    # IF COPD=1 THEN COPD_01=0;
    # IF COPD=2 THEN COPD_01=1;
    # RUN;
    temp = nhis_class.copy()
    temp["copd_01"] = np.nan
    temp.loc[temp["copd"] == 1, "copd_01"] = 0
    temp.loc[temp["copd"] == 2, "copd_01"] = 1

    # *EX7-3A: RUN MODEL AND PRINT ODDS RATIOS;
    #
    # PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR ;
    # MODEL COPD_01 = SEX AGE _RACEGR;
    # PRINT;
    # RUN;

    # *EX7-3B: RUN MODEL AND SAVE OUTPUT;
    #
    # PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR ;
    # REFLEVEL SEX=1 AGE=1 _RACEGR=1;*SET REF LEVEL FOR ORS-NOTE YOU CANNOT CHANGE FOR PREV RATIOS-SOLUTION
    # FOR PREV RATIOS IS TO CREATE A NEW VARIABLE WHERE REFERENT GROUP YOU WANT IS LAST VALUE;
    # MODEL COPD_01 = SEX AGE _RACEGR;
    # PRINT;
    # OUTPUT OR LOWOR UPOR/ FILETYPE=SAS FILENAME=ORS REPLACE; *SAVE ORS;
    # OUTPUT BETA DEFT P_BETA SEBETA T_BETA/ FILENAME=BETAS REPLACE; *SAVE PARMS, INCLUDING P-VALUE;
    # TEST SATADJF WALDF WALDCHI SATADJCHI ADJWALDF; *SAVE OVERALL MOEL STATISTICS;
    # OUTPUT WALDCHI WALDCHP/ FILENAME=ORTEST REPLACE FILETYPE=SAS;
    # RUN;
    logistic_model = fit_weighted_logistic_survey(temp)
    ors, betas, orbeta = rlogist_or_beta_tables(logistic_model)
    ortest_r = rlogist_model_tests(logistic_model)

    # *EX7-3C: INCLUDE TREND TEST;
    #
    # PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR  ;
    # REFLEVEL SEX=1 AGE=1 _RACEGR=1;*SET REF LEVEL FOR ORS-NOTE YOU CANNOT CHANGE FOR PREV RATIOS;
    # MODEL COPD_01 = SEX AGE _RACEGR AGE*_RACEGR;
    # EFFECTS AGE=(-5 -3 -1 1 3 5) / NAME="AGE LINEAR TREND";
    # EFFECTS AGE=(5 -1 -4 -4 -1 5) / NAME="AGE-QUADRATIC";
    # EFFECTS AGE=(0 1 0 0 0 0)*_RACEGR=(1 -1 0 0)  / EXP NAME="DIFFERENCE BETWEEN WHT-BL FOR 25-34YRS";
    # PRINT;
    # OUTPUT OR LOWOR UPOR/ FILETYPE=SAS FILENAME=ORS_C REPLACE; *SAVE ORS;
    # OUTPUT BETA DEFT P_BETA SEBETA T_BETA/ FILENAME=BETAS_C REPLACE; *SAVE PARMS, INCLUDING P-VALUE;
    # TEST SATADJF WALDF WALDCHI SATADJCHI ADJWALDF; *TEST OVERALL MODEL STATISTICS;
    # OUTPUT WALDCHI WALDCHP/ FILENAME=ORTEST_C REPLACE FILETYPE=SAS;*SAVE MODEL TESTS (TREND TEST CAN
    # BE FOUND HERE);
    # RUN;
    #
    # *clean up tables;
    #
    # *EX7-3D: ADD PREDICTED MARGINALS AND PREVALENCE RATIOS;
    #
    # PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
    # NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
    # WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
    # CLASS  SEX AGE _RACEGR ;
    # REFLEVEL SEX=1 AGE=1 _RACEGR=1;*SET REF LEVEL FOR ORS-NOTE YOU CANNOT CHANGE FOR PREV RATIOS;
    # MODEL COPD_01 = SEX AGE _RACEGR ;
    # PREDMARG SEX AGE _RACEGR  /ADJRR; *get prevalence ratios;
    # EFFECTS AGE=(-5 -3 -1 1 3 5) / NAME="AGE LINEAR TREND";
    # EFFECTS AGE=(5 -1 -4 -4 -1 5) / NAME="AGE-QUADRATIC";
    # PRINT;
    # output PREDMRG sePRDMRG / FILENAME=PM PREDMRGFMT=F8.5 sePRDMRGFMT=F8.5 REPLACE;*SAVE PREDICTED MARGINALS;
    # OUTPUT PRED_RR PRED_SERR PRED_LOWRR PRED_UPRR / FILENAME=PREV_RAT REPLACE;*FMT IS DEFAULT TO 2 DECINMAL PLACES;
    # OUTPUT OR LOWOR UPOR/ FILETYPE=SAS FILENAME=ORS_C REPLACE; *SAVE ORS;
    # OUTPUT BETA DEFT P_BETA SEBETA T_BETA/ FILENAME=BETAS_C REPLACE; *SAVE PARMS, INCLUDING P-VALUE;
    # TEST SATADJF WALDF WALDCHI SATADJCHI ADJWALDF; *TEST OVERALL MODEL STATISTICS;
    # OUTPUT WALDCHI WALDCHP/ FILENAME=ORTEST_C REPLACE FILETYPE=SAS;*SAVE MODEL TESTS (TREND TEST CAN
    # BE FOUND HERE);
    # RUN;

    # **************************************************************************************
    # *************************************************************************************
    # *ADVANCED USERS
    # *CODE TO RUN ANALYSES, CLEAN UP, AND SAVE RESULTS TO EXCEL FILE;
    # **************************************************************************************
    # *************************************************************************************

    tabs_clean = build_unweighted_sample_sizes(nhis_class)

    # %DSOUT(VAR1=TABS_CLEAN, VAR2=SS); *UNWEIGHTED SAMPLE SIZES;

    # PROC SORT DATA=NHIS.NHIS_CLASS;
    # BY NSTRATUM NPSU;
    # RUN;
    nhis_class_sorted = nhis_class.sort_values(["nstratum", "npsu"], kind="mergesort").reset_index(drop=True)

    # *EX 7-1B: ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD BY DEMOGRAPHIC
    # CHARACTERISTICS AND SAVE AS SAS DATATFILE--NOTE YOU CAN ALSO GET SAMPLE SIZE HERE INSTEAD OF PROC FREQ AT BEGINNIING;

    table1 = build_table1(ex7_1)

    # *MERGE FILES TOGETHER;
    #
    # DATA TABLE1 (DROP=PER_951);
    # MERGE TEMP1 TEMP2 TEMP3;
    # RUN;
    #
    # %DSOUT(VAR1=TABLE1, VAR2=TABLE1);

    t1_test = ex7_1test[["var_name", "stestval", "sdf", "spval"]].copy()

    # DATA T1_TEST (KEEP=VAR_NAME STESTVAL SDF SPVAL) ;
    # RETAIN VAR_NAME STESTVAL SDF SPVAL;
    # SET EX7_1TEST;
    # IF TABLENO=1 THEN VAR_NAME='SEX   ';
    # IF TABLENO=2 THEN VAR_NAME='AGE   ';
    # IF TABLENO=3 THEN VAR_NAME='RACE   ';
    # RUN;
    # %DSOUT(VAR1=T1_TEST, VAR2=T1_TEST);

    # **EX 7-2: NOW USE PROC DESCRIPT;
    # **EX 7-2A: INLCUDE ALL OUTCOMES IN ONE COMMAND;
    #
    # DATA EX7_2A_CLEAN (KEEP=VAR_NAME NSUM PERCENT SEPERCENT LOWPCT UPPCT);
    # RETAIN VAR_NAME NSUM PERCENT SEPERCENT LOWPCT UPPCT;
    # SET EX7_2A;
    # VAR_NAME=TRIM(PUT(SEX,SEXF.))||TRIM(PUT(AGE,AGEF.))||TRIM(PUT(_RACEGR,_RACEGRF.));
    # RUN;

    # *NOW CREATE YOUR OUTPUT TABLE;
    # DATA EX7_2B_TABLE (KEEP=VAR_NAME NSUM1 PER_95_1 NSUM2 PER_95_2 ) ;
    # MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
    # RUN;
    #
    # %DSOUT(VAR1=EX_7B_TABLE, VAR2=TABLE2);

    # *CREATE TABLE FOR GRAPHING;
    # DATA EX7_2B_GRAPH (KEEP=VAR_NAME PERCENT1 SEPERCENT1 PERCENT2 SEPERCENT2)  ;
    # MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
    # RUN;
    #
    # %DSOUT(VAR1=EX_7B_GRAPH, VAR2=T2_GRAPH);

    # DATA LABELS;
    # CONTRAST = _N_;
    # LENGTH CONTRAST_NAME $ 35;
    # INPUT CONTRAST_NAME $ @@;
    # CARDS;
    # M-F A1-A2 A1-A3 A1-A4 A1-A5 A1-A6 A2-A3 A2-A4 A2-A5
    # A2-A6 A3-A4 A3-A5 A3-A6 A4-A5 A4-A6 A5-A6 R1-R2 R1-R3
    # R1-R4 R2-R3 R2-R4 R3-R4 AGE-LINEAR AGE-QUAD
    # WHITE-HISP-CONTRAST
    # DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES
    # WH-HIS-MALE
    # WH-HISP-FEMALE
    # ;
    #
    # RUN;
    #
    # DATA EX7_2C_CLEAN (KEEP=CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT);
    # RETAIN CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT;
    # MERGE EX7_2C (WHERE=(_ONE_=0)) LABELS;
    # RUN;
    #
    # %DSOUT(VAR1=EX7_2C_CLEAN, VAR2=T1_TESTS);

    # data ORS_r;
    # set ORS;
    # RHS=_n_;
    # run;
    #
    # data betas_r;
    # set betas;
    # RHS=_n_;
    # run;

    # *CLEAN UP OUTPUT FROM MODEL #1-MERGING THE P-VALUE TO THE ODDS RATIOS DATA FILE;
    #
    # DATA ORBETA (KEEP=RHS LBL OR_95 P_BETA);
    # RETAIN RHS LBL OR_95 P_BETA;
    # LENGTH LBL $ 20;
    # MERGE ORS_r BETAS_r (KEEP=RHS P_BETA);
    # BY RHS;
    # FORMAT LBL $CHAR20.;
    # IF RHS=1 THEN LBL="INTERCEPT";
    # IF RHS=2 THEN LBL="MALE";
    # IF RHS=3 THEN LBL="FEMALE";
    # IF RHS=4 THEN LBL="18-24 YEARS";
    # IF RHS=5 THEN LBL="25-34 YEARS";
    # IF RHS=6 THEN LBL="35-44 YEARS";
    # IF RHS=7 THEN LBL="44-65 YEARS";
    # IF RHS=8 THEN LBL="65-74 YEARS";
    # IF RHS=9 THEN LBL="75+ YEARS";
    # IF RHS=10 THEN LBL="WHITE, NON-";
    # IF RHS=11 THEN LBL="BLACK, NON-";
    # IF RHS=12 THEN LBL="HISPANIC";
    # IF RHS=13 THEN LBL="OTHER, NON-HISP";
    # OR_95=PUT(OR ,5.2)||'('||PUT(LOWOR,5.2)||','||PUT(UPOR,5.2)||')';
    # RUN;

    # *PROVIDE MODEL LABELS OR OVERALL MODEL TESTS;
    # DATA ORTEST_R (KEEP=CONTRAST LBL WALDCHI WALDCHP);
    # RETAIN CONTRAST LBL WALDCHI WALDCHP;
    # SET ORTEST;
    # LENGTH LBL $ 35;
    # IF CONTRAST=1 THEN LBL="OVERALL MODEL";
    # IF CONTRAST=2 THEN LBL="MODEL MINUS INTERCEPT";
    # IF CONTRAST=3 THEN LBL="INTERCEPT";
    # IF CONTRAST=4 THEN LBL="SEX";
    # IF CONTRAST=5 THEN LBL="AGE";
    # IF CONTRAST=6 THEN LBL="RACE_ETHNICITY";
    # RUN;
    #
    # %DSOUT(VAR1=ORBETA, VAR2=OR_WITH_PVALUE); *ORS WITH P-VALUE;
    # %DSOUT(VAR1=ORTEST_R, VAR2=OVERALL_MODEL_TESTS); *OVERALL MODEL TESTS-GLOBAL TEST;

    output_tables: dict[str, pd.DataFrame] = {
        "CONTENTS": contents,
        "FREQUENCIES": frequencies,
        "EX7_1A": ex7_1a,
        "EX7_1": ex7_1,
        "EX7_1TEST": ex7_1test,
        "EX7_2A": ex7_2a,
        "ORS_RAW": ors,
        "BETAS_RAW": betas,
        "SS": tabs_clean,
        "TABLE1": table1,
        "T1_TEST": t1_test,
        "TABLE2": ex7_2b_table,
        "T2_GRAPH": ex7_2b_graph,
        "T1_TESTS": ex7_2c_clean,
        "OR_WITH_PVALUE": orbeta,
        "OVERALL_MODEL_TESTS": ortest_r,
    }

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        used_sheet_names: set[str] = set()
        for sheet_name, table in output_tables.items():
            safe_name = safe_sheet_name(sheet_name)
            base_name = safe_name
            counter = 1
            while safe_name in used_sheet_names:
                suffix = f"_{counter}"
                safe_name = f"{base_name[:31 - len(suffix)]}{suffix}"
                counter += 1
            used_sheet_names.add(safe_name)
            table.to_excel(writer, sheet_name=safe_name, index=False)

    # A single Excel workbook is produced because the SAS program uses PROC EXPORT
    # with DBMS=EXCEL and multiple SHEET= outputs to the same workbook.
    print(f"Wrote Excel workbook: {OUTPUT_XLSX}")
    print("Primary exported sheets: SS, TABLE1, T1_TEST, TABLE2, T2_GRAPH, T1_TESTS, OR_WITH_PVALUE, OVERALL_MODEL_TESTS")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise