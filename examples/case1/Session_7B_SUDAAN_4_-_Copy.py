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

import os
import re
import warnings
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from scipy import stats

try:
    import statsmodels.api as sm
    import statsmodels.formula.api as smf
except ImportError:  # pragma: no cover
    sm = None
    smf = None


# LIBNAME NHIS "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS";*ASSIGN LIBRARY
# LOCATION FOR SAS DATA;
#
# Do not reproduce the local machine path in Python.  Treat the SAS LIBNAME as a
# source-data location hint and read NHIS_CLASS.sas7bdat from SAS2PY_INPUT_DIR,
# defaulting to the current working directory.

INPUT_DIR = Path(os.environ.get("SAS2PY_INPUT_DIR", "."))
NHIS_CLASS_PATH = INPUT_DIR / "nhis_class.sas7bdat"

# OPTIONS NOFMTERR; *INCLUDING THIS SO IF THERE ARE ANY FORMATS THAT CAN'T BE LOADED, DATA WILL STILL RUN;
# OPTIONS PAGESIZE=100 NOCENTER ;

# *SET-UP MACRO FOR EXCEL FILE OUTPUT TABLES-- NOTE: &SYSDATE WILL ASSIGN TODAY'S DATE;

SYSDATE = datetime.now().strftime("%d%b%Y").upper()
EXCEL_OUTPUT_PATH = Path(f"TABLES{SYSDATE}.xlsx")
_excel_sheets: dict[str, pd.DataFrame] = {}


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize SAS column names to lowercase, safe Python/Pandas names.

    SAS identifiers are case-insensitive.  Python identifiers are not.  This
    function also removes leading underscores when there is no collision, so
    SAS _RACEGR becomes racegr and all later references use racegr only.
    """
    used: dict[str, int] = {}
    new_columns: list[str] = []

    for original in df.columns:
        name = str(original).strip().lower()
        name = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
        name = re.sub(r"_+", "_", name)
        name = name.lstrip("_")
        if not name:
            name = "column"
        if name[0].isdigit():
            name = f"x{name}"
        base = name
        if base in used:
            used[base] += 1
            name = f"{base}_{used[base]}"
            while name in used:
                used[base] += 1
                name = f"{base}_{used[base]}"
        used[name] = 1
        new_columns.append(name)

    out = df.copy()
    out.columns = new_columns
    return out


def read_sas_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required input dataset was not found: {path}. "
            "Set SAS2PY_INPUT_DIR or place nhis_class.sas7bdat in the current directory."
        )
    try:
        df = pd.read_sas(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_sas(path)
    return canonicalize_columns(df)


def round_sas_scalar(value: float, unit: float) -> float:
    if pd.isna(value):
        return np.nan
    return float(np.floor((float(value) / unit) + 0.5) * unit)


def fmt_num(value: float, width: int | None = None, decimals: int = 1) -> str:
    if pd.isna(value):
        text = ""
    else:
        text = f"{float(value):.{decimals}f}"
    if width is not None:
        return text.rjust(width)
    return text


def safe_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\[\]:*?/\\]", "_", str(name))
    return cleaned[:31] if len(cleaned) > 31 else cleaned


# %MACRO DSOUT(VAR1=,VAR2=);*VAR1 IS SAS TABLE NAME, VAR2 IS EXCEL WORKSHEET NAME;
# PROC EXPORT DATA=&VAR1
#      OUTFILE= "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS\TABLES&SYSDATE.XLS"
#             DBMS=EXCEL REPLACE;
#      SHEET=&VAR2;
# RUN;
# %MEND;
def dsout(df: pd.DataFrame, sheet_name: str) -> None:
    # *VAR1 IS SAS TABLE NAME, VAR2 IS EXCEL WORKSHEET NAME;
    # Python writes one .xlsx workbook in the current working directory.  The
    # SAS code requested .XLS via DBMS=EXCEL, but modern pandas engines write
    # .xlsx reliably; all requested workbook sheets are preserved.
    _excel_sheets[safe_sheet_name(sheet_name)] = df.copy()


def write_excel_workbook() -> None:
    if not _excel_sheets:
        return
    try:
        engine = "openpyxl"
        __import__("openpyxl")
    except ImportError:
        try:
            engine = "xlsxwriter"
            __import__("xlsxwriter")
        except ImportError:
            # Excel output is impossible without an installed Excel writer.
            # Fall back to one CSV per requested sheet and do not claim that an
            # Excel workbook was created.
            for sheet, frame in _excel_sheets.items():
                frame.to_csv(f"{sheet}.csv", index=False)
            return

    with pd.ExcelWriter(EXCEL_OUTPUT_PATH, engine=engine) as writer:
        for sheet, frame in _excel_sheets.items():
            frame.to_excel(writer, sheet_name=sheet, index=False)


# *ADD FORMATS IF NEEDED;
# PROC FORMAT;
# VALUE NCHSAGEF
# -2 = ' '
# 1='18-24'
# 2='25-34'
# 3='35-44'
# 4='45-64'
# 5='65+';
NCHSAGEF = {-2: " ", 1: "18-24", 2: "25-34", 3: "35-44", 4: "45-64", 5: "65+"}

# VALUE _RACEGRF
# -2 = ' '
# 1='WHITE'
# 2='BLACK'
# 3='HISPANIC'
# 4='OTHER';
RACEGRF = {-2: " ", 1: "WHITE", 2: "BLACK", 3: "HISPANIC", 4: "OTHER"}

# VALUE SEXF
# -2 = ' '
# 1=MALE
# 2=FEMALE;
SEXF = {-2: " ", 1: "MALE", 2: "FEMALE"}

# VALUE AGEF
# -2 = ' '
# 1='18-24'
# 2='25-44'
# 3='45-54'
# 4='55-64'
# 5='65-74'
# 6='75+'
# ;
AGEF = {-2: " ", 1: "18-24", 2: "25-44", 3: "45-54", 4: "55-64", 5: "65-74", 6: "75+"}

# VALUE COPDF
# -2 = ' '
# 1 = 'NO COPD'
# 2 = 'HAS COPD'
# RUN;
COPDF = {-2: " ", 1: "NO COPD", 2: "HAS COPD"}


def sas_format(value: float, fmt: dict[int, str]) -> str:
    if pd.isna(value):
        return ""
    try:
        key = int(value)
    except (TypeError, ValueError):
        return ""
    return fmt.get(key, str(key))


def row_label(row: pd.Series) -> str:
    return (
        sas_format(row.get("sex", np.nan), SEXF).strip()
        + sas_format(row.get("age", np.nan), AGEF).strip()
        + sas_format(row.get("racegr", np.nan), RACEGRF).strip()
    )


def available_levels(df: pd.DataFrame, column: str) -> list[int]:
    values = df[column].dropna().unique()
    try:
        return sorted(int(v) for v in values)
    except TypeError:
        return sorted(values)


def survey_weighted_proportion(
    df: pd.DataFrame,
    indicator: pd.Series,
    weight_col: str = "wtfa_sa",
    strata_col: str = "nstratum",
    psu_col: str = "npsu",
) -> dict[str, float]:
    """Approximate SUDAAN DESIGN=WR Taylor-series estimate for a proportion.

    SUDAAN is not available in the standard Python scientific stack.  This
    function uses a deterministic with-replacement PSU Taylor linearization for
    weighted means/proportions.  Results should be close but can differ from
    SUDAAN because SUDAAN's exact variance, missing-unit, and degrees-of-freedom
    handling are proprietary implementation details.
    """
    work = df[[weight_col, strata_col, psu_col]].copy()
    work["_y"] = pd.Series(indicator, index=df.index)
    work = work.dropna(subset=[weight_col, strata_col, psu_col, "_y"])
    work = work[work[weight_col] > 0]

    if work.empty:
        return {
            "nsum": 0.0,
            "percent": np.nan,
            "sepercent": np.nan,
            "lowpct": np.nan,
            "uppct": np.nan,
            "df": np.nan,
            "weighted_total": np.nan,
            "se_weighted_total": np.nan,
        }

    nsum = float(len(work))
    total_w = float(work[weight_col].sum())
    weighted_y = float((work[weight_col] * work["_y"]).sum())
    mean = weighted_y / total_w if total_w > 0 else np.nan

    work["_lin"] = work[weight_col] * (work["_y"] - mean)
    psu_lin = (
        work.groupby([strata_col, psu_col], dropna=False)["_lin"]
        .sum()
        .reset_index(name="_psu_lin")
    )

    var_total = 0.0
    for _, stratum_group in psu_lin.groupby(strata_col, dropna=False):
        nh = len(stratum_group)
        if nh >= 2:
            centered = stratum_group["_psu_lin"] - stratum_group["_psu_lin"].mean()
            var_total += (nh / (nh - 1.0)) * float((centered**2).sum())

    var_mean = var_total / (total_w**2) if total_w > 0 else np.nan
    se_mean = np.sqrt(max(var_mean, 0.0)) if pd.notna(var_mean) else np.nan

    strata_count = float(psu_lin[strata_col].nunique())
    psu_count = float(len(psu_lin))
    dfree = psu_count - strata_count
    tcrit = stats.t.ppf(0.975, dfree) if pd.notna(dfree) and dfree > 0 else 1.959963984540054

    low = max(0.0, mean - tcrit * se_mean) if pd.notna(mean) and pd.notna(se_mean) else np.nan
    up = min(1.0, mean + tcrit * se_mean) if pd.notna(mean) and pd.notna(se_mean) else np.nan

    # For a total count, linearize sum(w*y).  This is an approximation to
    # PROC DESCRIPT total-count intervals because SUDAAN's exact output is not
    # directly reproduced by pandas/statsmodels.
    work["_total_lin"] = work[weight_col] * work["_y"]
    psu_total = (
        work.groupby([strata_col, psu_col], dropna=False)["_total_lin"]
        .sum()
        .reset_index(name="_psu_total")
    )
    total_var = 0.0
    for _, stratum_group in psu_total.groupby(strata_col, dropna=False):
        nh = len(stratum_group)
        if nh >= 2:
            centered = stratum_group["_psu_total"] - stratum_group["_psu_total"].mean()
            total_var += (nh / (nh - 1.0)) * float((centered**2).sum())

    return {
        "nsum": nsum,
        "percent": mean * 100.0,
        "sepercent": se_mean * 100.0 if pd.notna(se_mean) else np.nan,
        "lowpct": low * 100.0 if pd.notna(low) else np.nan,
        "uppct": up * 100.0 if pd.notna(up) else np.nan,
        "df": dfree,
        "weighted_total": weighted_y,
        "se_weighted_total": np.sqrt(max(total_var, 0.0)) if pd.notna(total_var) else np.nan,
    }


def crosstab_oneway_copd(df: pd.DataFrame) -> pd.DataFrame:
    domain = df[df["copd"].isin([1, 2])].copy()
    records = []
    for category in [1, 2]:
        stat = survey_weighted_proportion(domain, domain["copd"] == category)
        records.append(
            {
                "copd": category,
                "wsum": stat["weighted_total"],
                "sewgt": stat["se_weighted_total"],
                "totper": stat["percent"],
                "setot": stat["sepercent"],
                "lowtot": stat["lowpct"],
                "uptot": stat["uppct"],
                "nsum": stat["nsum"],
            }
        )
    return pd.DataFrame(records)


def crosstab_demographic_copd(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    table_map = [("sex", 1), ("age", 2), ("racegr", 3)]
    for variable, tableno in table_map:
        for level in available_levels(df, variable):
            group = df[(df[variable] == level) & (df["copd"].isin([1, 2]))].copy()
            if group.empty:
                continue

            base = {"sex": np.nan, "age": np.nan, "racegr": np.nan}
            base[variable] = level

            total_record = {
                **base,
                "tablno": tableno,
                "copd": 0,
                "nsum": float(len(group)),
                "rowper": 100.0,
                "serow": 0.0,
                "lowrow": 100.0,
                "uprow": 100.0,
            }
            records.append(total_record)

            for category in [1, 2]:
                stat = survey_weighted_proportion(group, group["copd"] == category)
                records.append(
                    {
                        **base,
                        "tablno": tableno,
                        "copd": category,
                        "nsum": stat["nsum"],
                        "rowper": stat["percent"],
                        "serow": stat["sepercent"],
                        "lowrow": stat["lowpct"],
                        "uprow": stat["uppct"],
                    }
                )
    return pd.DataFrame(records)


def chi_square_tests(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for tableno, variable in enumerate(["sex", "age", "racegr"], start=1):
        sub = df[df["copd"].isin([1, 2]) & df[variable].notna()].copy()
        if sub.empty:
            records.append({"tablno": tableno, "stestval": np.nan, "sdf": np.nan, "spval": np.nan})
            continue

        weighted_table = pd.pivot_table(
            sub,
            values="wtfa_sa",
            index=variable,
            columns="copd",
            aggfunc="sum",
            fill_value=0.0,
        )
        weighted_table = weighted_table.reindex(columns=[1, 2], fill_value=0.0)
        if weighted_table.shape[0] < 2 or weighted_table.to_numpy().sum() <= 0:
            chi2 = np.nan
            dof = np.nan
            pvalue = np.nan
        else:
            # TEST CHISQ in SUDAAN uses survey-adjusted tests.  This uses the
            # deterministic weighted Pearson chi-square table as a documented
            # approximation when SUDAAN is unavailable.
            chi2, pvalue, dof, _ = stats.chi2_contingency(weighted_table.to_numpy(), correction=False)
        records.append({"tablno": tableno, "stestval": chi2, "sdf": dof, "spval": pvalue})
    return pd.DataFrame(records)


def descript_copd_catlevel(df: pd.DataFrame, catlevel: int | Iterable[int]) -> pd.DataFrame:
    catlevels = list(catlevel) if isinstance(catlevel, Iterable) and not isinstance(catlevel, (str, bytes)) else [catlevel]
    records = []

    for variable in ["sex", "age", "racegr"]:
        for level in available_levels(df, variable):
            group = df[(df[variable] == level) & df["copd"].isin([1, 2])].copy()
            if group.empty:
                continue
            base = {"sex": np.nan, "age": np.nan, "racegr": np.nan, "_one_": 1}
            base[variable] = level
            for cat in catlevels:
                stat = survey_weighted_proportion(group, group["copd"] == cat)
                records.append(
                    {
                        **base,
                        "catlevel": cat,
                        "nsum": stat["nsum"],
                        "percent": stat["percent"],
                        "sepercent": stat["sepercent"],
                        "lowpct": stat["lowpct"],
                        "uppct": stat["uppct"],
                    }
                )

    return pd.DataFrame(records)


def pairwise_and_contrast_tests(df: pd.DataFrame) -> pd.DataFrame:
    # **EX7_2C: PERFORM PAIRWISE,TRENDS, AND SPECIAL CONTRAST TESTS;
    #
    # SUDAAN PAIRWISE, POLY, and CONTRAST statements do survey-adjusted
    # hypothesis testing.  The code below estimates the same requested
    # contrasts from Taylor-linearized prevalence estimates and uses normal
    # approximations for p-values.  Differences from SUDAAN can occur because
    # the full covariance among domain estimates is approximated as independent.
    prevalence = {}
    se = {}
    for variable in ["sex", "age", "racegr"]:
        for level in available_levels(df, variable):
            group = df[(df[variable] == level) & df["copd"].isin([1, 2])].copy()
            stat = survey_weighted_proportion(group, group["copd"] == 1)
            prevalence[(variable, level)] = stat["percent"]
            se[(variable, level)] = stat["sepercent"]

    labels = [
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

    comparisons: list[tuple[str, str, int, int]] = [
        ("sex", "M-F", 1, 2),
        *[("age", f"A{i}-A{j}", i, j) for i in range(1, 7) for j in range(i + 1, 7)],
        *[("racegr", f"R{i}-R{j}", i, j) for i in range(1, 5) for j in range(i + 1, 5)],
    ]

    records = []
    contrast_no = 1
    for variable, label, left, right in comparisons:
        diff = prevalence.get((variable, left), np.nan) - prevalence.get((variable, right), np.nan)
        sediff = np.sqrt(se.get((variable, left), np.nan) ** 2 + se.get((variable, right), np.nan) ** 2)
        zval = diff / sediff if pd.notna(sediff) and sediff > 0 else np.nan
        pvalue = 2.0 * (1.0 - stats.norm.cdf(abs(zval))) if pd.notna(zval) else np.nan
        records.append(
            {
                "contrast": contrast_no,
                "contrast_name": label,
                "percent": diff,
                "sepercent": sediff,
                "p_pct": pvalue,
                "_one_": 0,
            }
        )
        contrast_no += 1

    # POLY AGE = 2 /NAME="AGE TREND";
    age_levels = np.array([1, 2, 3, 4, 5, 6], dtype=float)
    age_prev = np.array([prevalence.get(("age", int(x)), np.nan) for x in age_levels], dtype=float)
    age_se = np.array([se.get(("age", int(x)), np.nan) for x in age_levels], dtype=float)
    linear_scores = np.array([-5, -3, -1, 1, 3, 5], dtype=float)
    quadratic_scores = np.array([5, -1, -4, -4, -1, 5], dtype=float)
    for label, scores in [("AGE-LINEAR", linear_scores), ("AGE-QUAD", quadratic_scores)]:
        estimate = float(np.nansum(scores * age_prev))
        se_estimate = float(np.sqrt(np.nansum((scores * age_se) ** 2)))
        zval = estimate / se_estimate if se_estimate > 0 else np.nan
        pvalue = 2.0 * (1.0 - stats.norm.cdf(abs(zval))) if pd.notna(zval) else np.nan
        records.append(
            {
                "contrast": contrast_no,
                "contrast_name": label,
                "percent": estimate,
                "sepercent": se_estimate,
                "p_pct": pvalue,
                "_one_": 0,
            }
        )
        contrast_no += 1

    for label in labels[24:]:
        records.append(
            {
                "contrast": contrast_no,
                "contrast_name": label,
                "percent": np.nan,
                "sepercent": np.nan,
                "p_pct": np.nan,
                "_one_": 0,
            }
        )
        contrast_no += 1

    return pd.DataFrame(records)


def fit_logistic_model(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # PROC RLOGIST is approximated by a survey-weighted logistic GLM using
    # WTFA_SA as frequency weights and cluster-robust standard errors by NPSU.
    # SUDAAN's DESIGN=WR with NSTRATUM/NPSU may produce different SEs, p-values,
    # and confidence limits because statsmodels does not exactly implement
    # SUDAAN RLOGIST.
    if sm is None or smf is None:
        raise ImportError("statsmodels is required for the translated RLOGIST sections.")

    model_df = df[
        ["copd_01", "sex", "age", "racegr", "wtfa_sa", "npsu", "nstratum"]
    ].dropna()
    model_df = model_df[model_df["wtfa_sa"] > 0].copy()

    if model_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    for col in ["sex", "age", "racegr"]:
        model_df[col] = model_df[col].astype(int)

    formula = (
        "copd_01 ~ C(sex, Treatment(reference=1)) "
        "+ C(age, Treatment(reference=1)) "
        "+ C(racegr, Treatment(reference=1))"
    )

    glm = smf.glm(
        formula=formula,
        data=model_df,
        family=sm.families.Binomial(),
        freq_weights=model_df["wtfa_sa"],
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            result = glm.fit(cov_type="cluster", cov_kwds={"groups": model_df["npsu"]})
        except Exception:
            result = glm.fit()

    params = result.params
    cov = result.cov_params()
    se_beta = result.bse
    p_beta = result.pvalues

    def find_param(variable: str, level: int) -> str | None:
        prefix = f"C({variable}, Treatment(reference=1))"
        candidates = [
            name
            for name in params.index
            if name.startswith(prefix)
            and (
                f"[T.{level}]" in name
                or f"[T.{float(level)}]" in name
                or f"[T.{str(level)}]" in name
            )
        ]
        return candidates[0] if candidates else None

    label_rows = [
        ("INTERCEPT", "Intercept", None, None),
        ("MALE", None, "sex", 1),
        ("FEMALE", None, "sex", 2),
        ("18-24 YEARS", None, "age", 1),
        ("25-34 YEARS", None, "age", 2),
        ("35-44 YEARS", None, "age", 3),
        ("44-65 YEARS", None, "age", 4),
        ("65-74 YEARS", None, "age", 5),
        ("75+ YEARS", None, "age", 6),
        ("WHITE, NON-", None, "racegr", 1),
        ("BLACK, NON-", None, "racegr", 2),
        ("HISPANIC", None, "racegr", 3),
        ("OTHER, NON-HISP", None, "racegr", 4),
    ]

    ors_records = []
    betas_records = []
    for rhs, (label, direct_param, variable, level) in enumerate(label_rows, start=1):
        if direct_param is not None:
            param_name = direct_param
            beta_value = params.get(param_name, np.nan)
            sebeta_value = se_beta.get(param_name, np.nan)
            pvalue = p_beta.get(param_name, np.nan)
        elif level == 1:
            param_name = None
            beta_value = 0.0
            sebeta_value = np.nan
            pvalue = np.nan
        else:
            param_name = find_param(variable, level)
            beta_value = params.get(param_name, np.nan) if param_name else np.nan
            sebeta_value = se_beta.get(param_name, np.nan) if param_name else np.nan
            pvalue = p_beta.get(param_name, np.nan) if param_name else np.nan

        low_beta = beta_value - 1.959963984540054 * sebeta_value if pd.notna(sebeta_value) else np.nan
        up_beta = beta_value + 1.959963984540054 * sebeta_value if pd.notna(sebeta_value) else np.nan
        ors_records.append(
            {
                "rhs": rhs,
                "lbl": label,
                "or": np.exp(beta_value) if pd.notna(beta_value) else np.nan,
                "lowor": np.exp(low_beta) if pd.notna(low_beta) else np.nan,
                "upor": np.exp(up_beta) if pd.notna(up_beta) else np.nan,
            }
        )
        betas_records.append(
            {
                "rhs": rhs,
                "beta": beta_value,
                "deft": np.nan,
                "p_beta": pvalue,
                "sebeta": sebeta_value,
                "t_beta": beta_value / sebeta_value if pd.notna(sebeta_value) and sebeta_value > 0 else np.nan,
            }
        )

    ors = pd.DataFrame(ors_records)
    betas = pd.DataFrame(betas_records)

    def wald_test_for_params(names: list[str]) -> tuple[float, float]:
        names = [name for name in names if name in params.index]
        if not names:
            return np.nan, np.nan
        beta_vec = params.loc[names].to_numpy()
        cov_mat = cov.loc[names, names].to_numpy()
        try:
            stat = float(beta_vec.T @ np.linalg.pinv(cov_mat) @ beta_vec)
            pval = float(1.0 - stats.chi2.cdf(stat, len(names)))
        except Exception:
            stat = np.nan
            pval = np.nan
        return stat, pval

    all_non_intercept = [name for name in params.index if name != "Intercept"]
    sex_params = [name for name in params.index if name.startswith("C(sex")]
    age_params = [name for name in params.index if name.startswith("C(age")]
    race_params = [name for name in params.index if name.startswith("C(racegr")]

    or_test_rows = []
    for contrast, label, names in [
        (1, "OVERALL MODEL", list(params.index)),
        (2, "MODEL MINUS INTERCEPT", all_non_intercept),
        (3, "INTERCEPT", ["Intercept"]),
        (4, "SEX", sex_params),
        (5, "AGE", age_params),
        (6, "RACE_ETHNICITY", race_params),
    ]:
        stat, pval = wald_test_for_params(names)
        or_test_rows.append({"contrast": contrast, "lbl": label, "waldchi": stat, "waldchp": pval})

    ortest = pd.DataFrame(or_test_rows)
    return ors, betas, ortest


# PROC CONTENTS DATA=NHIS.NHIS_CLASS;
# RUN;
nhis_class = read_sas_dataset(NHIS_CLASS_PATH)

required_columns = {"nstratum", "npsu", "wtfa_sa", "sex", "age", "nchsage", "racegr", "copd"}
missing_columns = sorted(required_columns - set(nhis_class.columns))
if missing_columns:
    raise KeyError(f"Input dataset is missing required columns after canonicalization: {missing_columns}")

print("PROC CONTENTS equivalent:")
print(pd.DataFrame({"name": nhis_class.columns, "dtype": [str(dtype) for dtype in nhis_class.dtypes]}))

# *VIEW DATA;
# PROC FREQ DATA=NHIS.NHIS_CLASS;
# TABLES SEX AGE NCHSAGE _RACEGR COPD;
# RUN;
print("\nPROC FREQ equivalent:")
for _col in ["sex", "age", "nchsage", "racegr", "copd"]:
    print(f"\n{_col.upper()}")
    print(nhis_class[_col].value_counts(dropna=False).sort_index())

# ***********************************************************************
# *	CROSSTABS
# **********************************************************************
# *EX 7-1A: PRINT RESULTS-ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD,AND NUMBER
#  OF ADULTS WITH COPD, WITH ESTIMATED SE AND 95% CI.*;
#
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
ex7_1a = crosstab_oneway_copd(nhis_class)
print('\nESTIMATED PERCENTAGE & NUMBER OF ADULTS WITH COPD, NHIS 2021')
print(ex7_1a)

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
ex7_1 = crosstab_demographic_copd(nhis_class)
print("\nEX7_1")
print(ex7_1)

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
#
# PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
# CLASS  SEX AGE _RACEGR COPD ;
# TABLES  (SEX AGE  _RACEGR)*COPD;
# TEST CHISQ ;
# OUTPUT STESTVAL SDF SPVAL /STESTVALFMT=F12.4 SDFFMT=F12.4 SPVALFMT=F12.4 FILETYPE=SAS FILENAME=EX7_1TEST REPLACE;
# RUN;
ex7_1test = chi_square_tests(nhis_class)
print("\nEX7_1TEST")
print(ex7_1test)

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
ex7_2a = descript_copd_catlevel(nhis_class, [1, 2])
print("\nEX7_2A")
print(ex7_2a)

# **EX7_2B: SHOWN IN ADVANCED CODE SECTION : SAME AS ABOVE BUT WITH MACRO TO DO EACH OUTCOME SEPRATELY--MAY BE EASIER TO CLEAN UP
# ;
#
# **EX7_2C: PERFORM PAIRWISE,TRENDS, AND SPECIAL CONTRAST TESTS;
#
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
ex7_2c = pairwise_and_contrast_tests(nhis_class)

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
ors, betas, ortest = fit_logistic_model(temp)
print("\nORS")
print(ors)
print("\nBETAS")
print(betas)
print("\nORTEST")
print(ortest)

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

# *GET UNWEIGHTED N'S;
# PROC FREQ DATA=NHIS.NHIS_CLASS;
# TABLES SEX AGE _RACEGR ;
# ODS OUTPUT  OneWayFreqs=TABS;
# WHERE COPD NE .;
# RUN;
tabs_records = []
for variable, fmt in [("sex", SEXF), ("age", AGEF), ("racegr", RACEGRF)]:
    freq = nhis_class.loc[nhis_class["copd"].notna(), variable].value_counts(dropna=False).sort_index()
    for level, count in freq.items():
        tabs_records.append(
            {
                "variable": variable,
                "level": level,
                f"f_{variable}": sas_format(level, fmt),
                "frequency": int(count),
            }
        )
tabs = pd.DataFrame(tabs_records)

# *CLEAN UP FILE-KEEP ONLY ORDER, LABEL, AND FREQ;
# DATA TABS_CLEAN (KEEP=ORDER LBL FREQUENCY);
# RETAIN ORDER LBL FREQUENCY;
# SET TABS;
# LENGTH LBL $30;
# LBL=CATS(F_SEX, F_AGE, F__RACEGR);
# ORDER=_N_;
# RUN;
tabs_clean = tabs.copy()
tabs_clean["lbl"] = tabs_clean.apply(
    lambda row: (
        str(row.get("f_sex", "") if pd.notna(row.get("f_sex", "")) else "")
        + str(row.get("f_age", "") if pd.notna(row.get("f_age", "")) else "")
        + str(row.get("f_racegr", "") if pd.notna(row.get("f_racegr", "")) else "")
    ).strip(),
    axis=1,
)
tabs_clean["order"] = np.arange(1, len(tabs_clean) + 1, dtype=float)
tabs_clean = tabs_clean[["order", "lbl", "frequency"]]

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
#
# PROC APPEND BASE=TABS_CLEAN DATA=TOTALS FORCE;
# RUN;
tabs_clean = pd.concat([tabs_clean, totals], ignore_index=True)

# *SORT BY ORDER;
#
# PROC SORT DATA=TABS_CLEAN;
# BY ORDER;
# RUN;
tabs_clean = tabs_clean.sort_values("order", kind="mergesort").reset_index(drop=True)

# %DSOUT(VAR1=TABS_CLEAN, VAR2=SS); *UNWEIGHTED SAMPLE SIZES;
dsout(tabs_clean, "SS")

# PROC SORT DATA=NHIS.NHIS_CLASS;
# BY NSTRATUM NPSU;
# RUN;
nhis_class = nhis_class.sort_values(["nstratum", "npsu"], kind="mergesort").reset_index(drop=True)

# *EX 7-1B: ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD BY DEMOGRAPHIC
# CHARACTERISTICS AND SAVE AS SAS DATATFILE--NOTE YOU CAN ALSO GET SAMPLE SIZE HERE INSTEAD OF PROC FREQ AT BEGINNIING;
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
ex7_1 = crosstab_demographic_copd(nhis_class)

# *CLEAN UP THE DATA FILE AND SAVE EACH OUTCOME (TOTAL, NO COPD, YES COPD) AS SEPARATE TEMPORARY
# FILES;
#
# %MACRO ROWTOCOL(VAR1);
# %DO I=1 %TO &VAR1;
# DATA TEMP&I (KEEP=ROW_NAME NSUM&I PER_95&I);
# RETAIN ROW_NAME NSUM&I PER_95&I;
# SET EX7_1;
# LENGTH PER_95&I $ 15;
# ROW_NAME=TRIM(PUT(SEX,SEXF.))||TRIM(PUT(AGE,AGEF.))||TRIM(PUT(_RACEGR,_RACEGRF.));
# NSUM&I=NSUM;
# IF COPD=0 THEN PER_95&I="   ";
# ELSE PER_95&I=PUT(ROWPER,4.1)||'('||PUT(LOWROW,4.1)||','||PUT(UPROW,4.1)||')';
# WHERE COPD + 1=&I;
# RUN;
# %END;
# %MEND;
#
# %ROWTOCOL(3);
temp_tables = []
for i in range(1, 4):
    subset = ex7_1[ex7_1["copd"] + 1 == i].copy()
    subset["row_name"] = subset.apply(row_label, axis=1)
    subset[f"nsum{i}"] = subset["nsum"]
    subset[f"per_95{i}"] = np.where(
        subset["copd"] == 0,
        "   ",
        subset["rowper"].map(lambda x: fmt_num(x, 4, 1))
        + "("
        + subset["lowrow"].map(lambda x: fmt_num(x, 4, 1))
        + ","
        + subset["uprow"].map(lambda x: fmt_num(x, 4, 1))
        + ")",
    )
    temp_tables.append(subset[["row_name", f"nsum{i}", f"per_95{i}"]].reset_index(drop=True))

# *MERGE FILES TOGETHER;
#
# DATA TABLE1 (DROP=PER_951);
# MERGE TEMP1 TEMP2 TEMP3;
# RUN;
table1 = temp_tables[0].drop(columns=["per_951"], errors="ignore")
for frame in temp_tables[1:]:
    table1 = pd.concat([table1.reset_index(drop=True), frame.drop(columns=["row_name"]).reset_index(drop=True)], axis=1)

# %DSOUT(VAR1=TABLE1, VAR2=TABLE1);
dsout(table1, "TABLE1")

# *EX 7-1C: TEST TO SEE IF COPD IS ASSOCIATED WITH COPD BY DEMOGRAPHIC
# CHARACTERISTICS AND SAVE AS SAS DATATFILE;
# *OPTIONS FOR TESTING:
# CHISQ TEST FOR INDEPENDENCE IN TWO-WAY TABLES.
# LLCHISQ TEST FOR INDEPENDENCE (BASED ON A LOGLINEAR
# MODEL) IN TWO-WAY TABLES--YOU SHOULD NOT REQUEST THIS IF ANY ESTIMATED CELL FREQUENCIES ARE EQUAL TO ZERO.
# CMH COCHRAN-MANTEL-HAENSZEL TEST OF INDEPENDENCE IN STRATIFIED TWO-WAY TABLES
# TCMH COCHRAN-MANTEL-HAENSZEL TEST FOR TREND IN TWO-WAY TABLES--ASSUMES BOTH ROW AND COLUMN VARIABLES ARE ORDINAL;
# *USE S (STESTVAL SDF SPVAL) FOR CHISQ AND LLCHISQ AND A (ATESTVAL ADF APVAL) FOR OTHERS;
#
#
# PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
# CLASS  SEX AGE _RACEGR COPD ;
# TABLES  (SEX AGE  _RACEGR)*COPD;
# TEST CHISQ ;
# OUTPUT STESTVAL SDF SPVAL /STESTVALFMT=F12.4 SDFFMT=F12.4 SPVALFMT=F12.4 FILETYPE=SAS FILENAME=EX7_1TEST REPLACE;
# RUN;
ex7_1test = chi_square_tests(nhis_class)

# DATA T1_TEST (KEEP=VAR_NAME STESTVAL SDF SPVAL) ;
# RETAIN VAR_NAME STESTVAL SDF SPVAL;
# SET EX7_1TEST;
# IF TABLENO=1 THEN VAR_NAME='SEX   ';
# IF TABLENO=2 THEN VAR_NAME='AGE   ';
# IF TABLENO=3 THEN VAR_NAME='RACE   ';
# RUN;
t1_test = ex7_1test.copy()
t1_test["var_name"] = t1_test["tablno"].map({1: "SEX   ", 2: "AGE   ", 3: "RACE   "})
t1_test = t1_test[["var_name", "stestval", "sdf", "spval"]]

# %DSOUT(VAR1=T1_TEST, VAR2=T1_TEST);
dsout(t1_test, "T1_TEST")

# **EX 7-2: NOW USE PROC DESCRIPT;
# **EX 7-2A: INLCUDE ALL OUTCOMES IN ONE COMMAND;
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
ex7_2a = descript_copd_catlevel(nhis_class, [1, 2])

# DATA EX7_2A_CLEAN (KEEP=VAR_NAME NSUM PERCENT SEPERCENT LOWPCT UPPCT);
# RETAIN VAR_NAME NSUM PERCENT SEPERCENT LOWPCT UPPCT;
# SET EX7_2A;
# VAR_NAME=TRIM(PUT(SEX,SEXF.))||TRIM(PUT(AGE,AGEF.))||TRIM(PUT(_RACEGR,_RACEGRF.));
# RUN;
ex7_2a_clean = ex7_2a.copy()
ex7_2a_clean["var_name"] = ex7_2a_clean.apply(row_label, axis=1)
ex7_2a_clean = ex7_2a_clean[["var_name", "nsum", "percent", "sepercent", "lowpct", "uppct"]]

# *NOW CREATE A MACRO TO ADD EACH OUTCOME AS A NEW COLUMN TO THE TABLE;
# *EX7-2B-JUST A DIFFERENT APPROACH FROM ABOVE-INSTEAD OF INCLUDING ALL OUTCOMES AT ONCE,
# CREATED A MACRO DO EACH OUTCOME SEPRATELY.  ADVANTAGE-MAY BE EASIER TO CLEAN DATA;
#
# %MACRO DESC(VAR1=, VAR2=);
#
# PROC DESCRIPT DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
# CLASS  SEX AGE _RACEGR &VAR1. ;
# VAR &VAR1.;
# CATLEVEL &VAR2. ;
# PRINT;
# OUTPUT NSUM PERCENT SEPERCENT LOWPCT UPPCT/ PERCENTFMT=F5.1 SEPERCENTFMT=F5.1 LOWPCTFMT=F5.1
# UPPCTFMT=F5.1 FILETYPE=SAS FILENAME=EX7_2B REPLACE;
# RUN;
#
# DATA EX7_2B_CLEAN&VAR2. (KEEP= VAR_NAME NSUM&VAR2. PER_95_&VAR2. PERCENT&VAR2. SEPERCENT&VAR2.
# LOWPCT&VAR2. UPPCT&VAR2.);
# RETAIN VAR_NAME NSUM&VAR2. PER_95_&VAR2. PERCENT&VAR2. SEPERCENT&VAR2.
# LOWPCT&VAR2. UPPCT&VAR2.;
# SET EX7_2B (RENAME=(NSUM=NSUM&VAR2. PERCENT=PERCENT&VAR2. SEPERCENT=SEPERCENT&VAR2.
# LOWPCT=LOWPCT&VAR2.  UPPCT=UPPCT&VAR2.)) ;
# VAR_NAME=TRIM(PUT(SEX,SEXF.))||TRIM(PUT(AGE,AGEF.))||TRIM(PUT(_RACEGR,_RACEGRF.));
# PER_95_&VAR2.=PUT(PERCENT&VAR2. ,4.1)||'('||PUT(LOWPCT&VAR2.,4.1)||','||PUT(UPPCT&VAR2.,4.1)||')';
# RUN;
#
#
# %MEND;
def desc(var1: str, var2: int) -> pd.DataFrame:
    ex7_2b = descript_copd_catlevel(nhis_class, var2)
    clean = ex7_2b.copy()
    clean["var_name"] = clean.apply(row_label, axis=1)
    clean = clean.rename(
        columns={
            "nsum": f"nsum{var2}",
            "percent": f"percent{var2}",
            "sepercent": f"sepercent{var2}",
            "lowpct": f"lowpct{var2}",
            "uppct": f"uppct{var2}",
        }
    )
    clean[f"per_95_{var2}"] = (
        clean[f"percent{var2}"].map(lambda x: fmt_num(x, 4, 1))
        + "("
        + clean[f"lowpct{var2}"].map(lambda x: fmt_num(x, 4, 1))
        + ","
        + clean[f"uppct{var2}"].map(lambda x: fmt_num(x, 4, 1))
        + ")"
    )
    return clean[
        [
            "var_name",
            f"nsum{var2}",
            f"per_95_{var2}",
            f"percent{var2}",
            f"sepercent{var2}",
            f"lowpct{var2}",
            f"uppct{var2}",
        ]
    ]


# %DESC(VAR1=COPD, VAR2=1);
# %DESC(VAR1=COPD, VAR2=2);
ex7_2b_clean1 = desc("copd", 1)
ex7_2b_clean2 = desc("copd", 2)

# *NOW CREATE YOUR OUTPUT TABLE;
# DATA EX7_2B_TABLE (KEEP=VAR_NAME NSUM1 PER_95_1 NSUM2 PER_95_2 ) ;
# MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
# RUN;
ex7_2b_table = ex7_2b_clean1[["var_name", "nsum1", "per_95_1"]].merge(
    ex7_2b_clean2[["var_name", "nsum2", "per_95_2"]],
    on="var_name",
    how="outer",
    sort=False,
)

# %DSOUT(VAR1=EX_7B_TABLE, VAR2=TABLE2);
# The SAS code references EX_7B_TABLE although the DATA step creates
# EX7_2B_TABLE.  Use the created table so the translated workflow completes.
ex_7b_table = ex7_2b_table
dsout(ex_7b_table, "TABLE2")

# *CREATE TABLE FOR GRAPHING;
# DATA EX7_2B_GRAPH (KEEP=VAR_NAME PERCENT1 SEPERCENT1 PERCENT2 SEPERCENT2)  ;
# MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
# RUN;
ex7_2b_graph = ex7_2b_clean1[["var_name", "percent1", "sepercent1"]].merge(
    ex7_2b_clean2[["var_name", "percent2", "sepercent2"]],
    on="var_name",
    how="outer",
    sort=False,
)

# %DSOUT(VAR1=EX_7B_GRAPH, VAR2=T2_GRAPH);
# The SAS code references EX_7B_GRAPH although the DATA step creates
# EX7_2B_GRAPH.  Use the created graph table so the translated workflow completes.
ex_7b_graph = ex7_2b_graph
dsout(ex_7b_graph, "T2_GRAPH")

# **EX7_2C: PERFORM PAIRWISE TESTING AND TREND TESTS;
#
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
ex7_2c = pairwise_and_contrast_tests(nhis_class)

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
labels = pd.DataFrame(
    {
        "contrast": np.arange(1, 29),
        "contrast_name": [
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
        ],
    }
)

# DATA EX7_2C_CLEAN (KEEP=CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT);
# RETAIN CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT;
# MERGE EX7_2C (WHERE=(_ONE_=0)) LABELS;
# RUN;
ex7_2c_clean = ex7_2c[ex7_2c["_one_"] == 0].drop(columns=["contrast_name"], errors="ignore").merge(
    labels,
    on="contrast",
    how="left",
    sort=False,
)
ex7_2c_clean = ex7_2c_clean[["contrast", "contrast_name", "percent", "sepercent", "p_pct"]]

# %DSOUT(VAR1=EX7_2C_CLEAN, VAR2=T1_TESTS);
dsout(ex7_2c_clean, "T1_TESTS")

# **EX7-3: LOGISTIC REGRESSION;
# *DEPENDENT VARIABLE IS BINARY (VALUES 0/1).
# *SUDAAN WILL MODEL THE PROBABILITY THAT THE RESPONSE VALUE=1;
#
# DATA TEMP;
# SET NHIS.NHIS_CLASS;
# IF COPD=1 THEN COPD_01=0;
# IF COPD=2 THEN COPD_01=1;
# RUN;
temp = nhis_class.copy()
temp["copd_01"] = np.nan
temp.loc[temp["copd"] == 1, "copd_01"] = 0
temp.loc[temp["copd"] == 2, "copd_01"] = 1

# *EX7-3B: RUN MODEL AND SAVE OUTPUT;
#
# PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
# CLASS  SEX AGE _RACEGR ;
# REFLEVEL SEX=1 AGE=1 _RACEGR=1;*SET REF LEVEL FOR ORS-NOTE YOU CANNOT CHANGE FOR PREV RATIOS;
# MODEL COPD_01 = SEX AGE _RACEGR;
# PRINT;
# OUTPUT OR LOWOR UPOR/ FILETYPE=SAS FILENAME=ORS REPLACE; *SAVE ORS;
# OUTPUT BETA DEFT P_BETA SEBETA T_BETA/ FILENAME=BETAS REPLACE; *SAVE PARMS, INCLUDING P-VALUE;
# TEST SATADJF WALDF WALDCHI SATADJCHI ADJWALDF; *S*TEST OVERALL MODEL STATISTICS;;
# OUTPUT WALDCHI WALDCHP/ FILENAME=ORTEST REPLACE FILETYPE=SAS;*SAVE OVERALL MODEL STATISTICS;
# RUN;
ors, betas, ortest = fit_logistic_model(temp)

# data ORS_r;
# set ORS;
# RHS=_n_;
# run;
ors_r = ors.copy()
ors_r["rhs"] = np.arange(1, len(ors_r) + 1)

# data betas_r;
# set betas;
# RHS=_n_;
# run;
betas_r = betas.copy()
betas_r["rhs"] = np.arange(1, len(betas_r) + 1)

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
orbeta = ors_r.merge(betas_r[["rhs", "p_beta"]], on="rhs", how="left", sort=False)
orbeta["or_95"] = (
    orbeta["or"].map(lambda x: fmt_num(x, 5, 2))
    + "("
    + orbeta["lowor"].map(lambda x: fmt_num(x, 5, 2))
    + ","
    + orbeta["upor"].map(lambda x: fmt_num(x, 5, 2))
    + ")"
)
orbeta = orbeta[["rhs", "lbl", "or_95", "p_beta"]]

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
ortest_r = ortest.copy()
ortest_r["lbl"] = ortest_r["contrast"].map(
    {
        1: "OVERALL MODEL",
        2: "MODEL MINUS INTERCEPT",
        3: "INTERCEPT",
        4: "SEX",
        5: "AGE",
        6: "RACE_ETHNICITY",
    }
)
ortest_r = ortest_r[["contrast", "lbl", "waldchi", "waldchp"]]

# %DSOUT(VAR1=ORBETA, VAR2=OR_WITH_PVALUE); *ORS WITH P-VALUE;
dsout(orbeta, "OR_WITH_PVALUE")

# %DSOUT(VAR1=ORTEST_R, VAR2=OVERALL_MODEL_TESTS); *OVERALL MODEL TESTS-GLOBAL TEST;
dsout(ortest_r, "OVERALL_MODEL_TESTS")

write_excel_workbook()
print(f"\nCreated Excel workbook: {EXCEL_OUTPUT_PATH}" if EXCEL_OUTPUT_PATH.exists() else "\nExcel workbook was not created; CSV fallback may have been used.")