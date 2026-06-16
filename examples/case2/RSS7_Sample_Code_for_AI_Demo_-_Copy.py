# /*********************************************
# Aim: To examine prevalence of GLP-1 use, including compounded medications, sources of fills, and barriers to use
# Data Source: Rapid Survey System (RSS-7) data. Rapids is run by CDC/NCHS
# Analyst: Sam Pierce (NYA7)
# *********************************************/

from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from scipy.stats import f, t


# %include "C:\Users\nya7\OneDrive - CDC\Sam\PHHT Materials\NCHS Rapids Survey 2025 GLP1\Data\RSS7 PUF Input Program_SLP.sas";
# libname rss "C:\Users\nya7\OneDrive - CDC\Sam\PHHT Materials\NCHS Rapids Survey 2025 GLP1\Data";
#
# Use the uploaded/current execution directory rather than a hard-coded local
# Windows path from the SAS LIBNAME/%INCLUDE statements.
INPUT_DIR = Path(os.environ.get("SAS2PY_INPUT_DIR", "."))
INPUT_DATASET = INPUT_DIR / "rss7_puf.sas7bdat"
OUTPUT_WORKBOOK = Path("rss7_glp1_analysis_outputs.xlsx")


def canonicalize_name(name: Any) -> str:
    """Convert SAS-style column names to deterministic, Python-safe names."""
    text = str(name).strip().lower()
    text = re.sub(r"[^0-9a-zA-Z_]+", "_", text)
    text = text.lstrip("_")
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = "column"
    if text[0].isdigit():
        text = f"x{text}"
    return text


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe columns once, then use only canonical names."""
    used: dict[str, int] = {}
    new_columns: list[str] = []
    for col in df.columns:
        base = canonicalize_name(col)
        if base not in used:
            used[base] = 0
            new_columns.append(base)
        else:
            used[base] += 1
            new_columns.append(f"{base}_{used[base]}")
    out = df.copy()
    out.columns = new_columns
    return out


def read_sas_dataset(path: Path) -> pd.DataFrame:
    """Read a SAS7BDAT file with pyreadstat when available and pandas as fallback.

    Do not pass pyreadstat options such as apply_value_formats because some
    installed versions do not support those keywords. Explicit PROC FORMAT
    mappings from the SAS source are translated below for display output.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Input dataset not found: {path}. "
            "Place rss7_puf.sas7bdat in SAS2PY_INPUT_DIR or the current directory."
        )

    try:
        import pyreadstat  # type: ignore

        try:
            df, _metadata = pyreadstat.read_sas7bdat(str(path))
            return canonicalize_columns(df)
        except TypeError:
            df, _metadata = pyreadstat.read_sas7bdat(str(path))
            return canonicalize_columns(df)
    except ImportError:
        df = pd.read_sas(path, format="sas7bdat", encoding="utf-8")
        return canonicalize_columns(df)


def as_numeric(series: pd.Series) -> pd.Series:
    """Return numeric values for SAS-coded variables while preserving NaN."""
    return pd.to_numeric(series, errors="coerce")


def sas_round(value: float, unit: float) -> float:
    """Approximate SAS ROUND for scalar numeric output."""
    if pd.isna(value):
        return np.nan
    return float(np.round(value / unit) * unit)


def safe_sheet_name(name: str) -> str:
    """Excel sheet names must be <= 31 characters and cannot contain []:*?/\\."""
    clean = re.sub(r"[\[\]:*?/\\]", "_", name)
    return clean[:31]


# proc contents data=rss.rss7_puf; run;
rss7_puf = read_sas_dataset(INPUT_DATASET)
contents = pd.DataFrame(
    {
        "name": list(rss7_puf.columns),
        "dtype": [str(dtype) for dtype in rss7_puf.dtypes],
        "nonmissing": [int(rss7_puf[col].notna().sum()) for col in rss7_puf.columns],
        "missing": [int(rss7_puf[col].isna().sum()) for col in rss7_puf.columns],
    }
)

# proc format;
# 	value ages_b 1="18-39y" 2="40-64y" 3="65y+";
# 	value bmis -8="Not Ascertained" 1="Underweight" 2="Healthy Weight" 3="Overweight" 4="Obesity- Class 1 or 2" 6="Severe Obesity";
# 	value yn 1="Yes" 2="No"	3="DK";
# 	value re 1="White" 2="Black" 3="Hispanic" 4="Other/Multiracial" 5="Missing";
# 	value source 1="Multiple Source" 2="Single Source";
# 	value glp 1="Yes" 2="No" 3="Missing";
# 	value sexage 1="M 18-39" 2="M 40-64" 3="M 65+" 4="F 18-39" 5="F 40-64" 6="F 65+";
# run;
format_maps: dict[str, dict[Any, str]] = {
    "ages_b": {1: "18-39y", 2: "40-64y", 3: "65y+"},
    "bmis": {
        -8: "Not Ascertained",
        1: "Underweight",
        2: "Healthy Weight",
        3: "Overweight",
        4: "Obesity- Class 1 or 2",
        6: "Severe Obesity",
    },
    "yn": {1: "Yes", 2: "No", 3: "DK"},
    "re": {1: "White", 2: "Black", 3: "Hispanic", 4: "Other/Multiracial", 5: "Missing"},
    "source": {1: "Multiple Source", 2: "Single Source"},
    "glp": {1: "Yes", 2: "No", 3: "Missing"},
    "sexage": {
        1: "M 18-39",
        2: "M 40-64",
        3: "M 65+",
        4: "F 18-39",
        5: "F 40-64",
        6: "F 65+",
    },
    # The SAS source uses p_sex, p_poverty4_r, nchs_metro, and dem_region in
    # SUDAAN subgroup output without a visible FORMAT statement in this file.
    # These mappings keep displayed categories readable when those standard
    # RSS/NCHS coded values are present; unrecognized codes fall back to raw text.
    "sex": {1: "Male", 2: "Female"},
    "poverty4": {
        1: "Below 100% FPL",
        2: "100%-199% FPL",
        3: "200%-399% FPL",
        4: "400%+ FPL",
    },
    "metro": {1: "Metropolitan", 2: "Nonmetropolitan"},
    "region": {1: "Northeast", 2: "Midwest", 3: "South", 4: "West"},
}

variable_formats: dict[str, str] = {
    "agecat_b": "ages_b",
    "bmicat": "bmis",
    "race": "re",
    "compounded": "yn",
    "multiple_source": "source",
    "glp_med12m": "glp",
    "sexage": "sexage",
    "p_sex": "sex",
    "p_poverty4_r": "poverty4",
    "nchs_metro": "metro",
    "dem_region": "region",
    # The SAS code refers to format rx. for glp_medrx and glp_mednow, but rx.
    # is not defined in this visible PROC FORMAT block. Use the translated glp
    # yes/no/missing labels for display if these variables are exported.
    "glp_medrx": "glp",
    "glp_mednow": "glp",
}


def normalize_format_key(value: Any) -> Any:
    """Normalize numeric-looking values so SAS-style numeric format keys match."""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        if float(value).is_integer():
            return int(value)
        return float(value)
    text = str(value).strip()
    try:
        numeric = float(text)
        if numeric.is_integer():
            return int(numeric)
        return numeric
    except ValueError:
        return text


def format_label(variable_name: str, value: Any) -> str:
    """Apply translated SAS PROC FORMAT labels, falling back to the raw value."""
    if pd.isna(value):
        return ""
    fmt_name = variable_formats.get(variable_name)
    key = normalize_format_key(value)
    if fmt_name is not None:
        mapping = format_maps.get(fmt_name, {})
        if key in mapping:
            return mapping[key]
        if str(key) in mapping:
            return mapping[str(key)]
    if isinstance(key, float) and key.is_integer():
        return str(int(key))
    return str(key)


def require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise KeyError(f"Required column(s) not found after canonicalization: {missing}")


required_input_columns = [
    "p_age5yrs_r",
    "p_sex",
    "bmicat6",
    "dem_raceeth",
    "glp_compmed",
    "glp_rx12ma",
    "glp_rx12mb",
    "glp_rx12mc",
    "glp_rx12md",
    "glp_rx12me",
    "glp_med12m",
    "glp_medrx",
    "glp_mednow",
    "p_strata_r",
    "p_psu_r",
    "weight",
    "p_poverty4_r",
    "nchs_metro",
    "dem_region",
]
require_columns(rss7_puf, required_input_columns)

# data sam; set rss.rss7_puf;
sam = rss7_puf.copy()

# *Age Categories;
# 	if p_age5yrs_r in (1,2,3,4,5) then agecat_b=1;	*18-49y;
# 	else if p_age5yrs_r in (6,7,8,9,10) then agecat_b=2; 	*40-64y;
# 	else if p_age5yrs_r in (11,12) then agecat_b=3;	 	*65y+;
# 	format agecat_b ages_b.;
p_age5yrs_r = as_numeric(sam["p_age5yrs_r"])
sam["agecat_b"] = np.nan
sam.loc[p_age5yrs_r.isin([1, 2, 3, 4, 5]), "agecat_b"] = 1
sam.loc[p_age5yrs_r.isin([6, 7, 8, 9, 10]), "agecat_b"] = 2
sam.loc[p_age5yrs_r.isin([11, 12]), "agecat_b"] = 3

# *Sex-Age Categories: MALE=1, FEMALE=2;
# 	if p_sex=1 and agecat_b=1 then sexage=1;		else if p_sex=1 and agecat_b=2 then sexage=2;	else if p_sex=1 and agecat_b=3 then sexage=3;
# 	else if p_sex=2 and agecat_b=1 then sexage=4;	else if p_sex=2 and agecat_b=2 then sexage=5;	else if p_sex=2 and agecat_b=3 then sexage=6; format sexage sexage.;
p_sex = as_numeric(sam["p_sex"])
sam["sexage"] = np.nan
sam.loc[(p_sex == 1) & (sam["agecat_b"] == 1), "sexage"] = 1
sam.loc[(p_sex == 1) & (sam["agecat_b"] == 2), "sexage"] = 2
sam.loc[(p_sex == 1) & (sam["agecat_b"] == 3), "sexage"] = 3
sam.loc[(p_sex == 2) & (sam["agecat_b"] == 1), "sexage"] = 4
sam.loc[(p_sex == 2) & (sam["agecat_b"] == 2), "sexage"] = 5
sam.loc[(p_sex == 2) & (sam["agecat_b"] == 3), "sexage"] = 6

# *BMI Categories;
# 	bmicat=bmicat6; if bmicat in (4,5) then bmicat=4;	*combine class 1 and 2 obesity;
# 	format bmicat bmis.;
sam["bmicat"] = as_numeric(sam["bmicat6"])
sam.loc[sam["bmicat"].isin([4, 5]), "bmicat"] = 4

# *Race/Ethnicity;
# 	if dem_raceeth=7 then race=1;					*white, non-Hispanic;
# 	else if dem_raceeth=3 then race=2;				*black, non-Hispanic;
# 	else if dem_raceeth=4 then race=3;				*Hispanic;
# 	else if dem_raceeth in (1,2,5,6,8) then race=4; *other or multiracial;
# 	else if dem_raceeth=. then race=5;				*missing;
# 	format race re.;
dem_raceeth = as_numeric(sam["dem_raceeth"])
sam["race"] = np.nan
sam.loc[dem_raceeth == 7, "race"] = 1
sam.loc[dem_raceeth == 3, "race"] = 2
sam.loc[dem_raceeth == 4, "race"] = 3
sam.loc[dem_raceeth.isin([1, 2, 5, 6, 8]), "race"] = 4
sam.loc[dem_raceeth.isna(), "race"] = 5

# *Compounded Med Use- Recode;
# 	if glp_compmed=1 then compounded=1;			*yes;
# 	else if glp_compmed=0 then compounded=2;	*no;
# 	else compounded=3;							*unknown, skipped, question not asked;
# 	format compounded yn.;
glp_compmed = as_numeric(sam["glp_compmed"])
sam["compounded"] = np.where(glp_compmed == 1, 1, np.where(glp_compmed == 0, 2, 3))

# *Create variable to indicate Multiple Sources of GLP-1s
#  Recreate variables because there are negative values if the question was not asked OR if the respondent skipped that option (i.e., -5 vs. no/0);
# 	if glp_rx12ma=1 then rx_a=1; else rx_a=0; 	if glp_rx12mb=1 then rx_b=1; else rx_b=0;
# 	if glp_rx12mc=1 then rx_c=1; else rx_c=0;	if glp_rx12md=1 then rx_d=1; else rx_d=0;
# 	if glp_rx12me=1 then rx_e=1; else rx_e=0;
sam["rx_a"] = np.where(as_numeric(sam["glp_rx12ma"]) == 1, 1, 0)
sam["rx_b"] = np.where(as_numeric(sam["glp_rx12mb"]) == 1, 1, 0)
sam["rx_c"] = np.where(as_numeric(sam["glp_rx12mc"]) == 1, 1, 0)
sam["rx_d"] = np.where(as_numeric(sam["glp_rx12md"]) == 1, 1, 0)
sam["rx_e"] = np.where(as_numeric(sam["glp_rx12me"]) == 1, 1, 0)

# 	source_sum = rx_a + rx_b + rx_c + rx_d + rx_e;
# 	if source_sum>1 then multiple_source=1; else if source_sum=1 then multiple_source=2;	*1=Yes multiple sources, 2=No single source type of GLP1;
# 	format multiple_source source.;
sam["source_sum"] = sam["rx_a"] + sam["rx_b"] + sam["rx_c"] + sam["rx_d"] + sam["rx_e"]
sam["multiple_source"] = np.nan
sam.loc[sam["source_sum"] > 1, "multiple_source"] = 1
sam.loc[sam["source_sum"] == 1, "multiple_source"] = 2

# *Recode variables to fit SUDAAN- if 0, they are ignored as missing;
# 	if glp_med12m=0 then glp_med12m=2; else if glp_med12m=-6 then glp_med12m=3;
# 	format glp_med12m glp.;
sam["glp_med12m"] = as_numeric(sam["glp_med12m"])
sam.loc[sam["glp_med12m"] == 0, "glp_med12m"] = 2
sam.loc[sam["glp_med12m"] == -6, "glp_med12m"] = 3

# 	if glp_medrx=0 then glp_medrx=2; else if glp_medrx=-6 then glp_medrx=3; format glp_medrx rx.;
# 	if glp_mednow=0 then glp_mednow=2; else if glp_mednow=-6 then glp_mednow=3; format glp_mednow rx.;
sam["glp_medrx"] = as_numeric(sam["glp_medrx"])
sam.loc[sam["glp_medrx"] == 0, "glp_medrx"] = 2
sam.loc[sam["glp_medrx"] == -6, "glp_medrx"] = 3

sam["glp_mednow"] = as_numeric(sam["glp_mednow"])
sam.loc[sam["glp_mednow"] == 0, "glp_mednow"] = 2
sam.loc[sam["glp_mednow"] == -6, "glp_mednow"] = 3

# run;


def valid_design_mask(df: pd.DataFrame, weight_col: str, strata_col: str, psu_col: str) -> pd.Series:
    weight = as_numeric(df[weight_col])
    return (
        weight.notna()
        & np.isfinite(weight)
        & (weight > 0)
        & df[strata_col].notna()
        & df[psu_col].notna()
    )


def design_degrees_of_freedom(
    df: pd.DataFrame,
    weight_col: str = "weight",
    strata_col: str = "p_strata_r",
    psu_col: str = "p_psu_r",
) -> tuple[int, int, int]:
    """Return design df, number of strata, and number of PSUs.

    SUDAAN DESIGN=WR commonly uses PSU minus strata degrees of freedom. The
    /MISSUNIT option is approximated by keeping single-PSU strata in the design
    df count while their within-stratum variance contribution is zero.
    """
    mask = valid_design_mask(df, weight_col, strata_col, psu_col)
    psus = df.loc[mask, [strata_col, psu_col]].drop_duplicates()
    n_psu = int(len(psus))
    n_strata = int(psus[strata_col].nunique(dropna=False))
    return max(n_psu - n_strata, 0), n_strata, n_psu


def t_critical_95(design_df: int) -> float:
    if design_df > 0:
        return float(t.ppf(0.975, design_df))
    return float(t.ppf(0.975, 1))


def taylor_variance_from_psu_linearized(
    df: pd.DataFrame,
    linearized: pd.Series,
    weight_col: str = "weight",
    strata_col: str = "p_strata_r",
    psu_col: str = "p_psu_r",
) -> float:
    """Taylor-linearization WR variance from PSU totals within strata."""
    design_mask = valid_design_mask(df, weight_col, strata_col, psu_col)
    psu_frame = df.loc[design_mask, [strata_col, psu_col]].drop_duplicates().copy()
    lin = pd.DataFrame(
        {
            strata_col: df.loc[design_mask, strata_col].to_numpy(),
            psu_col: df.loc[design_mask, psu_col].to_numpy(),
            "_lin": linearized.loc[design_mask].fillna(0.0).to_numpy(dtype=float),
        }
    )
    psu_lin = lin.groupby([strata_col, psu_col], dropna=False, observed=False)["_lin"].sum().reset_index()
    psu_lin = psu_frame.merge(psu_lin, on=[strata_col, psu_col], how="left")
    psu_lin["_lin"] = psu_lin["_lin"].fillna(0.0)

    variance = 0.0
    for _stratum, group in psu_lin.groupby(strata_col, dropna=False, observed=False):
        nh = len(group)
        if nh >= 2:
            centered = group["_lin"] - group["_lin"].mean()
            variance += float(nh / (nh - 1.0) * np.sum(centered.to_numpy(dtype=float) ** 2))
    return variance


def survey_proportion_catlevel(
    df: pd.DataFrame,
    var_col: str,
    catlevel: Any,
    denominator_mask: pd.Series,
    weight_col: str = "weight",
    strata_col: str = "p_strata_r",
    psu_col: str = "p_psu_r",
) -> dict[str, float]:
    """SUDAAN PROC DESCRIPT-like weighted percent for one categorical level.

    Point estimates use the requested denominator domain/subgroup. Variance is
    computed with Taylor-linearized PSU totals while retaining the full design
    frame, rather than naively filtering before variance estimation.
    """
    weight = as_numeric(df[weight_col])
    value = as_numeric(df[var_col])
    design_mask = valid_design_mask(df, weight_col, strata_col, psu_col)
    denominator = denominator_mask.fillna(False) & design_mask & value.notna()
    numerator = denominator & (value == normalize_format_key(catlevel))

    denominator_weight = float(weight.loc[denominator].sum())
    numerator_weight = float(weight.loc[numerator].sum())
    nsum = int(denominator.sum())
    design_df, n_strata, n_psu = design_degrees_of_freedom(df, weight_col, strata_col, psu_col)

    if denominator_weight <= 0 or nsum == 0:
        return {
            "percent": np.nan,
            "sepercent": np.nan,
            "lowpct": np.nan,
            "uppct": np.nan,
            "nsum": float(nsum),
            "wsum": denominator_weight,
            "weighted_count": numerator_weight,
            "design_df": float(design_df),
            "n_strata": float(n_strata),
            "n_psu": float(n_psu),
        }

    p_hat = numerator_weight / denominator_weight
    indicator = pd.Series(0.0, index=df.index)
    indicator.loc[numerator] = 1.0

    linearized = pd.Series(0.0, index=df.index)
    linearized.loc[denominator] = weight.loc[denominator] * (indicator.loc[denominator] - p_hat)

    var_total = taylor_variance_from_psu_linearized(df, linearized, weight_col, strata_col, psu_col)
    var_p = var_total / (denominator_weight**2)
    se_p = math.sqrt(max(var_p, 0.0)) if np.isfinite(var_p) else np.nan

    crit = t_critical_95(design_df)
    percent = 100.0 * p_hat
    sepercent = 100.0 * se_p if pd.notna(se_p) else np.nan
    lowpct = max(0.0, percent - crit * sepercent) if pd.notna(sepercent) else np.nan
    uppct = min(100.0, percent + crit * sepercent) if pd.notna(sepercent) else np.nan

    return {
        "percent": percent,
        "sepercent": sepercent,
        "lowpct": lowpct,
        "uppct": uppct,
        "nsum": float(nsum),
        "wsum": denominator_weight,
        "weighted_count": numerator_weight,
        "design_df": float(design_df),
        "n_strata": float(n_strata),
        "n_psu": float(n_psu),
    }


def proc_descript_catlevel(
    df: pd.DataFrame,
    var_col: str,
    catlevel: Any,
    subgroup_levels: dict[str, list[Any]],
    subpopn_mask: pd.Series | None,
    output_name: str,
    weight_col: str = "weight",
    strata_col: str = "p_strata_r",
    psu_col: str = "p_psu_r",
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if subpopn_mask is None:
        base_mask = pd.Series(True, index=df.index)
        subpopulation = ""
    else:
        base_mask = subpopn_mask.fillna(False)
        subpopulation = "glp_med12m=1"

    overall_stats = survey_proportion_catlevel(
        df=df,
        var_col=var_col,
        catlevel=catlevel,
        denominator_mask=base_mask,
        weight_col=weight_col,
        strata_col=strata_col,
        psu_col=psu_col,
    )
    rows.append(
        {
            "output_dataset": output_name,
            "subpopulation": subpopulation,
            "subgroup_variable": "Overall",
            "subgroup_code": pd.NA,
            "subgroup": "Overall",
            "analysis_variable": var_col,
            "catlevel_code": catlevel,
            "catlevel": format_label(var_col, catlevel),
            **overall_stats,
        }
    )

    for subgroup_var, levels in subgroup_levels.items():
        subgroup_values = as_numeric(df[subgroup_var])
        for level in levels:
            level_mask = base_mask & (subgroup_values == normalize_format_key(level))
            stats = survey_proportion_catlevel(
                df=df,
                var_col=var_col,
                catlevel=catlevel,
                denominator_mask=level_mask,
                weight_col=weight_col,
                strata_col=strata_col,
                psu_col=psu_col,
            )
            rows.append(
                {
                    "output_dataset": output_name,
                    "subpopulation": subpopulation,
                    "subgroup_variable": subgroup_var,
                    "subgroup_code": level,
                    "subgroup": format_label(subgroup_var, level),
                    "analysis_variable": var_col,
                    "catlevel_code": catlevel,
                    "catlevel": format_label(var_col, catlevel),
                    **stats,
                }
            )

    out = pd.DataFrame(rows)
    ordered_cols = [
        "output_dataset",
        "subpopulation",
        "subgroup_variable",
        "subgroup_code",
        "subgroup",
        "analysis_variable",
        "catlevel_code",
        "catlevel",
        "percent",
        "sepercent",
        "lowpct",
        "uppct",
        "nsum",
        "wsum",
        "weighted_count",
        "design_df",
        "n_strata",
        "n_psu",
    ]
    return out[ordered_cols]


# *GLP-1 Use by Select Demographics;
# proc sort data=sam; by p_strata_r p_psu_r; run;
sam = sam.sort_values(["p_strata_r", "p_psu_r"], kind="mergesort").reset_index(drop=True)

# proc descript data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	var glp_med12m; catlevel 1;
# 	subgroup agecat_b p_sex race p_poverty4_r nchs_metro dem_region; levels 3 2 5 4 2 4;
# 	print percent sepercent lowpct uppct nsum wsum/style=nchs percentfmt=F6.3 sepercentfmt=F9.2 lowpctfmt=F6.3 uppctfmt=F6.3 wsumfmt=F12.0 nohead notime nodate;
# 	output / tablecell=all filetype=sas filename=demos replace;
# run;
subgroup_levels = {
    "agecat_b": [1, 2, 3],
    "p_sex": [1, 2],
    "race": [1, 2, 3, 4, 5],
    "p_poverty4_r": [1, 2, 3, 4],
    "nchs_metro": [1, 2],
    "dem_region": [1, 2, 3, 4],
}
demos = proc_descript_catlevel(
    df=sam,
    var_col="glp_med12m",
    catlevel=1,
    subgroup_levels=subgroup_levels,
    subpopn_mask=None,
    output_name="demos",
)

# *Compounded GLP-1 Use among those taking GLP-1s in the past year, by Select Demographics;
# proc sort data=sam; by p_strata_r p_psu_r; run;
sam = sam.sort_values(["p_strata_r", "p_psu_r"], kind="mergesort").reset_index(drop=True)

# proc descript data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	subpopn glp_med12m=1;
# 	var compounded; catlevel 1;
# 	subgroup agecat_b p_sex race p_poverty4_r nchs_metro dem_region; levels 3 2 5 4 2 4;
# 	print percent sepercent lowpct uppct nsum wsum/style=nchs percentfmt=F6.3 sepercentfmt=F9.2 lowpctfmt=F6.3 uppctfmt=F6.3 wsumfmt=F12.0 nohead notime nodate;
# 	output / tablecell=all filetype=sas filename=reliability_checks_comp replace;
# run;
reliability_checks_comp = proc_descript_catlevel(
    df=sam,
    var_col="compounded",
    catlevel=1,
    subgroup_levels=subgroup_levels,
    subpopn_mask=as_numeric(sam["glp_med12m"]) == 1,
    output_name="reliability_checks_comp",
)


def survey_weighted_means_cov(
    df: pd.DataFrame,
    columns: list[str],
    domain_mask: pd.Series,
    weight_col: str = "weight",
    strata_col: str = "p_strata_r",
    psu_col: str = "p_psu_r",
) -> tuple[np.ndarray, np.ndarray, float, int]:
    """Design-based covariance of weighted means for several columns."""
    weight = as_numeric(df[weight_col])
    design_mask = valid_design_mask(df, weight_col, strata_col, psu_col)
    domain = domain_mask.fillna(False) & design_mask

    x = df[columns].apply(pd.to_numeric, errors="coerce")
    complete = domain & x.notna().all(axis=1)
    denom = float(weight.loc[complete].sum())
    design_df, _n_strata, _n_psu = design_degrees_of_freedom(df, weight_col, strata_col, psu_col)

    if denom <= 0 or not complete.any():
        return np.full(len(columns), np.nan), np.full((len(columns), len(columns)), np.nan), denom, design_df

    means = np.array(
        [(weight.loc[complete] * x.loc[complete, col]).sum() / denom for col in columns],
        dtype=float,
    )

    design_rows = df.loc[design_mask, [strata_col, psu_col]].copy()
    for idx, col in enumerate(columns):
        lin_col = f"_lin_{idx}"
        lin_values = pd.Series(0.0, index=df.index)
        lin_values.loc[complete] = weight.loc[complete] * (x.loc[complete, col] - means[idx])
        design_rows[lin_col] = lin_values.loc[design_mask].to_numpy(dtype=float)

    psu_cols = [f"_lin_{idx}" for idx in range(len(columns))]
    psu_lin = (
        design_rows.groupby([strata_col, psu_col], dropna=False, observed=False)[psu_cols]
        .sum()
        .reset_index()
    )

    covariance_total = np.zeros((len(columns), len(columns)), dtype=float)
    for _stratum, group in psu_lin.groupby(strata_col, dropna=False, observed=False):
        nh = len(group)
        if nh >= 2:
            mat = group[psu_cols].to_numpy(dtype=float)
            centered = mat - mat.mean(axis=0, keepdims=True)
            covariance_total += nh / (nh - 1.0) * centered.T @ centered

    covariance_mean = covariance_total / (denom**2)
    return means, covariance_mean, denom, design_df


def build_weighted_crosstab(
    df: pd.DataFrame,
    row_var: str,
    col_var: str,
    row_levels: list[Any],
    col_levels: list[Any],
    weight_col: str = "weight",
) -> pd.DataFrame:
    """Weighted cell counts for the current crosstab cells, not a copied grand total."""
    weight = as_numeric(df[weight_col])
    row_values = as_numeric(df[row_var])
    col_values = as_numeric(df[col_var])
    rows: list[dict[str, Any]] = []

    for row_level in row_levels:
        row_mask = row_values == normalize_format_key(row_level)
        row_total_mask = row_mask & col_values.isin([normalize_format_key(x) for x in col_levels])
        row_wsum = float(weight.loc[row_total_mask & weight.notna() & (weight > 0)].sum())
        for col_level in col_levels:
            cell_mask = (
                row_mask
                & (col_values == normalize_format_key(col_level))
                & weight.notna()
                & (weight > 0)
            )
            cell_wsum = float(weight.loc[cell_mask].sum())
            unweighted_n = int(cell_mask.sum())
            row_percent = 100.0 * cell_wsum / row_wsum if row_wsum > 0 else np.nan
            rows.append(
                {
                    "row_variable": row_var,
                    "row_code": row_level,
                    "row_label": format_label(row_var, row_level),
                    "column_variable": col_var,
                    "column_code": col_level,
                    "column_label": format_label(col_var, col_level),
                    "nsum": unweighted_n,
                    "wsum": cell_wsum,
                    "row_wsum": row_wsum,
                    "row_percent": row_percent,
                }
            )

    return pd.DataFrame(rows)


def numerical_jacobian(function: Callable[[np.ndarray], np.ndarray], x: np.ndarray) -> np.ndarray:
    """Deterministic central-difference Jacobian for smooth table functions."""
    x = np.asarray(x, dtype=float)
    base = function(x)
    jac = np.zeros((len(base), len(x)), dtype=float)
    for idx in range(len(x)):
        step = 1e-6 * max(1.0, abs(x[idx]))
        x_plus = x.copy()
        x_minus = x.copy()
        x_plus[idx] += step
        x_minus[idx] -= step
        jac[:, idx] = (function(x_plus) - function(x_minus)) / (2.0 * step)
    return jac


def independence_contrasts(p_flat: np.ndarray, n_rows: int, n_cols: int) -> np.ndarray:
    """Independence contrasts for all cells except the last row/column."""
    p = np.asarray(p_flat, dtype=float).reshape((n_rows, n_cols))
    row_margins = p.sum(axis=1)
    col_margins = p.sum(axis=0)
    contrasts: list[float] = []
    for i in range(n_rows - 1):
        for j in range(n_cols - 1):
            contrasts.append(float(p[i, j] - row_margins[i] * col_margins[j]))
    return np.array(contrasts, dtype=float)


def survey_crosstab_tests(
    df: pd.DataFrame,
    row_var: str,
    col_var: str,
    row_levels: list[Any],
    col_levels: list[Any],
    weight_col: str = "weight",
    strata_col: str = "p_strata_r",
    psu_col: str = "p_psu_r",
) -> pd.DataFrame:
    """SUDAAN-like CHISQ and ACMH tests using design-based covariance.

    CHISQ is reported as an adjusted Wald F-style statistic: W / q with q
    numerator df and PSU-minus-strata denominator df. ACMH is a distinct
    design-based linear-by-linear trend test using ordered row/column scores.
    This avoids raw Pearson chi-square statistics from weighted population
    totals, which are not comparable to SUDAAN survey tests.
    """
    row_values = as_numeric(df[row_var])
    col_values = as_numeric(df[col_var])
    row_level_numeric = [normalize_format_key(x) for x in row_levels]
    col_level_numeric = [normalize_format_key(x) for x in col_levels]
    table_domain = row_values.isin(row_level_numeric) & col_values.isin(col_level_numeric)

    cell_columns: list[str] = []
    work = df.copy()
    for i, row_level in enumerate(row_level_numeric):
        for j, col_level in enumerate(col_level_numeric):
            cell_col = f"_cell_{i}_{j}"
            work[cell_col] = ((row_values == row_level) & (col_values == col_level)).astype(float)
            cell_columns.append(cell_col)

    p_hat, cov_p, denom, design_df = survey_weighted_means_cov(
        work,
        cell_columns,
        table_domain,
        weight_col=weight_col,
        strata_col=strata_col,
        psu_col=psu_col,
    )

    rows: list[dict[str, Any]] = []
    nominal_df = (len(row_levels) - 1) * (len(col_levels) - 1)

    if denom > 0 and np.all(np.isfinite(p_hat)) and np.all(np.isfinite(cov_p)) and nominal_df > 0:
        h_hat = independence_contrasts(p_hat, len(row_levels), len(col_levels))
        jac = numerical_jacobian(lambda p: independence_contrasts(p, len(row_levels), len(col_levels)), p_hat)
        cov_h = jac @ cov_p @ jac.T
        rank = int(np.linalg.matrix_rank(cov_h, tol=1e-12))
        q = min(nominal_df, rank) if rank > 0 else nominal_df
        if q > 0:
            cov_h_inv = np.linalg.pinv(cov_h)
            wald_chisq = float(h_hat.T @ cov_h_inv @ h_hat)
            stestval = wald_chisq / q
            p_value = float(f.sf(stestval, q, design_df)) if design_df > 0 else np.nan
        else:
            wald_chisq = np.nan
            stestval = np.nan
            p_value = np.nan
            q = nominal_df
    else:
        wald_chisq = np.nan
        stestval = np.nan
        p_value = np.nan
        q = nominal_df

    rows.append(
        {
            "test": "CHISQ",
            "stestval": stestval,
            "wald_chisq": wald_chisq,
            "sdf": nominal_df,
            "num_df": q,
            "den_df": design_df,
            "p_value": p_value,
            "note": "SUDAAN-like adjusted Wald F test from weighted cell proportions and Taylor PSU covariance.",
        }
    )

    row_score_map = {level: idx + 1.0 for idx, level in enumerate(row_level_numeric)}
    col_score_map = {level: idx + 1.0 for idx, level in enumerate(col_level_numeric)}
    work["_row_score"] = row_values.map(row_score_map)
    work["_col_score"] = col_values.map(col_score_map)
    work["_row_col_score"] = work["_row_score"] * work["_col_score"]

    means, cov_means, _denom_scores, design_df_scores = survey_weighted_means_cov(
        work,
        ["_row_score", "_col_score", "_row_col_score"],
        table_domain,
        weight_col=weight_col,
        strata_col=strata_col,
        psu_col=psu_col,
    )

    if np.all(np.isfinite(means)) and np.all(np.isfinite(cov_means)):
        mean_row, mean_col, mean_row_col = means
        theta = float(mean_row_col - mean_row * mean_col)
        grad = np.array([-mean_col, -mean_row, 1.0], dtype=float)
        var_theta = float(grad.T @ cov_means @ grad)
        if var_theta > 0:
            acmh_f = float((theta**2) / var_theta)
            acmh_p = float(f.sf(acmh_f, 1, design_df_scores)) if design_df_scores > 0 else np.nan
        else:
            acmh_f = np.nan
            acmh_p = np.nan
    else:
        theta = np.nan
        acmh_f = np.nan
        acmh_p = np.nan

    rows.append(
        {
            "test": "ACMH",
            "stestval": acmh_f,
            "wald_chisq": acmh_f,
            "sdf": 1,
            "num_df": 1,
            "den_df": design_df_scores,
            "p_value": acmh_p,
            "note": "Design-based linear-by-linear ordered score trend test; not reused from CHISQ.",
        }
    )

    return pd.DataFrame(rows)


# *Chi-square test for difference in GLP-1 use by age category;
# proc sort data=sam; by p_strata_r p_psu_r; run;
sam = sam.sort_values(["p_strata_r", "p_psu_r"], kind="mergesort").reset_index(drop=True)

# proc crosstab data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	subgroup agecat_b glp_med12m;
# 	tables agecat_b*glp_med12m; levels 3 3;
# 	test chisq acmh;
# run;
crosstab_age_glp = build_weighted_crosstab(
    df=sam,
    row_var="agecat_b",
    col_var="glp_med12m",
    row_levels=[1, 2, 3],
    col_levels=[1, 2, 3],
)
crosstab_tests = survey_crosstab_tests(
    df=sam,
    row_var="agecat_b",
    col_var="glp_med12m",
    row_levels=[1, 2, 3],
    col_levels=[1, 2, 3],
)


def rounded_for_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["percent", "lowpct", "uppct", "row_percent"]:
        if col in out.columns:
            out[col] = out[col].astype(float).round(3)
    if "sepercent" in out.columns:
        out["sepercent"] = out["sepercent"].astype(float).round(2)
    if "wsum" in out.columns:
        out["wsum"] = out["wsum"].astype(float).round(0)
    if "weighted_count" in out.columns:
        out["weighted_count"] = out["weighted_count"].astype(float).round(0)
    if "row_wsum" in out.columns:
        out["row_wsum"] = out["row_wsum"].astype(float).round(0)
    return out


demos_display = rounded_for_display(demos)
reliability_checks_comp_display = rounded_for_display(reliability_checks_comp)
crosstab_age_glp_display = rounded_for_display(crosstab_age_glp)
crosstab_tests_display = crosstab_tests.copy()
for numeric_col in ["stestval", "wald_chisq", "p_value"]:
    if numeric_col in crosstab_tests_display.columns:
        crosstab_tests_display[numeric_col] = crosstab_tests_display[numeric_col].astype(float).round(6)

with pd.ExcelWriter(OUTPUT_WORKBOOK, engine="openpyxl") as writer:
    contents.to_excel(writer, sheet_name=safe_sheet_name("contents"), index=False)
    demos_display.to_excel(writer, sheet_name=safe_sheet_name("demos"), index=False)
    reliability_checks_comp_display.to_excel(
        writer,
        sheet_name=safe_sheet_name("reliability_checks_comp"),
        index=False,
    )
    crosstab_age_glp_display.to_excel(writer, sheet_name=safe_sheet_name("age_glp_crosstab"), index=False)
    crosstab_tests_display.to_excel(writer, sheet_name=safe_sheet_name("age_glp_tests"), index=False)

print(f"Wrote Excel workbook: {OUTPUT_WORKBOOK}")
print("\nDemos:")
print(demos_display.to_string(index=False))
print("\nReliability checks - compounded:")
print(reliability_checks_comp_display.to_string(index=False))
print("\nAge category by GLP-1 past-year use crosstab:")
print(crosstab_age_glp_display.to_string(index=False))
print("\nSurvey-adjusted crosstab tests:")
print(crosstab_tests_display.to_string(index=False))