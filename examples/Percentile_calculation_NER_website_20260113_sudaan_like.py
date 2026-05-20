"""
/********
File:		Conf_Int_Exp.sas

Purpose:	Replicate Confidence Intervals in Exposure Report
                for 2013-2014 Total Blood Mercury 95th percentile
                estimates for 6-11, 12-19, and 20+ age groups,
                males and females, and various racial groups;

Date:		22 APR 05

Date Revised:	26 APR 05
		05 MAY 05
                27 OCT 08
                23 JUN 11
                02 FEB 17
		31 JAN 25

Note: This dataset is compliant with Executive Order 14168, titled "Defending Women from Gender Ideology Extremism and Restoring Biological Truth to the Federal Government".

Input Datasets:	PbCd_h and demo_h

Programmer:	Lisa Mirel / Sam Caudill / Wellington Onyenwe
*************************************************/
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import beta, t


# options nomprint nosymbolgen nomlogic nonotes nosource nosource2;

# ***bring in datasets;

# LIBNAME l06  XPORT 'C:\Users\ewn9\OneDrive - CDC\+My_Large_Workspace\NHANES project\PbCd_h.xpt';
# Libname demo xport 'C:\Users\ewn9\OneDrive - CDC\+My_Large_Workspace\NHANES project\demo_h.xpt';

# Use only filenames / relative paths instead of absolute local machine paths.
PBCD_XPT_PATH = "PbCd_h.xpt"
DEMO_XPT_PATH = "demo_h.xpt"


# ***Note: default method for CI of percentile in SUDAAN uses the Logit;

def round_sas(series: pd.Series, unit: float) -> pd.Series:
    return np.round(series / unit) * unit


def weighted_percentile(values, weights, percentile):
    """Approximate SAS PROC UNIVARIATE percentile with a FREQ variable.

    The SAS code uses PROC UNIVARIATE with FREQ wt_orig_rnd / wt_mean_rnd.
    FREQ behaves like an integer replication count, not like a continuous
    sampling weight. The default SAS percentile definition is PCTLDEF=5.
    This function avoids physically expanding millions of weighted rows.
    """
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)

    mask = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
    values = values[mask]
    # SAS ROUND(weight, 1) has already been applied upstream. Treat FREQ as integer.
    freqs = np.rint(weights[mask]).astype(np.int64)
    keep = freqs > 0
    values = values[keep]
    freqs = freqs[keep]

    if values.size == 0:
        return np.nan, 0

    order = np.argsort(values, kind="mergesort")
    values = values[order]
    freqs = freqs[order]
    cum_freq = np.cumsum(freqs)
    n_freq = int(cum_freq[-1])

    def kth_value(k):
        # k is 1-based position in the frequency-expanded data.
        k = max(1, min(int(k), n_freq))
        return values[np.searchsorted(cum_freq, k, side="left")]

    p = float(percentile) / 100.0
    np_index = n_freq * p
    j = int(np.floor(np_index))
    g = np_index - j

    if j <= 0:
        q = kth_value(1)
    elif g == 0:
        q = (kth_value(j) + kth_value(j + 1)) / 2.0
    else:
        q = kth_value(j + 1)

    # Return the unweighted valid observation count, matching PROC UNIVARIATE N= output.
    return q, int(values.size)


def read_xpt_fallback(path: Path) -> pd.DataFrame:
    df = pd.read_sas(path, format="xport", encoding="utf-8")
    # SAS variable names are not case-sensitive, but pandas column names are.
    # Normalize to lowercase so the translated SAS code can use lower-case names.
    df.columns = df.columns.str.lower()
    return df


# data lab6;
# 	set  l06.PbCd_h;
# 	run;
# data demo;
# 	set  demo.demo_h;
# 	run;

lab6 = read_xpt_fallback(PBCD_XPT_PATH).copy()
demo = read_xpt_fallback(DEMO_XPT_PATH).copy()

# proc sort data=lab6;
# 	by seqn;
# proc sort data=demo;
# 	by seqn;
lab6 = lab6.sort_values("seqn", kind="mergesort").reset_index(drop=True)
demo = demo.sort_values("seqn", kind="mergesort").reset_index(drop=True)

# data l6dem;
# 	merge lab6 (in=a) demo (in=b);
# 	by seqn;
# 	if a=1;
l6dem = lab6.merge(demo, on="seqn", how="left", sort=False, suffixes=("", "_demo"))

# *Define Age Groups;

l6dem["age_grp"] = np.nan
l6dem.loc[(l6dem["ridageyr"] >= 1) & (l6dem["ridageyr"] <= 5), "age_grp"] = 1
l6dem.loc[(l6dem["ridageyr"] >= 6) & (l6dem["ridageyr"] <= 11), "age_grp"] = 2
l6dem.loc[(l6dem["ridageyr"] >= 12) & (l6dem["ridageyr"] <= 19), "age_grp"] = 3
l6dem.loc[l6dem["ridageyr"] >= 20, "age_grp"] = 4
l6dem.loc[l6dem["ridageyr"] >= 1, "age_group"] = 1

l6dem["sex"] = np.nan
l6dem.loc[l6dem["riagendr"] == 1, "sex"] = 1
l6dem.loc[l6dem["riagendr"] == 2, "sex"] = 2

l6dem["race"] = np.nan
l6dem.loc[l6dem["ridreth3"] == 1, "race"] = 1  # 'MA';
# if ridreth3 eq 2 then race = 2; #'OH'; *An estimate for this category by itself is not calculated.  Instead OH is combined with MA to create AH;
l6dem.loc[l6dem["ridreth3"] == 3, "race"] = 2  # 'NHW';
l6dem.loc[l6dem["ridreth3"] == 4, "race"] = 3  # 'NHB';
l6dem.loc[l6dem["ridreth3"] == 6, "race"] = 4  # 'NHA';
l6dem.loc[l6dem["ridreth3"] == 7, "race"] = 5  # Non-Hispanic Multi-racial;
l6dem["racial"] = 2
l6dem.loc[l6dem["ridreth3"].isin([1, 2]), "racial"] = 1  # 'AH';

# 	run;

# ******
#
# 	Macro for calculating totals -- following steps in
# 	Appendix A Fourth Exposure Report:
# 	Confidence Interval Estimation for Percentiles
#
# ******;


def _weighted_mean_and_se_by_psu(df, value_col, weight_col, strata_col, psu_col):
    """Taylor-series WR variance for a weighted mean/proportion.

    This is closer to SUDAAN PROC DESCRIPT DESIGN=WR than taking the
    variance of PSU-level means.  For a weighted mean p = sum(w*y)/sum(w),
    the PSU linearized contribution is sum(w*(y-p)).  Variance is then
    accumulated within strata and divided by total_weight**2.
    """
    work = df[[value_col, weight_col, strata_col, psu_col]].copy()
    work = work.dropna(subset=[value_col, weight_col, strata_col, psu_col])
    work = work[work[weight_col] > 0]

    if work.empty:
        return {
            "NSUM": 0.0,
            "MEAN": np.nan,
            "SEMEAN": np.nan,
            "DEFFMEAN": np.nan,
            "atlev1": 0.0,
            "atlev2": 0.0,
        }

    nsum = float(len(work))
    total_w = float(work[weight_col].sum())
    mean = float((work[value_col] * work[weight_col]).sum() / total_w) if total_w > 0 else np.nan

    work = work.assign(_lin=work[weight_col] * (work[value_col] - mean))
    psu_lin = (
        work.groupby([strata_col, psu_col], dropna=False)["_lin"]
        .sum()
        .reset_index(name="_psu_lin")
    )

    var_total = 0.0
    for _, g in psu_lin.groupby(strata_col, dropna=False):
        nh = len(g)
        if nh >= 2:
            centered = g["_psu_lin"] - g["_psu_lin"].mean()
            var_total += (nh / (nh - 1.0)) * float((centered ** 2).sum())

    var_mean = var_total / (total_w ** 2) if total_w > 0 else np.nan
    semean = np.sqrt(var_mean) if pd.notna(var_mean) and var_mean >= 0 else np.nan

    # SUDAAN DEFF #4 is close to Taylor variance divided by SRSWR variance.
    # Use Kish effective n for the weighted SRS comparator.
    neff_kish = (total_w ** 2) / float((work[weight_col] ** 2).sum()) if (work[weight_col] ** 2).sum() > 0 else np.nan
    srs_var = mean * (1.0 - mean) / neff_kish if pd.notna(neff_kish) and neff_kish > 0 else np.nan
    deffmean = var_mean / srs_var if pd.notna(srs_var) and srs_var > 0 else np.nan

    atlev1 = float(psu_lin[strata_col].nunique())
    atlev2 = float(len(psu_lin))

    return {
        "NSUM": nsum,
        "MEAN": mean,
        "SEMEAN": semean,
        "DEFFMEAN": deffmean,
        "atlev1": atlev1,
        "atlev2": atlev2,
    }

def pcntci(var1, var2, var3, var4, var5, var6, var7, L6dem):
    # data L6dem2;
    # 	set L6dem;
    # 	mvar=1;
    #     anal_var_orig = &var6;
    #     wt_orig = &var7;
    #     wt_orig_rnd = round(wt_orig,1);
    # 	run;
    L6dem2 = L6dem.copy()
    L6dem2["mvar"] = 1
    L6dem2["anal_var_orig"] = L6dem2[var6]
    L6dem2["wt_orig"] = L6dem2[var7]
    L6dem2["wt_orig_rnd"] = round_sas(L6dem2["wt_orig"], 1)

    def domain_mask(df):
        return (df[var1] == var2) & (df["sddsrvyr"] == var3)


    # ***Step 1a;
    p_step1a, wtn_step1a = weighted_percentile(
        L6dem2.loc[domain_mask(L6dem2), "anal_var_orig"],
        L6dem2.loc[domain_mask(L6dem2), "wt_orig_rnd"],
        var4,
    )

    # data xpercent1a;
    # 	set xpercenta;
    # 	mvar=1;
    # 	RUN;
    xpercent1a = pd.DataFrame({"mvar": [1], f"P_{var4}": [p_step1a], "wtn": [wtn_step1a]})

    # data xchperc1a;
    # 	merge l6dem2 (in=a) xpercent1a (in=b);
    # 	by mvar;
    # 	run;
    xchperc1a = L6dem2.merge(xpercent1a, on="mvar", how="left", sort=False)

    # ***Step 1b;
    # output out=xwt_mean mean=wt_mean; *wt_mean is now the mean weight of all
    #                                    subjects in the same domain/subsample
    #                                    with the same measured result;
    domain_subset = L6dem2.loc[domain_mask(L6dem2), ["anal_var_orig", "wt_orig"]].copy()
    xwt_mean = (
        domain_subset.groupby("anal_var_orig", dropna=False)["wt_orig"]
        .mean()
        .reset_index(name="wt_mean")
    )

    # data xl6dem2;
    #  merge xwt_mean l6dem2;
    #   by anal_var_orig;
    #     wt_mean_rnd = round(wt_mean,1);
    # run;
    xl6dem2 = L6dem2.merge(xwt_mean, on="anal_var_orig", how="left", sort=False)
    xl6dem2["wt_mean_rnd"] = round_sas(xl6dem2["wt_mean"], 1)

    # data xxl6dem2;
    #  set xl6dem2;
    #   by anal_var_orig;
    #
    #  if first.anal_var_orig then do;
    #   num = 1;
    #  end;
    #  else do;
    #   num + 1;
    #  end;
    #    anal_var_incr = anal_var_orig + num/1000000000;
    #   run;
    xxl6dem2 = xl6dem2.sort_values("anal_var_orig", kind="mergesort").copy()
    xxl6dem2["num"] = xxl6dem2.groupby("anal_var_orig", dropna=False).cumcount() + 1
    xxl6dem2["anal_var_incr"] = xxl6dem2["anal_var_orig"] + xxl6dem2["num"] / 1000000000.0

    p_step1b, wtn_step1b = weighted_percentile(
        xxl6dem2.loc[domain_mask(xxl6dem2), "anal_var_incr"],
        xxl6dem2.loc[domain_mask(xxl6dem2), "wt_mean_rnd"],
        var4,
    )
    xpercent1b = pd.DataFrame({"mvar": [1], f"P_{var4}": [p_step1b], "wtn": [wtn_step1b]})
    xchperc1b = xxl6dem2.merge(xpercent1b, on="mvar", how="left", sort=False)

    # ***Step 2a;
    xchperc2a = xchperc1a.copy()
    pcol = f"P_{var4}"
    xchperc2a["ind2"] = np.where(
        domain_mask(xchperc2a) & (xchperc2a["anal_var_orig"] >= 0) & (xchperc2a["anal_var_orig"] < xchperc2a[pcol]),
        1,
        np.where(
            domain_mask(xchperc2a) & (xchperc2a["anal_var_orig"] >= xchperc2a[pcol]),
            0,
            np.nan,
        ),
    )

    #   weight wt_orig;                  *BE SURE TO USE THE PROPER WEIGHT;
    stats2a = _weighted_mean_and_se_by_psu(
        xchperc2a.loc[domain_mask(xchperc2a)].copy(),
        "ind2",
        "wt_orig",
        "sdmvstra",
        "sdmvpsu",
    )

    # DATA xpest2a;
    #   SET xest1;
    #   if _N_=1;
    #   mvar = 1;
    #   semean_orig = semean;
    # deffmean = max(1,deffmean);
    #   deffmean_orig = deffmean;
    #   drop nsum mean semean geomean segeomean deffmean atlev2 atlev1;
    # run;
    xpest2a = pd.DataFrame(
        {
            "mvar": [1],
            "semean_orig": [stats2a["SEMEAN"]],
            "deffmean_orig": [max(1, stats2a["DEFFMEAN"]) if pd.notna(stats2a["DEFFMEAN"]) else np.nan],
            "NSUM": [stats2a["NSUM"]],
            "MEAN": [stats2a["MEAN"]],
        }
    )

    # ***Step 2b;
    xchperc2b = xchperc1b.copy()
    xchperc2b["ind2"] = np.where(
        domain_mask(xchperc2b) & (xchperc2b["anal_var_incr"] >= 0) & (xchperc2b["anal_var_incr"] < xchperc2b[pcol]),
        1,
        np.where(
            domain_mask(xchperc2b) & (xchperc2b["anal_var_incr"] >= xchperc2b[pcol]),
            0,
            np.nan,
        ),
    )

    #   weight wt_mean;                  *BE SURE TO USE THE PROPER WEIGHT;
    stats2b = _weighted_mean_and_se_by_psu(
        xchperc2b.loc[domain_mask(xchperc2b)].copy(),
        "ind2",
        "wt_mean",
        "sdmvstra",
        "sdmvpsu",
    )

    # DATA xpest2b;
    #   SET xest2;
    #   if _N_=1;
    #   mvar = 1;
    #   ddf=atlev2-atlev1;
    # run;
    xpest2b = pd.DataFrame({"mvar": [1], "ddf": [stats2b["atlev2"] - stats2b["atlev1"]]})

    # ***Step 3;

    # ******************************************************************;
    # *The forumlas of Korn et al are used to estimate the proportion of
    #  subjects below the selected percentile-- from Sam Caudill code
    # ******************************************************************;
    xtest = xpest2a.merge(xpest2b, on="mvar").merge(xpercent1b, on="mvar")
    xtest["N_ACT"] = xtest["NSUM"]  # *ACTUAL SAMPLE SIZE;
    xtest["PT"] = xtest["MEAN"]  # *SUDAAN WEIGHTED MEAN PROPORTION;

    def _safe_tinv(prob, df_):
        if pd.isna(df_) or df_ <= 0:
            return np.nan
        return t.ppf(prob, df_)

    xtest["T_NUM"] = xtest["NSUM"].apply(lambda x: _safe_tinv(0.975, x - 1))
    xtest["T_DEN"] = xtest["ddf"].apply(lambda x: _safe_tinv(0.975, x))
    xtest["N1"] = ((xtest["T_NUM"] / xtest["T_DEN"]) ** 2) * xtest["N_ACT"] / xtest["deffmean_orig"]  # *EFFECTIVE SAMPLE SIZE - SAM METHOD;
    xtest["N"] = ((xtest["T_NUM"] / xtest["T_DEN"]) ** 2) * xtest["MEAN"] * (1 - xtest["MEAN"]) / (xtest["semean_orig"] ** 2)  # *EFFECTIVE SAMPLE SIZE DUE TO;
    #                                        *COMPLEX STRATIFIED SAMPLING - KORN METHOD;
    xtest.loc[xtest["N"].isna(), "N"] = xtest["N1"]
    xtest.loc[xtest["N"] > xtest["NSUM"], "N"] = xtest["NSUM"]
    xtest.loc[xtest["MEAN"] == 0.0, "N"] = xtest["NSUM"]
    xtest["NA"] = xtest["MEAN"] * xtest["N"]  # *EFFECTIVE NUMBER OF SUBJECTS;

    # DATA xCYTO;
    #   SET xtest;
    #   V1 = 2*NA;
    #   V2 = 2*(N - NA + 1);
    #   V3 = 2*(NA + 1);
    #   V4 = 2*(N - NA);
    #   PL = V1*FINV(0.025,V1,V2)/(V2 + V1*FINV(0.025,V1,V2));
    #   PU = V3*FINV(0.975,V3,V4)/(V4 + V3*FINV(0.975,V3,V4));
    #   PT = PT*100;
    #   N_EFF = N;
    # RUN;
    xCYTO = xtest.copy()
    xCYTO["V1"] = 2 * xCYTO["NA"]
    xCYTO["V2"] = 2 * (xCYTO["N"] - xCYTO["NA"] + 1)
    xCYTO["V3"] = 2 * (xCYTO["NA"] + 1)
    xCYTO["V4"] = 2 * (xCYTO["N"] - xCYTO["NA"])

    def _pl(row):
        if any(pd.isna([row["V1"], row["V2"]])) or row["V1"] <= 0 or row["V2"] <= 0:
            return np.nan
        return beta.ppf(0.025, row["V1"] / 2.0, row["V2"] / 2.0)

    def _pu(row):
        if any(pd.isna([row["V3"], row["V4"]])) or row["V3"] <= 0 or row["V4"] <= 0:
            return np.nan
        return beta.ppf(0.975, row["V3"] / 2.0, row["V4"] / 2.0)

    xCYTO["PL"] = xCYTO.apply(_pl, axis=1)
    xCYTO["PU"] = xCYTO.apply(_pu, axis=1)
    xCYTO["PT"] = xCYTO["PT"] * 100
    xCYTO["N_EFF"] = xCYTO["N"]

    # DATA xxCYTO;
    #   SET xCYTO;
    # L95 = PL*100;
    # U95 = PU*100;
    #
    # IF L95 EQ . THEN L95 = 0.0;
    # IF U95 EQ . THEN U95 = 100.0;
    xxCYTO = xCYTO.copy()
    xxCYTO["L95"] = xxCYTO["PL"] * 100
    xxCYTO["U95"] = xxCYTO["PU"] * 100

    xxCYTO.loc[xxCYTO["L95"].isna(), "L95"] = 0.0
    xxCYTO.loc[xxCYTO["U95"].isna(), "U95"] = 100.0

    #   %IF &var4 EQ 10 %THEN %DO; PT = 10.0 ;  %END;
    #   %IF &var4 EQ 25 %THEN %DO; PT = 25.0 ;  %END;
    #   %IF &var4 EQ 50 %THEN %DO; PT = 50.0 ;  %END;
    #   %IF &var4 EQ 75 %THEN %DO; PT = 75.0 ;  %END;
    #   %IF &var4 EQ 90 %THEN %DO; PT = 90.0 ;  %END;
    #   %IF &var4 EQ 95 %THEN %DO; PT = 95.0 ;  %END;
    if var4 in {10, 25, 50, 75, 90, 95}:
        xxCYTO["PT"] = float(var4)

    xxCYTO.loc[xxCYTO["L95"] > xxCYTO["PT"], "L95"] = xxCYTO["PT"]
    xxCYTO.loc[xxCYTO["U95"] < xxCYTO["PT"], "U95"] = xxCYTO["PT"]

    #   title3 "PERCENTILE (WITH CIs)";

    # ***Step 4;
    xXtest = xxCYTO.copy()
    # SAS uses CALL SYMPUT('L95', LEFT(PUT(L95,8.1))) etc., so these
    # percentile points are rounded to one decimal before Step 4.
    L95_macro = round(float(xXtest["L95"].iloc[0]), 1)
    U95_macro = round(float(xXtest["U95"].iloc[0]), 1)
    MEAN_macro = round(float(xXtest["PT"].iloc[0]), 1)
    xXtest["mvar"] = 1

    # data xcomp;
    # 	merge XXtest (in=a) xxl6dem2 (in=b);
    # 	by mvar;
    # 	if b=1;
    # 	run;
    xcomp = xxl6dem2.merge(xXtest[["mvar"]], on="mvar", how="left", sort=False)

    # proc univariate data = xcomp;
    # 	var anal_var_incr;
    # 	where &var1=&var2 and sddsrvyr=&var3;
    #    	output out=xxEST pctlpre=P_ pctlpts=&L95 &U95 &MEAN
    # 	PCTLPRE = A
    #   	PCTLNAME = L95 U95 MEAN ;
    #   	freq wt_mean_rnd;
    #    run;
    p_l95, _ = weighted_percentile(
        xcomp.loc[domain_mask(xcomp), "anal_var_incr"],
        xcomp.loc[domain_mask(xcomp), "wt_mean_rnd"],
        L95_macro,
    )
    p_u95, _ = weighted_percentile(
        xcomp.loc[domain_mask(xcomp), "anal_var_incr"],
        xcomp.loc[domain_mask(xcomp), "wt_mean_rnd"],
        U95_macro,
    )
    p_mean, _ = weighted_percentile(
        xcomp.loc[domain_mask(xcomp), "anal_var_incr"],
        xcomp.loc[domain_mask(xcomp), "wt_mean_rnd"],
        MEAN_macro,
    )

    xxEST = pd.DataFrame({"P_L95": [p_l95], "P_U95": [p_u95], "P_MEAN": [p_mean]})

    # proc print data=xxEST;
    # print(xxEST)

    # *creates multiple final dataset to be put in a final large dataset with all
    # datapoints;
    fin = xxEST.copy()

    Population_Group = None
    if var1 == "age_grp" and var2 == 1:
        Population_Group = "Age:   1-5"
    if var1 == "age_grp" and var2 == 2:
        Population_Group = "Age:  6-11"
    if var1 == "age_grp" and var2 == 3:
        Population_Group = "Age: 12-19"
    if var1 == "age_grp" and var2 == 4:
        Population_Group = "Age: 20+  "
    if var1 == "age_group" and var2 == 1:
        Population_Group = "Age: ALL"
    if var1 == "sex" and var2 == 1:
        Population_Group = "MALE  "
    if var1 == "sex" and var2 == 2:
        Population_Group = "FEMALE"
    if var1 == "race" and var2 == 1:
        Population_Group = "MA    "
    if var1 == "race" and var2 == 2:
        Population_Group = "NHW   "
    if var1 == "race" and var2 == 3:
        Population_Group = "NHB   "
    if var1 == "race" and var2 == 4:
        Population_Group = "NHA   "
    if var1 == "racial" and var2 == 1:
        Population_Group = "AH    "

    fin["Population_Group"] = Population_Group
    fin["yr"] = var3
    fin["per"] = var4
    fin["anly"] = var6
    fin["P_MEAN"] = round_sas(fin["P_MEAN"], 0.01)
    fin["P_L95"] = round_sas(fin["P_L95"], 0.01)
    fin["P_U95"] = round_sas(fin["P_U95"], 0.01)

    def _sas_num_to_str(x):
        if pd.isna(x):
            return ""
        return f"{x:g}"

    fin["perci"] = (
        fin["P_MEAN"].map(_sas_num_to_str)
        + "("
        + fin["P_L95"].map(_sas_num_to_str)
        + "-"
        + fin["P_U95"].map(_sas_num_to_str)
        + ")"
    )
    fin["N_ACT"] = xtest["N_ACT"].iloc[0]
    fin = fin[["anly", "Population_Group", "yr", "per", "perci", "N_ACT"]]

    # proc print data=fin&var5 noobs;
    # var  Population_Group yr per perci N_ACT;
    # run;
    print(fin[["Population_Group", "yr", "per", "perci", "N_ACT"]].to_string(index=False))

    return fin


# **			Variable definitions used in Macro
#
# VAR1 = SubPopulation Variable Name
# VAR2 = SubPopulation Value to Select
# VAR3 = SURVEY YEAR
# VAR4 = PERCENTILE
# VAR5 = INDICATES DATASET NUMBER for Concatination of All Data Sets
# VAR6 = ANALYTE OF INTEREST
# VAR7 = WEIGHT VARIABLE;
#
# ***NOTE: CAN ADD MORE MACRO VARIABLES IN CODE TO SELECT ON SEX AND RACE/ETHNICTY;

fin1 = pcntci("age_grp", 1, 8, 95, 1, "lbxthg", "wtsh2yr", l6dem)
fin2 = pcntci("age_grp", 2, 8, 95, 2, "lbxthg", "wtsh2yr", l6dem)
fin3 = pcntci("age_grp", 3, 8, 95, 3, "lbxthg", "wtsh2yr", l6dem)
fin4 = pcntci("age_grp", 4, 8, 95, 4, "lbxthg", "wtsh2yr", l6dem)
fin5 = pcntci("age_group", 1, 8, 95, 5, "lbxthg", "wtsh2yr", l6dem)

fin6 = pcntci("sex", 1, 8, 95, 6, "lbxthg", "wtsh2yr", l6dem)
fin7 = pcntci("sex", 2, 8, 95, 7, "lbxthg", "wtsh2yr", l6dem)

fin8 = pcntci("race", 1, 8, 95, 8, "lbxthg", "wtsh2yr", l6dem)
fin9 = pcntci("race", 2, 8, 95, 9, "lbxthg", "wtsh2yr", l6dem)
fin10 = pcntci("race", 3, 8, 95, 10, "lbxthg", "wtsh2yr", l6dem)
fin11 = pcntci("race", 4, 8, 95, 11, "lbxthg", "wtsh2yr", l6dem)
fin12 = pcntci("racial", 1, 8, 95, 12, "lbxthg", "wtsh2yr", l6dem)

# ***CREATE ONE LARGE DATASET USING ALL DATASETS CREATED IN THE MACRO;
# options nocenter;
# data allperc;
# 	set fin1-fin12;
# 	run;
allperc = pd.concat(
    [fin1, fin2, fin3, fin4, fin5, fin6, fin7, fin8, fin9, fin10, fin11, fin12],
    ignore_index=True,
)

# proc print data=allperc;
# run;
print(allperc)
allperc.to_csv(Path(__file__).with_name("allperc_python_sudaan_like.csv"), index=False)

# Quit;