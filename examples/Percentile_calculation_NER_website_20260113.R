# /********
# File:		Conf_Int_Exp.sas
#
# Purpose:	Replicate Confidence Intervals in Exposure Report
#                 for 2013-2014 Total Blood Mercury 95th percentile
#                 estimates for 6-11, 12-19, and 20+ age groups,
#                 males and females, and various racial groups;
#
# Date:		22 APR 05
#
# Date Revised:	26 APR 05
# 		05 MAY 05
#                 27 OCT 08
#                 23 JUN 11
#                 02 FEB 17
# 		31 JAN 25
#
# Note: This dataset is compliant with Executive Order 14168, titled "Defending Women from Gender Ideology Extremism and Restoring Biological Truth to the Federal Government".
#
# Input Datasets:	PbCd_h and demo_h
#
# Programmer:	Lisa Mirel / Sam Caudill / Wellington Onyenwe
# *************************************************/

suppressPackageStartupMessages({
  library(haven)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(survey)
  library(tibble)
})

options(survey.lonely.psu = "adjust")

# ***bring in datasets;

# Relative/path-variable style references only; do not hardcode absolute local machine paths.
pbcd_h_path <- "PbCd_h.xpt"
demo_h_path <- "demo_h.xpt"

# ***Note: default method for CI of percentile in SUDAAN uses the Logit;

lab6 <- read_xpt(pbcd_h_path)
names(lab6) <- tolower(names(lab6))

demo <- read_xpt(demo_h_path)
names(demo) <- tolower(names(demo))

lab6 <- lab6 %>% arrange(.data[['seqn']])
demo <- demo %>% arrange(.data[['seqn']])

l6dem <- lab6 %>%
  left_join(demo, by = "seqn")

names(l6dem) <- tolower(names(l6dem))

# *Define Age Groups;

l6dem <- l6dem %>%
  mutate(
    age_grp = case_when(
      .data[['ridageyr']] >= 1 & .data[['ridageyr']] <= 5 ~ 1,
      .data[['ridageyr']] >= 6 & .data[['ridageyr']] <= 11 ~ 2,
      .data[['ridageyr']] >= 12 & .data[['ridageyr']] <= 19 ~ 3,
      .data[['ridageyr']] >= 20 ~ 4,
      TRUE ~ NA_real_
    ),
    age_group = case_when(
      .data[['ridageyr']] >= 1 ~ 1,
      TRUE ~ NA_real_
    ),
    sex = case_when(
      .data[['riagendr']] == 1 ~ 1,
      .data[['riagendr']] == 2 ~ 2,
      TRUE ~ NA_real_
    ),
    race = case_when(
      .data[['ridreth3']] == 1 ~ 1, # *'MA';
      # *if ridreth3 eq 2 then race = 2; *'OH'; *An estimate for this category by itself is not calculated.  Instead OH is combined with MA to create AH;
      .data[['ridreth3']] == 3 ~ 2, # *'NHW';
      .data[['ridreth3']] == 4 ~ 3, # *'NHB';
      .data[['ridreth3']] == 6 ~ 4, # *'NHA';
      .data[['ridreth3']] == 7 ~ 5, # *Non-Hispanic Multi-racial;
      TRUE ~ NA_real_
    ),
    racial = case_when(
      .data[['ridreth3']] %in% c(1, 2) ~ 1, # *'AH';
      TRUE ~ 2
    )
  )

# ******
#
# 	Macro for calculating totals -- following steps in
# 	Appendix A Fourth Exposure Report:
# 	Confidence Interval Estimation for Percentiles
#
# ******;

weighted_percentile_freq_round <- function(x, w, p) {
  ok <- !(is.na(x) | is.na(w))
  x <- x[ok]
  w <- w[ok]
  if (length(x) == 0) return(NA_real_)

  w_rnd <- round(w, 1)
  ord <- order(x)
  x <- x[ord]
  w_rnd <- w_rnd[ord]

  agg <- tibble(x = x, w = w_rnd) %>%
    group_by(.data[['x']]) %>%
    summarise(w = sum(.data[['w']]), .groups = "drop") %>%
    arrange(.data[['x']])

  total_w <- sum(agg[['w']])
  if (is.na(total_w) || total_w <= 0) return(NA_real_)

  target <- p / 100 * total_w
  cs <- cumsum(agg[['w']])
  idx <- which(cs >= target)[1]
  agg[['x']][idx]
}

# Approximation note:
# SAS PROC UNIVARIATE with FREQ and PCTLPTS is matched here using a weighted empirical CDF
# after rounding weights to 0.1 as in the SAS code. Minor differences may remain due to exact
# percentile algorithm details used internally by SAS PROC UNIVARIATE.

make_incremented_values <- function(df, anal_var_col, wt_col, domain_var, domain_val, yr_val) {
  names(df) <- tolower(names(df))
  anal_var_col <- tolower(anal_var_col)
  wt_col <- tolower(wt_col)
  domain_var <- tolower(domain_var)

  x <- df %>%
    mutate(
      anal_var_orig = .data[[anal_var_col]],
      wt_orig = .data[[wt_col]],
      wt_orig_rnd = round(.data[['wt_orig']], 1)
    )

  dom <- x %>%
    filter(.data[[domain_var]] == domain_val, .data[['sddsrvyr']] == yr_val)

  wt_mean_tbl <- dom %>%
    group_by(.data[['anal_var_orig']]) %>%
    summarise(
      wt_mean = mean(.data[['wt_orig']], na.rm = TRUE),
      .groups = "drop"
    )

  xl6dem2 <- x %>%
    left_join(wt_mean_tbl, by = "anal_var_orig") %>%
    mutate(wt_mean_rnd = round(.data[['wt_mean']], 1))

  xxl6dem2 <- xl6dem2 %>%
    arrange(.data[['anal_var_orig']]) %>%
    group_by(.data[['anal_var_orig']]) %>%
    mutate(
      num = row_number(),
      anal_var_incr = .data[['anal_var_orig']] + .data[['num']] / 1000000000
    ) %>%
    ungroup()

  xxl6dem2
}

survey_prop_below <- function(df, indicator_col, weight_col, domain_var, domain_val, yr_val) {
  names(df) <- tolower(names(df))
  indicator_col <- tolower(indicator_col)
  weight_col <- tolower(weight_col)
  domain_var <- tolower(domain_var)

  df2 <- df %>%
    mutate(
      .subpop = (.data[[domain_var]] == domain_val & .data[['sddsrvyr']] == yr_val)
    ) %>%
    filter(
      !is.na(.data[[weight_col]]),
      !is.na(.data[['sdmvpsu']]),
      !is.na(.data[['sdmvstra']]),
      !is.na(.data[[indicator_col]])
    )

  if (nrow(df2) == 0 || !any(df2[['.subpop']])) {
    return(tibble(
      nsum = 0,
      mean = NA_real_,
      semean = NA_real_,
      deffmean = NA_real_,
      ddf = NA_real_
    ))
  }

  des <- svydesign(
    ids = ~sdmvpsu,
    strata = ~sdmvstra,
    weights = stats::as.formula(paste0("~", weight_col)),
    data = df2,
    nest = TRUE
  )

  subdes <- subset(des, .subpop)

  fml <- stats::as.formula(paste0("~", indicator_col))
  est <- svymean(fml, subdes, na.rm = TRUE, deff = TRUE)

  mean_val <- as.numeric(coef(est)[1])
  se_val <- as.numeric(SE(est)[1])

  deff_attr <- survey::deff(est)
  deff_val <- suppressWarnings(as.numeric(deff_attr[1]))
  if (is.na(deff_val)) deff_val <- 1
  deff_val <- max(1, deff_val)

  # SUDAAN atlevel2-atlevel1 is approximated here by survey design degrees of freedom.
  # This is typically close for NHANES Taylor-linearized designs, but may not exactly match SAS/SUDAAN.
  ddf_val <- survey::degf(subdes)

  nsum_val <- sum(df2[['.subpop']] & !is.na(df2[[indicator_col]]))

  tibble(
    nsum = nsum_val,
    mean = mean_val,
    semean = se_val,
    deffmean = deff_val,
    ddf = ddf_val
  )
}

pcntci <- function(var1, var2, var3, var4, var5, var6, var7, source_df) {
  names(source_df) <- tolower(names(source_df))
  var1 <- tolower(var1)
  var6 <- tolower(var6)
  var7 <- tolower(var7)

  l6dem2 <- source_df %>%
    mutate(
      mvar = 1,
      anal_var_orig = .data[[var6]],
      wt_orig = .data[[var7]],
      wt_orig_rnd = round(.data[['wt_orig']], 1)
    )

  # ***Step 1a;
  dom1a <- l6dem2 %>%
    filter(.data[[var1]] == var2, .data[['sddsrvyr']] == var3)

  p_a <- weighted_percentile_freq_round(dom1a[['anal_var_orig']], dom1a[['wt_orig']], var4)

  xpercent1a <- tibble(
    mvar = 1,
    pctl = p_a
  )

  xchperc1a <- l6dem2 %>%
    left_join(xpercent1a, by = "mvar")

  # ***Step 1b;
  xxl6dem2 <- make_incremented_values(l6dem2, "anal_var_orig", "wt_orig", var1, var2, var3)

  dom1b <- xxl6dem2 %>%
    filter(.data[[var1]] == var2, .data[['sddsrvyr']] == var3, !is.na(.data[['wt_mean']]))

  p_b <- weighted_percentile_freq_round(dom1b[['anal_var_incr']], dom1b[['wt_mean']], var4)

  xpercent1b <- tibble(
    mvar = 1,
    pctl = p_b
  )

  xchperc1b <- xxl6dem2 %>%
    left_join(xpercent1b, by = "mvar")

  # ***Step 2a;
  xchperc2a <- xchperc1a %>%
    mutate(
      ind2 = case_when(
        .data[[var1]] == var2 & .data[['sddsrvyr']] == var3 & !is.na(.data[['anal_var_orig']]) & .data[['anal_var_orig']] >= 0 & .data[['anal_var_orig']] < .data[['pctl']] ~ 1,
        .data[[var1]] == var2 & .data[['sddsrvyr']] == var3 & !is.na(.data[['anal_var_orig']]) & .data[['anal_var_orig']] >= .data[['pctl']] ~ 0,
        TRUE ~ NA_real_
      )
    )

  xpest2a <- survey_prop_below(xchperc2a, "ind2", "wt_orig", var1, var2, var3) %>%
    mutate(
      mvar = 1,
      semean_orig = .data[['semean']],
      deffmean_orig = .data[['deffmean']]
    ) %>%
    select("mvar", "semean_orig", "deffmean_orig")

  # ***Step 2b;
  xchperc2b <- xchperc1b %>%
    mutate(
      ind2 = case_when(
        .data[[var1]] == var2 & .data[['sddsrvyr']] == var3 & !is.na(.data[['anal_var_incr']]) & .data[['anal_var_incr']] >= 0 & .data[['anal_var_incr']] < .data[['pctl']] ~ 1,
        .data[[var1]] == var2 & .data[['sddsrvyr']] == var3 & !is.na(.data[['anal_var_incr']]) & .data[['anal_var_incr']] >= .data[['pctl']] ~ 0,
        TRUE ~ NA_real_
      )
    )

  xpest2b <- survey_prop_below(xchperc2b, "ind2", "wt_mean", var1, var2, var3) %>%
    mutate(mvar = 1) %>%
    select("mvar", "nsum", "mean", "semean", "deffmean", "ddf")

  # ***Step 3;

  # ******************************************************************;
  # *The forumlas of Korn et al are used to estimate the proportion of
  #  subjects below the selected percentile-- from Sam Caudill code
  # ******************************************************************;
  xtest <- xpest2a %>%
    inner_join(xpest2b, by = "mvar") %>%
    mutate(
      n_act = .data[['nsum']],                # *ACTUAL SAMPLE SIZE;
      pt = .data[['mean']],                   # *SUDAAN WEIGHTED MEAN PROPORTION;
      t_num = qt(0.975, df = .data[['nsum']] - 1),
      t_den = qt(0.975, df = .data[['ddf']]),
      n1 = ((.data[['t_num']] / .data[['t_den']])^2) * .data[['n_act']] / .data[['deffmean_orig']], # *EFFECTIVE SAMPLE SIZE - SAM METHOD;
      n = ((.data[['t_num']] / .data[['t_den']])^2) * .data[['mean']] * (1 - .data[['mean']]) / (.data[['semean_orig']]^2), # *EFFECTIVE SAMPLE SIZE DUE TO;
      #                                        *COMPLEX STRATIFIED SAMPLING - KORN METHOD;
      n = if_else(is.na(.data[['n']]), .data[['n1']], .data[['n']]),
      n = if_else(.data[['n']] > .data[['nsum']], .data[['nsum']], .data[['n']]),
      n = if_else(.data[['mean']] == 0.0, .data[['nsum']], .data[['n']]),
      na = .data[['mean']] * .data[['n']]                    # *EFFECTIVE NUMBER OF SUBJECTS;
    )

  xcyto <- xtest %>%
    mutate(
      v1 = 2 * .data[['na']],
      v2 = 2 * (.data[['n']] - .data[['na']] + 1),
      v3 = 2 * (.data[['na']] + 1),
      v4 = 2 * (.data[['n']] - .data[['na']]),
      pl = (.data[['v1']] * qf(0.025, .data[['v1']], .data[['v2']])) / (.data[['v2']] + .data[['v1']] * qf(0.025, .data[['v1']], .data[['v2']])),
      pu = (.data[['v3']] * qf(0.975, .data[['v3']], .data[['v4']])) / (.data[['v4']] + .data[['v3']] * qf(0.975, .data[['v3']], .data[['v4']])),
      pt = .data[['pt']] * 100,
      n_eff = .data[['n']]
    )

  xxcyto <- xcyto %>%
    mutate(
      l95 = .data[['pl']] * 100,
      u95 = .data[['pu']] * 100,
      l95 = if_else(is.na(.data[['l95']]), 0.0, .data[['l95']]),
      u95 = if_else(is.na(.data[['u95']]), 100.0, .data[['u95']]),
      pt = case_when(
        var4 == 10 ~ 10.0,
        var4 == 25 ~ 25.0,
        var4 == 50 ~ 50.0,
        var4 == 75 ~ 75.0,
        var4 == 90 ~ 90.0,
        var4 == 95 ~ 95.0,
        TRUE ~ .data[['pt']]
      ),
      l95 = if_else(.data[['l95']] > .data[['pt']], .data[['pt']], .data[['l95']]),
      u95 = if_else(.data[['u95']] < .data[['pt']], .data[['pt']], .data[['u95']])
    )

  #   title3 "PERCENTILE (WITH CIs)";

  # ***Step 4;
  l95_pct <- xxcyto[['l95']][[1]]
  u95_pct <- xxcyto[['u95']][[1]]
  mean_pct <- xxcyto[['pt']][[1]]

  dom_comp <- xxl6dem2 %>%
    filter(.data[[var1]] == var2, .data[['sddsrvyr']] == var3, !is.na(.data[['wt_mean']]))

  p_l95 <- weighted_percentile_freq_round(dom_comp[['anal_var_incr']], dom_comp[['wt_mean']], l95_pct)
  p_u95 <- weighted_percentile_freq_round(dom_comp[['anal_var_incr']], dom_comp[['wt_mean']], u95_pct)
  p_mean <- weighted_percentile_freq_round(dom_comp[['anal_var_incr']], dom_comp[['wt_mean']], mean_pct)

  xxest <- tibble(
    p_l95 = p_l95,
    p_u95 = p_u95,
    p_mean = p_mean
  )

  # *creates multiple final dataset to be put in a final large dataset with all
  # datapoints;
  fin <- xxest %>%
    mutate(
      population_group = case_when(
        var1 == "age_grp" & var2 == 1 ~ "Age:   1-5",
        var1 == "age_grp" & var2 == 2 ~ "Age:  6-11",
        var1 == "age_grp" & var2 == 3 ~ "Age: 12-19",
        var1 == "age_grp" & var2 == 4 ~ "Age: 20+  ",
        var1 == "age_group" & var2 == 1 ~ "Age: ALL",
        var1 == "sex" & var2 == 1 ~ "MALE  ",
        var1 == "sex" & var2 == 2 ~ "FEMALE",
        var1 == "race" & var2 == 1 ~ "MA    ",
        var1 == "race" & var2 == 2 ~ "NHW   ",
        var1 == "race" & var2 == 3 ~ "NHB   ",
        var1 == "race" & var2 == 4 ~ "NHA   ",
        var1 == "racial" & var2 == 1 ~ "AH    ",
        TRUE ~ NA_character_
      ),
      yr = var3,
      per = var4,
      anly = var6,
      p_mean = round(.data[['p_mean']], 2),
      p_l95 = round(.data[['p_l95']], 2),
      p_u95 = round(.data[['p_u95']], 2),
      perci = paste0(.data[['p_mean']], "(", .data[['p_l95']], "-", .data[['p_u95']], ")"),
      n_act = xtest[['n_act']][[1]]
    ) %>%
    select("anly", "population_group", "yr", "per", "perci", "n_act")

  print(fin)

  fin
}

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

fin1  <- pcntci("age_grp",   1, 8, 95,  1, "lbxthg", "wtsh2yr", l6dem)
fin2  <- pcntci("age_grp",   2, 8, 95,  2, "lbxthg", "wtsh2yr", l6dem)
fin3  <- pcntci("age_grp",   3, 8, 95,  3, "lbxthg", "wtsh2yr", l6dem)
fin4  <- pcntci("age_grp",   4, 8, 95,  4, "lbxthg", "wtsh2yr", l6dem)
fin5  <- pcntci("age_group", 1, 8, 95,  5, "lbxthg", "wtsh2yr", l6dem)

fin6  <- pcntci("sex",       1, 8, 95,  6, "lbxthg", "wtsh2yr", l6dem)
fin7  <- pcntci("sex",       2, 8, 95,  7, "lbxthg", "wtsh2yr", l6dem)

fin8  <- pcntci("race",      1, 8, 95,  8, "lbxthg", "wtsh2yr", l6dem)
fin9  <- pcntci("race",      2, 8, 95,  9, "lbxthg", "wtsh2yr", l6dem)
fin10 <- pcntci("race",      3, 8, 95, 10, "lbxthg", "wtsh2yr", l6dem)
fin11 <- pcntci("race",      4, 8, 95, 11, "lbxthg", "wtsh2yr", l6dem)
fin12 <- pcntci("racial",    1, 8, 95, 12, "lbxthg", "wtsh2yr", l6dem)

# ***CREATE ONE LARGE DATASET USING ALL DATASETS CREATED IN THE MACRO;
allperc <- bind_rows(
  fin1, fin2, fin3, fin4, fin5, fin6,
  fin7, fin8, fin9, fin10, fin11, fin12
)

print(allperc)

# Quit;