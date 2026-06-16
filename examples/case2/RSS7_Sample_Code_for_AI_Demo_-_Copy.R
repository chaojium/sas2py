# /*********************************************
# Aim: To examine prevalence of GLP-1 use, including compounded medications, sources of fills, and barriers to use
# Data Source: Rapid Survey System (RSS-7) data. Rapids is run by CDC/NCHS
# Analyst: Sam Pierce (NYA7)
# *********************************************/

suppressPackageStartupMessages({
  library(haven)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(survey)
  library(tibble)
  library(openxlsx)
})

options(survey.lonely.psu = "adjust")

canonicalize_names <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+", "", x)
  x <- gsub("_+$", "", x)
  x <- ifelse(x == "", "x", x)
  x <- ifelse(grepl("^[0-9]", x), paste0("x", x), x)
  make.unique(x, sep = "_")
}

require_columns <- function(df, cols, df_name = "data") {
  missing_cols <- setdiff(cols, names(df))
  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "%s is missing required column(s): %s",
        df_name,
        paste(missing_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

first_numeric <- function(x) {
  if (length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(x)[1]
}

lookup_label <- function(var_name, value, format_lookup) {
  if (is.na(value)) {
    return(NA_character_)
  }
  key <- as.character(value)
  if (!is.null(format_lookup[[var_name]]) && key %in% names(format_lookup[[var_name]])) {
    return(unname(format_lookup[[var_name]][[key]]))
  }
  key
}

# %include "C:\Users\nya7\OneDrive - CDC\Sam\PHHT Materials\NCHS Rapids Survey 2025 GLP1\Data\RSS7 PUF Input Program_SLP.sas";
# libname rss "C:\Users\nya7\OneDrive - CDC\Sam\PHHT Materials\NCHS Rapids Survey 2025 GLP1\Data";
# Relative/path-variable style references only; do not hardcode absolute local machine paths.
input_dir <- Sys.getenv("SAS2PY_INPUT_DIR", unset = getwd())
rss7_puf_path <- file.path(input_dir, "rss7_puf.sas7bdat")

if (!file.exists(rss7_puf_path)) {
  stop(
    sprintf(
      "Input dataset not found: %s. Place rss7_puf.sas7bdat in SAS2PY_INPUT_DIR or the current working directory.",
      rss7_puf_path
    ),
    call. = FALSE
  )
}

rss7_puf <- read_sas(rss7_puf_path)
names(rss7_puf) <- canonicalize_names(names(rss7_puf))

# proc contents data=rss.rss7_puf; run;
print(str(rss7_puf))

# proc format;
format_lookup <- list(
  agecat_b = c(
    "1" = "18-39y",
    "2" = "40-64y",
    "3" = "65y+"
  ),
  bmicat = c(
    "-8" = "Not Ascertained",
    "1" = "Underweight",
    "2" = "Healthy Weight",
    "3" = "Overweight",
    "4" = "Obesity- Class 1 or 2",
    "6" = "Severe Obesity"
  ),
  compounded = c(
    "1" = "Yes",
    "2" = "No",
    "3" = "DK"
  ),
  yn = c(
    "1" = "Yes",
    "2" = "No",
    "3" = "DK"
  ),
  race = c(
    "1" = "White",
    "2" = "Black",
    "3" = "Hispanic",
    "4" = "Other/Multiracial",
    "5" = "Missing"
  ),
  multiple_source = c(
    "1" = "Multiple Source",
    "2" = "Single Source"
  ),
  glp_med12m = c(
    "1" = "Yes",
    "2" = "No",
    "3" = "Missing"
  ),
  glp_medrx = c(
    "1" = "Yes",
    "2" = "No",
    "3" = "Missing"
  ),
  glp_mednow = c(
    "1" = "Yes",
    "2" = "No",
    "3" = "Missing"
  ),
  sexage = c(
    "1" = "M 18-39",
    "2" = "M 40-64",
    "3" = "M 65+",
    "4" = "F 18-39",
    "5" = "F 40-64",
    "6" = "F 65+"
  ),
  p_sex = c(
    "1" = "Male",
    "2" = "Female"
  ),
  p_poverty4_r = c(
    "1" = "Below 100% FPL",
    "2" = "100%-199% FPL",
    "3" = "200%-399% FPL",
    "4" = "400%+ FPL"
  ),
  nchs_metro = c(
    "1" = "Metropolitan",
    "2" = "Nonmetropolitan"
  ),
  dem_region = c(
    "1" = "Northeast",
    "2" = "Midwest",
    "3" = "South",
    "4" = "West"
  )
)
# run;

required_input_columns <- c(
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
  "dem_region"
)
require_columns(rss7_puf, required_input_columns, "rss7_puf")

sam <- rss7_puf %>%
  mutate(
    p_age5yrs_r_num = as.numeric(.data[["p_age5yrs_r"]]),
    p_sex_num = as.numeric(.data[["p_sex"]]),
    bmicat6_num = as.numeric(.data[["bmicat6"]]),
    dem_raceeth_num = as.numeric(.data[["dem_raceeth"]]),
    glp_compmed_num = as.numeric(.data[["glp_compmed"]]),
    glp_rx12ma_num = as.numeric(.data[["glp_rx12ma"]]),
    glp_rx12mb_num = as.numeric(.data[["glp_rx12mb"]]),
    glp_rx12mc_num = as.numeric(.data[["glp_rx12mc"]]),
    glp_rx12md_num = as.numeric(.data[["glp_rx12md"]]),
    glp_rx12me_num = as.numeric(.data[["glp_rx12me"]]),
    glp_med12m_num = as.numeric(.data[["glp_med12m"]]),
    glp_medrx_num = as.numeric(.data[["glp_medrx"]]),
    glp_mednow_num = as.numeric(.data[["glp_mednow"]]),

    # *Age Categories;
    agecat_b = case_when(
      .data[["p_age5yrs_r_num"]] %in% c(1, 2, 3, 4, 5) ~ 1,  # *18-49y;
      .data[["p_age5yrs_r_num"]] %in% c(6, 7, 8, 9, 10) ~ 2, # *40-64y;
      .data[["p_age5yrs_r_num"]] %in% c(11, 12) ~ 3,         # *65y+;
      TRUE ~ NA_real_
    ),

    # *Sex-Age Categories: MALE=1, FEMALE=2;
    sexage = case_when(
      .data[["p_sex_num"]] == 1 & .data[["agecat_b"]] == 1 ~ 1,
      .data[["p_sex_num"]] == 1 & .data[["agecat_b"]] == 2 ~ 2,
      .data[["p_sex_num"]] == 1 & .data[["agecat_b"]] == 3 ~ 3,
      .data[["p_sex_num"]] == 2 & .data[["agecat_b"]] == 1 ~ 4,
      .data[["p_sex_num"]] == 2 & .data[["agecat_b"]] == 2 ~ 5,
      .data[["p_sex_num"]] == 2 & .data[["agecat_b"]] == 3 ~ 6,
      TRUE ~ NA_real_
    ),

    # *BMI Categories;
    bmicat = .data[["bmicat6_num"]],
    bmicat = if_else(.data[["bmicat"]] %in% c(4, 5), 4, .data[["bmicat"]]), # *combine class 1 and 2 obesity;

    # *Race/Ethnicity;
    race = case_when(
      .data[["dem_raceeth_num"]] == 7 ~ 1,                  # *white, non-Hispanic;
      .data[["dem_raceeth_num"]] == 3 ~ 2,                  # *black, non-Hispanic;
      .data[["dem_raceeth_num"]] == 4 ~ 3,                  # *Hispanic;
      .data[["dem_raceeth_num"]] %in% c(1, 2, 5, 6, 8) ~ 4, # *other or multiracial;
      is.na(.data[["dem_raceeth_num"]]) ~ 5,                # *missing;
      TRUE ~ NA_real_
    ),

    # *Compounded Med Use- Recode;
    compounded = case_when(
      .data[["glp_compmed_num"]] == 1 ~ 1, # *yes;
      .data[["glp_compmed_num"]] == 0 ~ 2, # *no;
      TRUE ~ 3                             # *unknown, skipped, question not asked;
    ),

    # *Create variable to indicate Multiple Sources of GLP-1s
    #  Recreate variables because there are negative values if the question was not asked OR if the respondent skipped that option (i.e., -5 vs. no/0);
    rx_a = if_else(.data[["glp_rx12ma_num"]] == 1, 1, 0),
    rx_b = if_else(.data[["glp_rx12mb_num"]] == 1, 1, 0),
    rx_c = if_else(.data[["glp_rx12mc_num"]] == 1, 1, 0),
    rx_d = if_else(.data[["glp_rx12md_num"]] == 1, 1, 0),
    rx_e = if_else(.data[["glp_rx12me_num"]] == 1, 1, 0),
    source_sum = .data[["rx_a"]] + .data[["rx_b"]] + .data[["rx_c"]] + .data[["rx_d"]] + .data[["rx_e"]],
    multiple_source = case_when(
      .data[["source_sum"]] > 1 ~ 1,
      .data[["source_sum"]] == 1 ~ 2, # *1=Yes multiple sources, 2=No single source type of GLP1;
      TRUE ~ NA_real_
    ),

    # *Recode variables to fit SUDAAN- if 0, they are ignored as missing;
    glp_med12m = case_when(
      .data[["glp_med12m_num"]] == 0 ~ 2,
      .data[["glp_med12m_num"]] == -6 ~ 3,
      TRUE ~ .data[["glp_med12m_num"]]
    ),
    glp_medrx = case_when(
      .data[["glp_medrx_num"]] == 0 ~ 2,
      .data[["glp_medrx_num"]] == -6 ~ 3,
      TRUE ~ .data[["glp_medrx_num"]]
    ),
    glp_mednow = case_when(
      .data[["glp_mednow_num"]] == 0 ~ 2,
      .data[["glp_mednow_num"]] == -6 ~ 3,
      TRUE ~ .data[["glp_mednow_num"]]
    )
  )

# *GLP-1 Use by Select Demographics;
# proc sort data=sam; by p_strata_r p_psu_r; run;
sam <- sam %>%
  arrange(.data[["p_strata_r"]], .data[["p_psu_r"]])

design_data <- sam %>%
  filter(
    !is.na(.data[["p_strata_r"]]),
    !is.na(.data[["p_psu_r"]]),
    !is.na(.data[["weight"]])
  )

rss_design <- svydesign(
  ids = ~p_psu_r,
  strata = ~p_strata_r,
  weights = ~weight,
  data = design_data,
  nest = TRUE
)

survey_descript_catlevel <- function(design, var_name, catlevel, subgroup_levels, format_lookup, subpop_mask = NULL) {
  vars <- design$variables
  if (!var_name %in% names(vars)) {
    stop(sprintf("Variable %s is not present in the survey design.", var_name), call. = FALSE)
  }

  if (is.null(subpop_mask)) {
    subpop_mask <- rep(TRUE, nrow(vars))
  } else {
    subpop_mask <- as.logical(subpop_mask)
    subpop_mask[is.na(subpop_mask)] <- FALSE
  }

  outcome <- as.numeric(vars[[var_name]])
  indicator <- ifelse(is.na(outcome), NA_real_, as.numeric(outcome == catlevel))

  make_one_row <- function(subgroup_var, subgroup_level, subgroup_label, row_name, row_label, domain_mask) {
    domain_mask <- as.logical(domain_mask)
    domain_mask[is.na(domain_mask)] <- FALSE
    analysis_mask <- domain_mask & !is.na(indicator)

    nsum_value <- sum(analysis_mask, na.rm = TRUE)

    fixed_row <- tibble(
      row_name = as.character(row_name),
      var_name = var_name,
      catlevel = catlevel,
      subgroup = subgroup_var,
      subgroup_level = as.character(subgroup_level),
      subgroup_label = subgroup_label,
      row_label = row_label,
      percent = NA_real_,
      sepercent = NA_real_,
      lowpct = NA_real_,
      uppct = NA_real_,
      nsum = nsum_value,
      wsum = NA_real_,
      df = NA_real_,
      ci_method = "t-based Wald interval using survey design degrees of freedom"
    )

    if (is.na(nsum_value) || nsum_value == 0) {
      return(fixed_row)
    }

    one_tmp <- rep(1, nrow(vars))
    des_tmp <- update(
      design,
      domain_tmp = analysis_mask,
      indicator_tmp = indicator,
      one_tmp = one_tmp
    )
    subdes <- subset(des_tmp, domain_tmp)

    est <- svymean(~indicator_tmp, subdes, na.rm = TRUE)
    est_coef <- coef(est)
    est_se <- SE(est)

    prop_value <- first_numeric(est_coef)
    se_value <- first_numeric(est_se)

    total_est <- svytotal(~one_tmp, subdes, na.rm = TRUE)
    wsum_value <- first_numeric(coef(total_est))

    df_value <- survey::degf(subdes)
    crit_value <- if (!is.na(df_value) && df_value > 0) {
      qt(0.975, df = df_value)
    } else {
      # SUDAAN Taylor WR intervals are t-based. A normal critical value is used only when
      # design degrees of freedom are unavailable or nonpositive.
      qnorm(0.975)
    }

    percent_value <- prop_value * 100
    se_percent_value <- se_value * 100
    low_value <- percent_value - crit_value * se_percent_value
    upp_value <- percent_value + crit_value * se_percent_value

    fixed_row %>%
      mutate(
        percent = percent_value,
        sepercent = se_percent_value,
        lowpct = max(0, low_value, na.rm = TRUE),
        uppct = min(100, upp_value, na.rm = TRUE),
        wsum = wsum_value,
        df = df_value
      )
  }

  rows <- list()

  # PROC DESCRIPT: include SUDAAN-style overall rows such as _ONE_=0/ROW_NAME='0' when downstream cleanup expects them.
  overall_mask <- subpop_mask
  rows[[length(rows) + 1]] <- make_one_row(
    subgroup_var = "_one_",
    subgroup_level = "0",
    subgroup_label = "Overall",
    row_name = "0",
    row_label = "Overall",
    domain_mask = overall_mask
  )

  for (subgroup_var in names(subgroup_levels)) {
    if (!subgroup_var %in% names(vars)) {
      stop(sprintf("Subgroup variable %s is not present in the survey design.", subgroup_var), call. = FALSE)
    }

    subgroup_values <- as.numeric(vars[[subgroup_var]])
    levels_for_var <- subgroup_levels[[subgroup_var]]

    for (level_value in levels_for_var) {
      level_mask <- subpop_mask & !is.na(subgroup_values) & subgroup_values == level_value
      level_label <- lookup_label(subgroup_var, level_value, format_lookup)

      rows[[length(rows) + 1]] <- make_one_row(
        subgroup_var = subgroup_var,
        subgroup_level = level_value,
        subgroup_label = level_label,
        row_name = as.character(level_value),
        row_label = level_label,
        domain_mask = level_mask
      )
    }
  }

  bind_rows(rows) %>%
    mutate(
      percent = round(.data[["percent"]], 3),
      sepercent = round(.data[["sepercent"]], 2),
      lowpct = round(.data[["lowpct"]], 3),
      uppct = round(.data[["uppct"]], 3),
      wsum = round(.data[["wsum"]], 0)
    )
}

subgroup_levels <- list(
  agecat_b = c(1, 2, 3),
  p_sex = c(1, 2),
  race = c(1, 2, 3, 4, 5),
  p_poverty4_r = c(1, 2, 3, 4),
  nchs_metro = c(1, 2),
  dem_region = c(1, 2, 3, 4)
)

# proc descript data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	var glp_med12m; catlevel 1;
# 	subgroup agecat_b p_sex race p_poverty4_r nchs_metro dem_region; levels 3 2 5 4 2 4;
# 	print percent sepercent lowpct uppct nsum wsum/style=nchs percentfmt=F6.3 sepercentfmt=F9.2 lowpctfmt=F6.3 uppctfmt=F6.3 wsumfmt=F12.0 nohead notime nodate;
# 	output / tablecell=all filetype=sas filename=demos replace;
# run;
demos <- survey_descript_catlevel(
  design = rss_design,
  var_name = "glp_med12m",
  catlevel = 1,
  subgroup_levels = subgroup_levels,
  format_lookup = format_lookup
)

# *Compounded GLP-1 Use among those taking GLP-1s in the past year, by Select Demographics;
# proc sort data=sam; by p_strata_r p_psu_r; run;
# proc descript data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	subpopn glp_med12m=1;
# 	var compounded; catlevel 1;
# 	subgroup agecat_b p_sex race p_poverty4_r nchs_metro dem_region; levels 3 2 5 4 2 4;
# 	print percent sepercent lowpct uppct nsum wsum/style=nchs percentfmt=F6.3 sepercentfmt=F9.2 lowpctfmt=F6.3 uppctfmt=F6.3 wsumfmt=F12.0 nohead notime nodate;
# 	output / tablecell=all filetype=sas filename=reliability_checks_comp replace;
# run;
comp_subpop_mask <- as.numeric(rss_design$variables[["glp_med12m"]]) == 1
comp_subpop_mask[is.na(comp_subpop_mask)] <- FALSE

reliability_checks_comp <- survey_descript_catlevel(
  design = rss_design,
  var_name = "compounded",
  catlevel = 1,
  subgroup_levels = subgroup_levels,
  format_lookup = format_lookup,
  subpop_mask = comp_subpop_mask
)

survey_crosstab_age_glp <- function(design, format_lookup) {
  vars <- design$variables

  age_raw <- as.numeric(vars[["agecat_b"]])
  glp_raw <- as.numeric(vars[["glp_med12m"]])

  age_factor <- factor(
    age_raw,
    levels = c(1, 2, 3),
    labels = unname(format_lookup[["agecat_b"]][c("1", "2", "3")])
  )
  glp_factor <- factor(
    glp_raw,
    levels = c(1, 2, 3),
    labels = unname(format_lookup[["glp_med12m"]][c("1", "2", "3")])
  )

  valid_mask <- !is.na(age_factor) & !is.na(glp_factor)
  valid_mask[is.na(valid_mask)] <- FALSE

  des_tmp <- update(
    design,
    agecat_b_factor = age_factor,
    glp_med12m_factor = glp_factor,
    crosstab_domain_tmp = valid_mask,
    age_score = age_raw,
    glp_score = glp_raw
  )
  subdes <- subset(des_tmp, crosstab_domain_tmp)

  weighted_table <- svytable(~agecat_b_factor + glp_med12m_factor, subdes)
  names(dimnames(weighted_table)) <- c("agecat_b", "glp_med12m")
  weighted_cells <- as_tibble(as.data.frame(weighted_table), .name_repair = "unique")
  names(weighted_cells)[seq_len(3)] <- c("agecat_b_label", "glp_med12m_label", "weighted_frequency")

  unweighted_table <- table(
    factor(age_factor[valid_mask], levels = levels(age_factor)),
    factor(glp_factor[valid_mask], levels = levels(glp_factor)),
    useNA = "no"
  )
  names(dimnames(unweighted_table)) <- c("agecat_b", "glp_med12m")
  unweighted_cells <- as_tibble(as.data.frame(unweighted_table), .name_repair = "unique")
  names(unweighted_cells)[seq_len(3)] <- c("agecat_b_label", "glp_med12m_label", "nsum")

  cell_output <- weighted_cells %>%
    left_join(unweighted_cells, by = c("agecat_b_label", "glp_med12m_label")) %>%
    group_by(.data[["agecat_b_label"]]) %>%
    mutate(
      row_weighted_total = sum(.data[["weighted_frequency"]], na.rm = TRUE),
      row_percent = if_else(
        .data[["row_weighted_total"]] > 0,
        100 * .data[["weighted_frequency"]] / .data[["row_weighted_total"]],
        NA_real_
      )
    ) %>%
    ungroup() %>%
    mutate(
      weighted_frequency = round(.data[["weighted_frequency"]], 0),
      row_percent = round(.data[["row_percent"]], 3)
    )

  chisq_test <- svychisq(
    ~agecat_b_factor + glp_med12m_factor,
    design = subdes,
    statistic = "adjWald"
  )

  chisq_output <- tibble(
    test = "chisq_adjwald",
    statistic = first_numeric(chisq_test$statistic),
    sdf = (3 - 1) * (3 - 1),
    denominator_df = first_numeric(chisq_test$parameter),
    p_value = first_numeric(chisq_test$p.value),
    note = "SUDAAN-like adjusted Wald F statistic from survey::svychisq(statistic = 'adjWald'); SDF is nominal table df."
  )

  score_vars <- subdes$variables
  age_score_ok <- !is.na(score_vars[["age_score"]])
  glp_score_ok <- !is.na(score_vars[["glp_score"]])
  enough_scores <- length(unique(score_vars[["age_score"]][age_score_ok])) > 1 &&
    length(unique(score_vars[["glp_score"]][glp_score_ok])) > 1

  if (enough_scores) {
    acmh_model <- svyglm(glp_score ~ age_score, design = subdes)
    beta <- coef(acmh_model)
    cov_beta <- vcov(acmh_model)

    if ("age_score" %in% names(beta) &&
        "age_score" %in% rownames(cov_beta) &&
        "age_score" %in% colnames(cov_beta) &&
        is.finite(cov_beta["age_score", "age_score"]) &&
        cov_beta["age_score", "age_score"] > 0) {
      acmh_stat <- as.numeric(beta["age_score"]^2 / cov_beta["age_score", "age_score"])
      acmh_p <- pchisq(acmh_stat, df = 1, lower.tail = FALSE)
      acmh_output <- tibble(
        test = "acmh_linear_score_wald_approx",
        statistic = acmh_stat,
        sdf = 1,
        denominator_df = survey::degf(subdes),
        p_value = acmh_p,
        note = "Approximation to SUDAAN ACMH using a design-based Wald test for linear association of ordinal scores."
      )
    } else {
      acmh_output <- tibble(
        test = "acmh_linear_score_wald_approx",
        statistic = NA_real_,
        sdf = 1,
        denominator_df = survey::degf(subdes),
        p_value = NA_real_,
        note = "ACMH approximation not estimable because the age_score coefficient or covariance was singular/non-estimable."
      )
    }
  } else {
    acmh_output <- tibble(
      test = "acmh_linear_score_wald_approx",
      statistic = NA_real_,
      sdf = 1,
      denominator_df = survey::degf(subdes),
      p_value = NA_real_,
      note = "ACMH approximation not estimable because fewer than two observed age or GLP score levels were present."
    )
  }

  list(
    cells = cell_output,
    tests = bind_rows(chisq_output, acmh_output)
  )
}

# *Chi-square test for difference in GLP-1 use by age category;
# proc sort data=sam; by p_strata_r p_psu_r; run;
# proc crosstab data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	subgroup agecat_b glp_med12m;
# 	tables agecat_b*glp_med12m; levels 3 3;
# 	test chisq acmh;
# run;
crosstab_results <- survey_crosstab_age_glp(rss_design, format_lookup)
age_glp_crosstab <- crosstab_results$cells
age_glp_tests <- crosstab_results$tests

print(demos)
print(reliability_checks_comp)
print(age_glp_crosstab)
print(age_glp_tests)

output_workbook <- "rss7_glp1_analysis_outputs.xlsx"
wb <- createWorkbook()

addWorksheet(wb, "demos")
writeData(wb, "demos", demos)

addWorksheet(wb, "reliability_checks_comp")
writeData(wb, "reliability_checks_comp", reliability_checks_comp)

addWorksheet(wb, "age_glp_crosstab")
writeData(wb, "age_glp_crosstab", age_glp_crosstab)

addWorksheet(wb, "age_glp_tests")
writeData(wb, "age_glp_tests", age_glp_tests)

saveWorkbook(wb, output_workbook, overwrite = TRUE)

message(sprintf("Analysis outputs written to %s", output_workbook))