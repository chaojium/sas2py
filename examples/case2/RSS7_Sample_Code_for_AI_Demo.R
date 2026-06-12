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

canonical_names <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+", "", x)
  x <- gsub("_+$", "", x)
  x <- ifelse(grepl("^[0-9]", x), paste0("x", x), x)
  make.unique(x, sep = "_")
}

normalize_columns <- function(df) {
  names(df) <- canonical_names(names(df))
  df
}

safe_factor <- function(x, levels, labels) {
  factor(x, levels = levels, labels = labels, exclude = NULL)
}

# %include "C:\Users\nya7\OneDrive - CDC\Sam\PHHT Materials\NCHS Rapids Survey 2025 GLP1\Data\RSS7 PUF Input Program_SLP.sas";
# libname rss "C:\Users\nya7\OneDrive - CDC\Sam\PHHT Materials\NCHS Rapids Survey 2025 GLP1\Data";

# Local SAS paths are source-data location hints only. Read the SAS member rss.rss7_puf
# from the execution input directory, with the current working directory as fallback.
input_dir <- Sys.getenv("SAS2PY_INPUT_DIR", unset = getwd())
rss7_puf_path <- file.path(input_dir, "rss7_puf.sas7bdat")

rss7_puf <- read_sas(rss7_puf_path) %>%
  normalize_columns()

# proc contents data=rss.rss7_puf; run;
rss7_puf_contents <- tibble(
  variable = names(rss7_puf),
  class = map_chr(rss7_puf, ~ paste(class(.x), collapse = "/")),
  n_missing = map_int(rss7_puf, ~ sum(is.na(.x)))
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

fmt_ages_b <- c("1" = "18-39y", "2" = "40-64y", "3" = "65y+")
fmt_bmis <- c("-8" = "Not Ascertained", "1" = "Underweight", "2" = "Healthy Weight", "3" = "Overweight", "4" = "Obesity- Class 1 or 2", "6" = "Severe Obesity")
fmt_yn <- c("1" = "Yes", "2" = "No", "3" = "DK")
fmt_re <- c("1" = "White", "2" = "Black", "3" = "Hispanic", "4" = "Other/Multiracial", "5" = "Missing")
fmt_source <- c("1" = "Multiple Source", "2" = "Single Source")
fmt_glp <- c("1" = "Yes", "2" = "No", "3" = "Missing")
fmt_sexage <- c("1" = "M 18-39", "2" = "M 40-64", "3" = "M 65+", "4" = "F 18-39", "5" = "F 40-64", "6" = "F 65+")

format_lookup <- list(
  agecat_b = fmt_ages_b,
  bmicat = fmt_bmis,
  compounded = fmt_yn,
  race = fmt_re,
  multiple_source = fmt_source,
  glp_med12m = fmt_glp,
  sexage = fmt_sexage,
  p_sex = c("1" = "Male", "2" = "Female"),
  p_poverty4_r = c("1" = "Below 100% FPL", "2" = "100%-199% FPL", "3" = "200%-399% FPL", "4" = "400%+ FPL"),
  nchs_metro = c("1" = "Metropolitan", "2" = "Nonmetropolitan"),
  dem_region = c("1" = "Northeast", "2" = "Midwest", "3" = "South", "4" = "West")
)

get_value_label <- function(variable, value) {
  key <- as.character(value)
  lookup <- format_lookup[[variable]]
  if (is.null(lookup)) {
    return(key)
  }
  out <- unname(lookup[key])
  ifelse(is.na(out), key, out)
}

require_columns <- function(df, cols, df_name = "data") {
  missing_cols <- setdiff(cols, names(df))
  if (length(missing_cols) > 0) {
    stop(
      "Missing required columns in ", df_name, ": ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

required_input_columns <- c(
  "p_age5yrs_r", "p_sex", "bmicat6", "dem_raceeth", "glp_compmed",
  "glp_rx12ma", "glp_rx12mb", "glp_rx12mc", "glp_rx12md", "glp_rx12me",
  "glp_med12m", "glp_medrx", "glp_mednow", "p_strata_r", "p_psu_r",
  "weight", "p_poverty4_r", "nchs_metro", "dem_region"
)
require_columns(rss7_puf, required_input_columns, "rss7_puf")

# data sam; set rss.rss7_puf;
sam <- rss7_puf %>%
  mutate(
    # *Age Categories;
    agecat_b = case_when(
      p_age5yrs_r %in% c(1, 2, 3, 4, 5) ~ 1,      # *18-49y;
      p_age5yrs_r %in% c(6, 7, 8, 9, 10) ~ 2,     # *40-64y;
      p_age5yrs_r %in% c(11, 12) ~ 3,             # *65y+;
      TRUE ~ NA_real_
    ),
    # 	format agecat_b ages_b.;
    agecat_b_label = safe_factor(agecat_b, levels = c(1, 2, 3), labels = unname(fmt_ages_b)),

    # *Sex-Age Categories: MALE=1, FEMALE=2;
    sexage = case_when(
      p_sex == 1 & agecat_b == 1 ~ 1,
      p_sex == 1 & agecat_b == 2 ~ 2,
      p_sex == 1 & agecat_b == 3 ~ 3,
      p_sex == 2 & agecat_b == 1 ~ 4,
      p_sex == 2 & agecat_b == 2 ~ 5,
      p_sex == 2 & agecat_b == 3 ~ 6,
      TRUE ~ NA_real_
    ),
    # 	else if p_sex=2 and agecat_b=1 then sexage=4;	else if p_sex=2 and agecat_b=2 then sexage=5;	else if p_sex=2 and agecat_b=3 then sexage=6; format sexage sexage.;
    sexage_label = safe_factor(sexage, levels = c(1, 2, 3, 4, 5, 6), labels = unname(fmt_sexage)),

    # *BMI Categories;
    bmicat = bmicat6,
    # 	bmicat=bmicat6; if bmicat in (4,5) then bmicat=4;	*combine class 1 and 2 obesity;
    bmicat = if_else(bmicat %in% c(4, 5), 4, bmicat),
    # 	format bmicat bmis.;
    bmicat_label = safe_factor(bmicat, levels = c(-8, 1, 2, 3, 4, 6), labels = unname(fmt_bmis)),

    # *Race/Ethnicity;
    race = case_when(
      dem_raceeth == 7 ~ 1,                         # *white, non-Hispanic;
      dem_raceeth == 3 ~ 2,                         # *black, non-Hispanic;
      dem_raceeth == 4 ~ 3,                         # *Hispanic;
      dem_raceeth %in% c(1, 2, 5, 6, 8) ~ 4,        # *other or multiracial;
      is.na(dem_raceeth) ~ 5,                       # *missing;
      TRUE ~ NA_real_
    ),
    # 	format race re.;
    race_label = safe_factor(race, levels = c(1, 2, 3, 4, 5), labels = unname(fmt_re)),

    # *Compounded Med Use- Recode;
    compounded = case_when(
      glp_compmed == 1 ~ 1,                         # *yes;
      glp_compmed == 0 ~ 2,                         # *no;
      TRUE ~ 3                                      # *unknown, skipped, question not asked;
    ),
    # 	format compounded yn.;
    compounded_label = safe_factor(compounded, levels = c(1, 2, 3), labels = unname(fmt_yn)),

    # *Create variable to indicate Multiple Sources of GLP-1s
    #  Recreate variables because there are negative values if the question was not asked OR if the respondent skipped that option (i.e., -5 vs. no/0);
    rx_a = if_else(glp_rx12ma == 1, 1, 0),
    rx_b = if_else(glp_rx12mb == 1, 1, 0),
    rx_c = if_else(glp_rx12mc == 1, 1, 0),
    rx_d = if_else(glp_rx12md == 1, 1, 0),
    rx_e = if_else(glp_rx12me == 1, 1, 0),
    source_sum = rx_a + rx_b + rx_c + rx_d + rx_e,
    multiple_source = case_when(
      source_sum > 1 ~ 1,
      source_sum == 1 ~ 2,
      TRUE ~ NA_real_
    ),
    # 	if source_sum>1 then multiple_source=1; else if source_sum=1 then multiple_source=2;	*1=Yes multiple sources, 2=No single source type of GLP1;
    # 	format multiple_source source.;
    multiple_source_label = safe_factor(multiple_source, levels = c(1, 2), labels = unname(fmt_source)),

    # *Recode variables to fit SUDAAN- if 0, they are ignored as missing;
    glp_med12m = case_when(
      glp_med12m == 0 ~ 2,
      glp_med12m == -6 ~ 3,
      TRUE ~ as.numeric(glp_med12m)
    ),
    # 	format glp_med12m glp.;
    glp_med12m_label = safe_factor(glp_med12m, levels = c(1, 2, 3), labels = unname(fmt_glp)),

    glp_medrx = case_when(
      glp_medrx == 0 ~ 2,
      glp_medrx == -6 ~ 3,
      TRUE ~ as.numeric(glp_medrx)
    ),
    # 	if glp_medrx=0 then glp_medrx=2; else if glp_medrx=-6 then glp_medrx=3; format glp_medrx rx.;
    glp_medrx_label = safe_factor(glp_medrx, levels = c(1, 2, 3), labels = unname(fmt_glp)),

    glp_mednow = case_when(
      glp_mednow == 0 ~ 2,
      glp_mednow == -6 ~ 3,
      TRUE ~ as.numeric(glp_mednow)
    ),
    # 	if glp_mednow=0 then glp_mednow=2; else if glp_mednow=-6 then glp_mednow=3; format glp_mednow rx.;
    glp_mednow_label = safe_factor(glp_mednow, levels = c(1, 2, 3), labels = unname(fmt_glp))
  )
# run;

sam <- sam %>%
  arrange(p_strata_r, p_psu_r)

analysis_design <- svydesign(
  ids = ~p_psu_r,
  strata = ~p_strata_r,
  weights = ~weight,
  data = sam,
  nest = TRUE
)

estimate_catlevel_percent <- function(design, data, outcome_var, catlevel, subgroup_vars = NULL, subpop_expr = NULL) {
  if (!is.null(subpop_expr)) {
    design <- subset(design, eval(substitute(subpop_expr), data, parent.frame()))
    data <- design$variables
  }

  outcome_values <- design$variables[[outcome_var]]
  design$variables$.indicator_catlevel <- as.numeric(outcome_values == catlevel)
  design$variables$.indicator_catlevel[is.na(outcome_values)] <- NA_real_

  degf_design <- survey::degf(design)
  critical_value <- qt(0.975, df = degf_design)

  overall_est <- svymean(~.indicator_catlevel, design, na.rm = TRUE)
  overall_coef <- as.numeric(coef(overall_est)[1])
  overall_se <- as.numeric(SE(overall_est)[1])
  overall_nsum <- sum(!is.na(design$variables[[outcome_var]]))
  overall_wsum <- sum(weights(design)[!is.na(design$variables[[outcome_var]])], na.rm = TRUE)

  overall <- tibble(
    subgroup_variable = "Overall",
    subgroup_level = NA_real_,
    subgroup_label = "Overall",
    outcome_variable = outcome_var,
    catlevel = catlevel,
    percent = overall_coef * 100,
    sepercent = overall_se * 100,
    lowpct = pmax(0, (overall_coef - critical_value * overall_se) * 100),
    uppct = pmin(100, (overall_coef + critical_value * overall_se) * 100),
    nsum = overall_nsum,
    wsum = overall_wsum
  )

  if (is.null(subgroup_vars) || length(subgroup_vars) == 0) {
    return(overall)
  }

  subgroup_results <- map_dfr(subgroup_vars, function(g) {
    g_values <- sort(unique(design$variables[[g]][!is.na(design$variables[[g]])]))

    map_dfr(g_values, function(gl) {
      domain_design <- subset(design, design$variables[[g]] == gl)

      if (nrow(domain_design$variables) == 0) {
        return(tibble(
          subgroup_variable = g,
          subgroup_level = as.numeric(gl),
          subgroup_label = get_value_label(g, gl),
          outcome_variable = outcome_var,
          catlevel = catlevel,
          percent = NA_real_,
          sepercent = NA_real_,
          lowpct = NA_real_,
          uppct = NA_real_,
          nsum = 0,
          wsum = NA_real_
        ))
      }

      est <- tryCatch(
        svymean(~.indicator_catlevel, domain_design, na.rm = TRUE),
        error = function(e) NULL
      )

      valid_rows <- !is.na(domain_design$variables[[outcome_var]])
      nsum_val <- sum(valid_rows)
      wsum_val <- sum(weights(domain_design)[valid_rows], na.rm = TRUE)

      if (is.null(est) || nsum_val == 0) {
        tibble(
          subgroup_variable = g,
          subgroup_level = as.numeric(gl),
          subgroup_label = get_value_label(g, gl),
          outcome_variable = outcome_var,
          catlevel = catlevel,
          percent = NA_real_,
          sepercent = NA_real_,
          lowpct = NA_real_,
          uppct = NA_real_,
          nsum = nsum_val,
          wsum = wsum_val
        )
      } else {
        est_coef <- as.numeric(coef(est)[1])
        est_se <- as.numeric(SE(est)[1])

        tibble(
          subgroup_variable = g,
          subgroup_level = as.numeric(gl),
          subgroup_label = get_value_label(g, gl),
          outcome_variable = outcome_var,
          catlevel = catlevel,
          percent = est_coef * 100,
          sepercent = est_se * 100,
          lowpct = pmax(0, (est_coef - critical_value * est_se) * 100),
          uppct = pmin(100, (est_coef + critical_value * est_se) * 100),
          nsum = nsum_val,
          wsum = wsum_val
        )
      }
    })
  })

  bind_rows(overall, subgroup_results)
}

subgroup_vars <- c("agecat_b", "p_sex", "race", "p_poverty4_r", "nchs_metro", "dem_region")

# *GLP-1 Use by Select Demographics;
# proc sort data=sam; by p_strata_r p_psu_r; run;
# proc descript data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	var glp_med12m; catlevel 1;
# 	subgroup agecat_b p_sex race p_poverty4_r nchs_metro dem_region; levels 3 2 5 4 2 4;
# 	print percent sepercent lowpct uppct nsum wsum/style=nchs percentfmt=F6.3 sepercentfmt=F9.2 lowpctfmt=F6.3 uppctfmt=F6.3 wsumfmt=F12.0 nohead notime nodate;
# 	output / tablecell=all filetype=sas filename=demos replace;
# run;
demos <- estimate_catlevel_percent(
  design = analysis_design,
  data = sam,
  outcome_var = "glp_med12m",
  catlevel = 1,
  subgroup_vars = subgroup_vars
) %>%
  mutate(
    percent = round(percent, 3),
    sepercent = round(sepercent, 2),
    lowpct = round(lowpct, 3),
    uppct = round(uppct, 3),
    wsum = round(wsum, 0)
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
comp_design <- subset(analysis_design, glp_med12m == 1)

reliability_checks_comp <- estimate_catlevel_percent(
  design = comp_design,
  data = comp_design$variables,
  outcome_var = "compounded",
  catlevel = 1,
  subgroup_vars = subgroup_vars
) %>%
  mutate(
    percent = round(percent, 3),
    sepercent = round(sepercent, 2),
    lowpct = round(lowpct, 3),
    uppct = round(uppct, 3),
    wsum = round(wsum, 0)
  )

# *Chi-square test for difference in GLP-1 use by age category;
# proc sort data=sam; by p_strata_r p_psu_r; run;
# proc crosstab data=sam filetype=sas design=wr;
# 	nest p_strata_r p_psu_r/missunit;	weight weight;
# 	subgroup agecat_b glp_med12m;
# 	tables agecat_b*glp_med12m; levels 3 3;
# 	test chisq acmh;
# run;

crosstab_design <- subset(analysis_design, !is.na(agecat_b) & !is.na(glp_med12m))

age_glp_table <- svytable(~agecat_b + glp_med12m, crosstab_design)
names(dimnames(age_glp_table)) <- c("agecat_b", "glp_med12m")

age_glp_crosstab <- as_tibble(as.data.frame(age_glp_table), .name_repair = "unique")
names(age_glp_crosstab)[seq_len(3)] <- c("agecat_b", "glp_med12m", "weighted_frequency")
age_glp_crosstab <- age_glp_crosstab %>%
  mutate(
    agecat_b = as.numeric(as.character(agecat_b)),
    glp_med12m = as.numeric(as.character(glp_med12m)),
    agecat_b_label = map_chr(agecat_b, ~ get_value_label("agecat_b", .x)),
    glp_med12m_label = map_chr(glp_med12m, ~ get_value_label("glp_med12m", .x))
  ) %>%
  select(agecat_b, agecat_b_label, glp_med12m, glp_med12m_label, weighted_frequency)

chisq_satterthwaite <- svychisq(~agecat_b + glp_med12m, crosstab_design, statistic = "F")
chisq_rao_scott <- svychisq(~agecat_b + glp_med12m, crosstab_design, statistic = "Chisq")

chi_square_tests <- tibble(
  test = c(
    "Rao-Scott adjusted F test of association",
    "Rao-Scott adjusted chi-square test of association"
  ),
  statistic = c(
    unname(as.numeric(chisq_satterthwaite$statistic)),
    unname(as.numeric(chisq_rao_scott$statistic))
  ),
  parameter = c(
    paste(unname(chisq_satterthwaite$parameter), collapse = ", "),
    paste(unname(chisq_rao_scott$parameter), collapse = ", ")
  ),
  p_value = c(
    unname(as.numeric(chisq_satterthwaite$p.value)),
    unname(as.numeric(chisq_rao_scott$p.value))
  ),
  note = c(
    "R survey::svychisq adjusted F test; closest standard R analogue to SUDAAN design-based association testing.",
    "R survey::svychisq Rao-Scott chi-square. SUDAAN CHISQ/ACMH details may differ slightly due to implementation."
  )
)

print(demos)
print(reliability_checks_comp)
print(age_glp_crosstab)
print(chi_square_tests)

output_workbook <- "rss7_glp1_analysis_outputs.xlsx"

wb <- createWorkbook()
addWorksheet(wb, "contents")
writeData(wb, "contents", rss7_puf_contents)

addWorksheet(wb, "demos")
writeData(wb, "demos", demos)

addWorksheet(wb, "reliability_checks_comp")
writeData(wb, "reliability_checks_comp", reliability_checks_comp)

addWorksheet(wb, "age_glp_crosstab")
writeData(wb, "age_glp_crosstab", age_glp_crosstab)

addWorksheet(wb, "chi_square_tests")
writeData(wb, "chi_square_tests", chi_square_tests)

saveWorkbook(wb, output_workbook, overwrite = TRUE)

write.csv(demos, "demos.csv", row.names = FALSE)
write.csv(reliability_checks_comp, "reliability_checks_comp.csv", row.names = FALSE)
write.csv(age_glp_crosstab, "age_glp_crosstab.csv", row.names = FALSE)
write.csv(chi_square_tests, "chi_square_tests.csv", row.names = FALSE)