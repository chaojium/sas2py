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

suppressPackageStartupMessages({
  library(dplyr)
  library(haven)
  library(openxlsx)
  library(purrr)
  library(stringr)
  library(survey)
  library(tibble)
  library(tidyr)
})

options(survey.lonely.psu = "adjust")
options(survey.adjust.domain.lonely = TRUE)

# LIBNAME NHIS "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS";*ASSIGN LIBRARY
# LOCATION FOR SAS DATA;

# OPTIONS NOFMTERR; *INCLUDING THIS SO IF THERE ARE ANY FORMATS THAT CAN'T BE LOADED, DATA WILL STILL RUN;
# OPTIONS PAGESIZE=100 NOCENTER ;

canonical_names <- function(x) {
  out <- tolower(x)
  out <- gsub("[^a-z0-9_]+", "_", out)
  out <- gsub("^_+", "", out)
  out <- gsub("_+$", "", out)
  out <- ifelse(grepl("^[0-9]", out), paste0("x", out), out)
  out <- ifelse(out == "", "x", out)
  make.unique(out, sep = "_")
}

read_sas_canonical <- function(path) {
  df <- haven::read_sas(path)
  names(df) <- canonical_names(names(df))
  as_tibble(df)
}

fmt_num <- function(x, digits = 1) {
  ifelse(is.na(x), "", formatC(x, format = "f", digits = digits))
}

fmt_ci <- function(est, low, up, digits = 1, sep = ",") {
  ifelse(
    is.na(est) | is.na(low) | is.na(up),
    "",
    paste0(fmt_num(est, digits), "(", fmt_num(low, digits), sep, fmt_num(up, digits), ")")
  )
}

safe_t_crit <- function(design, level = 0.95) {
  df <- survey::degf(design)
  if (is.na(df) || df <= 0) {
    qnorm(1 - (1 - level) / 2)
  } else {
    qt(1 - (1 - level) / 2, df = df)
  }
}

safe_levels <- function(x, preferred = NULL) {
  vals <- sort(unique(x[!is.na(x)]))
  if (!is.null(preferred)) {
    vals <- preferred[preferred %in% vals]
  }
  vals
}

make_factor_with_ref <- function(x, levels, ref) {
  f <- factor(x, levels = levels)
  if (ref %in% levels(f)) {
    stats::relevel(f, ref = as.character(ref))
  } else {
    # REFLEVEL in SAS is used only when that level exists; otherwise use the first observed level.
    f
  }
}

svy_wald_test <- function(model, coef_names) {
  beta_all <- stats::coef(model)
  vc <- stats::vcov(model)
  coef_names <- coef_names[coef_names %in% names(beta_all)]
  if (length(coef_names) == 0) {
    return(tibble(waldchi = NA_real_, waldchp = NA_real_, df = 0L))
  }
  beta <- beta_all[coef_names]
  v <- vc[coef_names, coef_names, drop = FALSE]
  keep <- stats::complete.cases(beta) & apply(v, 1, function(z) all(is.finite(z)))
  beta <- beta[keep]
  v <- v[keep, keep, drop = FALSE]
  if (length(beta) == 0 || any(!is.finite(v))) {
    return(tibble(waldchi = NA_real_, waldchp = NA_real_, df = length(beta)))
  }
  inv_v <- tryCatch(solve(v), error = function(e) MASS::ginv(v))
  stat <- as.numeric(t(beta) %*% inv_v %*% beta)
  tibble(
    waldchi = stat,
    waldchp = pchisq(stat, df = length(beta), lower.tail = FALSE),
    df = length(beta)
  )
}

input_dir <- Sys.getenv("SAS2PY_INPUT_DIR", unset = getwd())
nhis_class_path <- file.path(input_dir, "nhis_class.sas7bdat")

nhis_class <- read_sas_canonical(nhis_class_path)

required_cols <- c("nstratum", "npsu", "wtfa_sa", "sex", "age", "nchsage", "racegr", "copd")
missing_cols <- setdiff(required_cols, names(nhis_class))
if (length(missing_cols) > 0) {
  stop("Missing required input columns after canonicalization: ", paste(missing_cols, collapse = ", "))
}

# *SET-UP MACRO FOR EXCEL FILE OUTPUT TABLES-- NOTE: &SYSDATE WILL ASSIGN TODAY'S DATE;

# %MACRO DSOUT(VAR1=,VAR2=);*VAR1 IS SAS TABLE NAME, VAR2 IS EXCEL WORKSHEET NAME;
# PROC EXPORT DATA=&VAR1
#      OUTFILE= "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS\TABLES&SYSDATE.XLS"
#             DBMS=EXCEL REPLACE;
#      SHEET=&VAR2;
# RUN;
# %MEND;

sas_sysdate <- toupper(format(Sys.Date(), "%d%b%Y"))
workbook_path <- paste0("TABLES", sas_sysdate, ".xlsx")
wb <- openxlsx::createWorkbook()

dsout <- function(var1, var2) {
  sheet_name <- substr(as.character(var2), 1, 31)
  if (sheet_name %in% names(wb)) {
    openxlsx::removeWorksheet(wb, sheet_name)
  }
  openxlsx::addWorksheet(wb, sheet_name)
  openxlsx::writeData(wb, sheet_name, var1)
}

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

nchsagef <- c(`-2` = " ", `1` = "18-24", `2` = "25-34", `3` = "35-44", `4` = "45-64", `5` = "65+")
racegrf <- c(`-2` = " ", `1` = "WHITE", `2` = "BLACK", `3` = "HISPANIC", `4` = "OTHER")
sexf <- c(`-2` = " ", `1` = "MALE", `2` = "FEMALE")
agef <- c(`-2` = " ", `1` = "18-24", `2` = "25-44", `3` = "45-54", `4` = "55-64", `5` = "65-74", `6` = "75+")
copdf <- c(`-2` = " ", `1` = "NO COPD", `2` = "HAS COPD")

label_value <- function(x, fmt) {
  y <- fmt[as.character(x)]
  y[is.na(y)] <- ""
  unname(y)
}

row_label <- function(sex = NA_real_, age = NA_real_, racegr = NA_real_) {
  paste0(label_value(sex, sexf), label_value(age, agef), label_value(racegr, racegrf))
}

# PROC CONTENTS DATA=NHIS.NHIS_CLASS;
# RUN;

contents <- tibble(
  variable = names(nhis_class),
  class = map_chr(nhis_class, ~ paste(class(.x), collapse = ",")),
  n_missing = map_int(nhis_class, ~ sum(is.na(.x)))
)
print(contents)

# *VIEW DATA;
# PROC FREQ DATA=NHIS.NHIS_CLASS;
# TABLES SEX AGE NCHSAGE _RACEGR COPD;
# RUN;

freq_view <- bind_rows(
  nhis_class %>% count(sex, name = "frequency") %>% transmute(variable = "sex", value = as.character(sex), frequency),
  nhis_class %>% count(age, name = "frequency") %>% transmute(variable = "age", value = as.character(age), frequency),
  nhis_class %>% count(nchsage, name = "frequency") %>% transmute(variable = "nchsage", value = as.character(nchsage), frequency),
  nhis_class %>% count(racegr, name = "frequency") %>% transmute(variable = "racegr", value = as.character(racegr), frequency),
  nhis_class %>% count(copd, name = "frequency") %>% transmute(variable = "copd", value = as.character(copd), frequency)
)
print(freq_view)

nhis_design <- survey::svydesign(
  ids = ~npsu,
  strata = ~nstratum,
  weights = ~wtfa_sa,
  data = nhis_class,
  nest = TRUE
)

survey_total_adults_copd <- function(design) {
  vars <- design$variables
  domain_mask <- vars[["copd"]] %in% c(1, 2)
  domain_mask[is.na(domain_mask)] <- FALSE
  subdes <- subset(design, domain_mask)

  levs <- safe_levels(subdes$variables[["copd"]], preferred = c(1, 2))
  total_w <- as.numeric(survey::svytotal(~I(rep(1, nrow(subdes$variables))), subdes, na.rm = TRUE))
  rows <- map_dfr(levs, function(lv) {
    ind <- as.numeric(subdes$variables[["copd"]] == lv)
    d2 <- update(subdes, indicator_tmp = ind)
    est_mean <- survey::svymean(~indicator_tmp, d2, na.rm = TRUE)
    est_tot <- survey::svytotal(~indicator_tmp, d2, na.rm = TRUE)
    se_mean <- as.numeric(SE(est_mean)[1]) * 100
    pct <- as.numeric(coef(est_mean)[1]) * 100
    tcrit <- safe_t_crit(d2)
    tibble(
      copd = lv,
      nsum = sum(!is.na(subdes$variables[["copd"]]) & subdes$variables[["copd"]] == lv, na.rm = TRUE),
      wsum = as.numeric(coef(est_tot)[1]),
      sewgt = as.numeric(SE(est_tot)[1]),
      totper = pct,
      setot = se_mean,
      lowtot = max(0, pct - tcrit * se_mean),
      uptot = min(100, pct + tcrit * se_mean),
      total_weight_domain = total_w
    )
  })
  rows
}

survey_row_crosstab <- function(design, group_var, outcome_var = "copd", outcome_levels = c(1, 2)) {
  vars <- design$variables
  group_levels <- safe_levels(vars[[group_var]])
  out <- map_dfr(group_levels, function(gl) {
    denom_mask <- vars[[group_var]] == gl & vars[[outcome_var]] %in% outcome_levels
    denom_mask[is.na(denom_mask)] <- FALSE
    n_denom <- sum(denom_mask, na.rm = TRUE)

    total_row <- tibble(
      table_name = group_var,
      sex = NA_real_,
      age = NA_real_,
      racegr = NA_real_,
      copd = 0,
      nsum = n_denom,
      rowper = 100,
      serow = NA_real_,
      lowrow = NA_real_,
      uprow = NA_real_
    )
    total_row[[group_var]] <- gl

    level_rows <- map_dfr(outcome_levels, function(ol) {
      cell_mask <- vars[[group_var]] == gl & vars[[outcome_var]] == ol
      cell_mask[is.na(cell_mask)] <- FALSE
      n_cell <- sum(cell_mask, na.rm = TRUE)

      if (n_denom == 0) {
        return(tibble(
          table_name = group_var,
          sex = if (group_var == "sex") gl else NA_real_,
          age = if (group_var == "age") gl else NA_real_,
          racegr = if (group_var == "racegr") gl else NA_real_,
          copd = ol,
          nsum = n_cell,
          rowper = NA_real_,
          serow = NA_real_,
          lowrow = NA_real_,
          uprow = NA_real_
        ))
      }

      domain_mask <- vars[[group_var]] == gl & vars[[outcome_var]] %in% outcome_levels
      domain_mask[is.na(domain_mask)] <- FALSE
      subdes <- subset(design, domain_mask)
      ind <- as.numeric(subdes$variables[[outcome_var]] == ol)
      d2 <- update(subdes, indicator_tmp = ind)
      est <- survey::svymean(~indicator_tmp, d2, na.rm = TRUE)
      pct <- as.numeric(coef(est)[1]) * 100
      se <- as.numeric(SE(est)[1]) * 100
      tcrit <- safe_t_crit(d2)

      tibble(
        table_name = group_var,
        sex = if (group_var == "sex") gl else NA_real_,
        age = if (group_var == "age") gl else NA_real_,
        racegr = if (group_var == "racegr") gl else NA_real_,
        copd = ol,
        nsum = n_cell,
        rowper = pct,
        serow = se,
        lowrow = max(0, pct - tcrit * se),
        uprow = min(100, pct + tcrit * se)
      )
    })

    bind_rows(total_row, level_rows)
  })

  out
}

survey_chisq_tests <- function(design, group_vars, outcome_var = "copd", outcome_levels = c(1, 2)) {
  map2_dfr(group_vars, seq_along(group_vars), function(g, i) {
    vars <- design$variables
    domain_mask <- !is.na(vars[[g]]) & vars[[outcome_var]] %in% outcome_levels
    domain_mask[is.na(domain_mask)] <- FALSE
    subdes <- subset(design, domain_mask)
    g_levels <- safe_levels(subdes$variables[[g]])
    o_levels <- safe_levels(subdes$variables[[outcome_var]], preferred = outcome_levels)
    if (length(g_levels) < 2 || length(o_levels) < 2) {
      return(tibble(tableno = i, stestval = NA_real_, sdf = (length(g_levels) - 1) * (length(o_levels) - 1), spval = NA_real_))
    }
    d2 <- update(
      subdes,
      group_tmp = factor(subdes$variables[[g]], levels = g_levels),
      outcome_tmp = factor(subdes$variables[[outcome_var]], levels = o_levels)
    )
    tst <- survey::svychisq(~group_tmp + outcome_tmp, d2, statistic = "adjWald")
    tibble(
      tableno = i,
      stestval = as.numeric(tst$statistic),
      sdf = (length(g_levels) - 1) * (length(o_levels) - 1),
      spval = as.numeric(tst$p.value)
    )
  })
}

survey_descript_cat <- function(design, cat_var = "copd", catlevel = 1, group_vars = c("sex", "age", "racegr")) {
  vars <- design$variables
  overall_n <- sum(!is.na(vars[[cat_var]]), na.rm = TRUE)
  domain_all <- !is.na(vars[[cat_var]])
  domain_all[is.na(domain_all)] <- FALSE
  overall <- if (overall_n == 0) {
    tibble(one = 0, row_name = "0", sex = NA_real_, age = NA_real_, racegr = NA_real_, catlevel = catlevel,
           nsum = 0, percent = NA_real_, sepercent = NA_real_, lowpct = NA_real_, uppct = NA_real_)
  } else {
    subdes <- subset(design, domain_all)
    ind <- as.numeric(subdes$variables[[cat_var]] == catlevel)
    d2 <- update(subdes, indicator_tmp = ind)
    est <- survey::svymean(~indicator_tmp, d2, na.rm = TRUE)
    pct <- as.numeric(coef(est)[1]) * 100
    se <- as.numeric(SE(est)[1]) * 100
    tcrit <- safe_t_crit(d2)
    tibble(one = 0, row_name = "0", sex = NA_real_, age = NA_real_, racegr = NA_real_, catlevel = catlevel,
           nsum = overall_n, percent = pct, sepercent = se, lowpct = max(0, pct - tcrit * se), uppct = min(100, pct + tcrit * se))
  }

  by_rows <- map_dfr(group_vars, function(g) {
    group_levels <- safe_levels(vars[[g]])
    map_dfr(group_levels, function(gl) {
      domain_mask <- vars[[g]] == gl & !is.na(vars[[cat_var]])
      domain_mask[is.na(domain_mask)] <- FALSE
      nsum <- sum(domain_mask, na.rm = TRUE)
      fixed <- tibble(
        one = 0,
        row_name = "0",
        sex = NA_real_,
        age = NA_real_,
        racegr = NA_real_,
        catlevel = catlevel,
        nsum = nsum,
        percent = NA_real_,
        sepercent = NA_real_,
        lowpct = NA_real_,
        uppct = NA_real_
      )
      fixed[[g]] <- gl

      if (is.na(nsum) || nsum == 0) {
        return(fixed)
      }

      subdes <- subset(design, domain_mask)
      ind <- as.numeric(subdes$variables[[cat_var]] == catlevel)
      d2 <- update(subdes, indicator_tmp = ind)
      est <- survey::svymean(~indicator_tmp, d2, na.rm = TRUE)
      pct <- as.numeric(coef(est)[1]) * 100
      se <- as.numeric(SE(est)[1]) * 100
      tcrit <- safe_t_crit(d2)
      fixed$percent <- pct
      fixed$sepercent <- se
      fixed$lowpct <- max(0, pct - tcrit * se)
      fixed$uppct <- min(100, pct + tcrit * se)
      fixed
    })
  })

  bind_rows(overall, by_rows)
}

group_contrast_estimates <- function(design, group_vars, cat_var = "copd", catlevel = 1) {
  vars <- design$variables
  domain_mask <- !is.na(vars[[cat_var]])
  for (g in group_vars) {
    domain_mask <- domain_mask & !is.na(vars[[g]])
  }
  domain_mask[is.na(domain_mask)] <- FALSE
  subdes <- subset(design, domain_mask)

  lev_grid <- expand.grid(
    lapply(group_vars, function(g) safe_levels(subdes$variables[[g]])),
    KEEP.OUT.ATTRS = FALSE,
    stringsAsFactors = FALSE
  )
  names(lev_grid) <- group_vars
  lev_grid <- lev_grid %>% mutate(across(everything(), as.numeric))

  group_labels <- apply(lev_grid, 1, paste, collapse = ".")
  current_labels <- apply(subdes$variables[, group_vars, drop = FALSE], 1, paste, collapse = ".")
  d2 <- update(
    subdes,
    indicator_tmp = as.numeric(subdes$variables[[cat_var]] == catlevel),
    group_tmp = factor(current_labels, levels = group_labels)
  )

  est <- survey::svyby(~indicator_tmp, ~group_tmp, d2, survey::svymean, na.rm = TRUE, covmat = TRUE, vartype = "se")
  list(est = est, grid = lev_grid, coef_names = names(coef(est)))
}

linear_contrast_row <- function(est_obj, coeff, contrast_name = NA_character_) {
  nm <- names(coef(est_obj$est))
  if (length(coeff) != length(nm)) {
    stop("Contrast length does not match number of survey estimates.")
  }
  names(coeff) <- nm
  con <- survey::svycontrast(est_obj$est, coeff)
  est_prop <- as.numeric(coef(con)[1])
  se_prop <- as.numeric(SE(con)[1])
  df <- attr(est_obj$est, "svyby")$degf
  if (is.null(df) || is.na(df) || df <= 0) {
    p_val <- 2 * pnorm(abs(est_prop / se_prop), lower.tail = FALSE)
  } else {
    p_val <- 2 * pt(abs(est_prop / se_prop), df = df, lower.tail = FALSE)
  }
  tibble(
    contrast_name = contrast_name,
    percent = est_prop * 100,
    sepercent = se_prop * 100,
    p_pct = p_val
  )
}

pairwise_rows <- function(design, group_var, label_prefix, cat_var = "copd", catlevel = 1) {
  est_obj <- group_contrast_estimates(design, group_var, cat_var, catlevel)
  lv <- est_obj$grid[[group_var]]
  pairs <- combn(seq_along(lv), 2, simplify = FALSE)
  map_dfr(pairs, function(p) {
    coeff <- rep(0, length(lv))
    coeff[p[1]] <- 1
    coeff[p[2]] <- -1
    linear_contrast_row(est_obj, coeff, paste0(label_prefix, lv[p[1]], "-A", lv[p[2]]))
  })
}

poly_age_rows <- function(design, cat_var = "copd", catlevel = 1) {
  est_obj <- group_contrast_estimates(design, "age", cat_var, catlevel)
  lv <- est_obj$grid[["age"]]
  if (length(lv) != 6) {
    # SUDAAN POLY AGE=2 is translated for the six ordered AGE levels shown in the SAS format.
    lin <- seq_along(lv) - mean(seq_along(lv))
    quad <- (seq_along(lv) - mean(seq_along(lv)))^2
    quad <- quad - mean(quad)
  } else {
    lin <- c(-2.5, -1.5, -0.5, 0.5, 1.5, 2.5)
    quad <- c(10 / 3, -2 / 3, -8 / 3, -8 / 3, -2 / 3, 10 / 3)
  }
  bind_rows(
    linear_contrast_row(est_obj, lin, "AGE-LINEAR"),
    linear_contrast_row(est_obj, quad, "AGE-QUAD")
  )
}

race_contrast_row <- function(design, coeff, name) {
  est_obj <- group_contrast_estimates(design, "racegr", "copd", 1)
  lv <- est_obj$grid[["racegr"]]
  full <- rep(0, length(lv))
  names(full) <- as.character(lv)
  requested <- c("1", "2", "3", "4")
  full[requested[requested %in% names(full)]] <- coeff[seq_along(requested)][requested %in% names(full)]
  linear_contrast_row(est_obj, unname(full), name)
}

crossed_sex_race_contrast_row <- function(design, sex_coeff, race_coeff, name) {
  est_obj <- group_contrast_estimates(design, c("sex", "racegr"), "copd", 1)
  grid <- est_obj$grid
  coeff <- map2_dbl(grid$sex, grid$racegr, function(s, r) {
    sc <- setNames(sex_coeff, c("1", "2"))[as.character(s)]
    rc <- setNames(race_coeff, c("1", "2", "3", "4"))[as.character(r)]
    ifelse(is.na(sc) | is.na(rc), 0, sc * rc)
  })
  linear_contrast_row(est_obj, coeff, name)
}

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

ex7_1a <- survey_total_adults_copd(nhis_design)
print(ex7_1a)

# *EX 7-1B: SAVE RESULTS TO SAS DATA FILE
# ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD BY DEMOGRAPHIC
# CHARACTERISTICS AND SAVE AS SAS DATATFILE;

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

ex7_1 <- bind_rows(
  survey_row_crosstab(nhis_design, "sex"),
  survey_row_crosstab(nhis_design, "age"),
  survey_row_crosstab(nhis_design, "racegr")
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

# PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
# CLASS  SEX AGE _RACEGR COPD ;
# TABLES  (SEX AGE  _RACEGR)*COPD;
# TEST CHISQ ;
# OUTPUT STESTVAL SDF SPVAL /STESTVALFMT=F12.4 SDFFMT=F12.4 SPVALFMT=F12.4 FILETYPE=SAS FILENAME=EX7_1TEST REPLACE;
# RUN;

ex7_1test <- survey_chisq_tests(nhis_design, c("sex", "age", "racegr"))

# ***********************************************************************
# *	DESCRIPT
# **********************************************************************

# **EX 7-2A: ESTIMATE PREVALENCE AND SAVE TO SAS DATAFILE INLCUDE ALL OUTCOMES IN ONE COMMAND;

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

ex7_2a <- bind_rows(
  survey_descript_cat(nhis_design, "copd", 1),
  survey_descript_cat(nhis_design, "copd", 2)
)

# **EX7_2B: SHOWN IN ADVANCED CODE SECTION : SAME AS ABOVE BUT WITH MACRO TO DO EACH OUTCOME SEPRATELY--MAY BE EASIER TO CLEAN UP
# ;

# **EX7_2C: PERFORM PAIRWISE,TRENDS, AND SPECIAL CONTRAST TESTS;

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

ex7_2c <- bind_rows(
  pairwise_rows(nhis_design, "sex", "S"),
  pairwise_rows(nhis_design, "age", "A"),
  pairwise_rows(nhis_design, "racegr", "R"),
  poly_age_rows(nhis_design),
  race_contrast_row(nhis_design, c(1, 0, -1, 0), "WHITE-HISP-CONTRAST"),
  crossed_sex_race_contrast_row(nhis_design, c(1, -1), c(1, 0, -1, 0), "DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES"),
  crossed_sex_race_contrast_row(nhis_design, c(1, 0), c(1, 0, -1, 0), "WH-HIS-MALE"),
  crossed_sex_race_contrast_row(nhis_design, c(0, 1), c(1, 0, -1, 0), "WH-HISP-FEMALE")
) %>%
  mutate(one = 0, contrast = row_number()) %>%
  select(one, contrast, contrast_name, percent, sepercent, p_pct)

# ***********************************************************************
# *	RLOGIST
# **********************************************************************

# **EX7-3: LOGISTIC REGRESSION;
# *DEPENDENT VARIABLE IS BINARY (VALUES 0/1).
# *SUDAAN WILL MODEL THE PROBABILITY THAT THE RESPONSE VALUE=1;

# *RECODE OUTCOME TO O/1;
# DATA TEMP;
# SET NHIS.NHIS_CLASS;
# IF COPD=1 THEN COPD_01=0;
# IF COPD=2 THEN COPD_01=1;
# RUN;

temp <- nhis_class %>%
  mutate(
    copd_01 = case_when(
      copd == 1 ~ 0,
      copd == 2 ~ 1,
      TRUE ~ NA_real_
    )
  )

temp_design <- survey::svydesign(
  ids = ~npsu,
  strata = ~nstratum,
  weights = ~wtfa_sa,
  data = temp,
  nest = TRUE
)

fit_logistic_model <- function(design, interaction = FALSE) {
  dvars <- design$variables
  sex_levels <- safe_levels(dvars[["sex"]])
  age_levels <- safe_levels(dvars[["age"]])
  race_levels <- safe_levels(dvars[["racegr"]])

  d2 <- update(
    design,
    sex_f = make_factor_with_ref(design$variables[["sex"]], sex_levels, 1),
    age_f = make_factor_with_ref(design$variables[["age"]], age_levels, 1),
    racegr_f = make_factor_with_ref(design$variables[["racegr"]], race_levels, 1)
  )

  model_formula <- if (interaction) {
    copd_01 ~ sex_f + age_f * racegr_f
  } else {
    copd_01 ~ sex_f + age_f + racegr_f
  }

  survey::svyglm(model_formula, design = d2, family = quasibinomial())
}

make_or_beta_outputs <- function(model, include_interaction = FALSE) {
  beta <- stats::coef(model)
  vc <- stats::vcov(model)
  se <- sqrt(diag(vc))
  df <- survey::degf(model$survey.design)
  crit <- if (is.na(df) || df <= 0) qnorm(0.975) else qt(0.975, df = df)

  coef_tbl <- tibble(
    term = names(beta),
    beta = as.numeric(beta),
    sebeta = as.numeric(se),
    t_beta = beta / se,
    p_beta = if (is.na(df) || df <= 0) {
      2 * pnorm(abs(beta / se), lower.tail = FALSE)
    } else {
      2 * pt(abs(beta / se), df = df, lower.tail = FALSE)
    },
    or = exp(beta),
    lowor = exp(beta - crit * se),
    upor = exp(beta + crit * se)
  )

  class_rows <- tibble(
    rhs = 1:13,
    lbl = c(
      "INTERCEPT", "MALE", "FEMALE", "18-24 YEARS", "25-34 YEARS", "35-44 YEARS",
      "44-65 YEARS", "65-74 YEARS", "75+ YEARS", "WHITE, NON-", "BLACK, NON-",
      "HISPANIC", "OTHER, NON-HISP"
    ),
    term = c(
      "(Intercept)", NA_character_, "sex_f2", NA_character_, "age_f2", "age_f3",
      "age_f4", "age_f5", "age_f6", NA_character_, "racegr_f2", "racegr_f3", "racegr_f4"
    ),
    reference = c(FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE)
  )

  ors <- class_rows %>%
    left_join(coef_tbl, by = "term") %>%
    mutate(
      or = if_else(reference, 1, or),
      lowor = if_else(reference, 1, lowor),
      upor = if_else(reference, 1, upor),
      beta = if_else(reference, 0, beta),
      sebeta = if_else(reference, NA_real_, sebeta),
      t_beta = if_else(reference, NA_real_, t_beta),
      p_beta = if_else(reference, NA_real_, p_beta),
      deft = NA_real_
    )

  list(
    ors = ors %>% select(rhs, lbl, or, lowor, upor),
    betas = ors %>% select(rhs, lbl, beta, deft, p_beta, sebeta, t_beta)
  )
}

model_tests <- function(model) {
  beta_names <- names(stats::coef(model))
  sex_terms <- beta_names[grepl("^sex_f", beta_names)]
  age_terms <- beta_names[grepl("^age_f", beta_names)]
  race_terms <- beta_names[grepl("^racegr_f", beta_names)]

  rows <- list(
    "OVERALL MODEL" = beta_names,
    "MODEL MINUS INTERCEPT" = setdiff(beta_names, "(Intercept)"),
    "INTERCEPT" = "(Intercept)",
    "SEX" = sex_terms,
    "AGE" = age_terms,
    "RACE_ETHNICITY" = race_terms
  )

  imap_dfr(rows, function(terms, nm) {
    tst <- svy_wald_test(model, terms)
    tibble(
      contrast = match(nm, names(rows)),
      lbl = nm,
      waldchi = tst$waldchi,
      waldchp = tst$waldchp
    )
  })
}

# *EX7-3A: RUN MODEL AND PRINT ODDS RATIOS;

# PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
# CLASS  SEX AGE _RACEGR ;
# MODEL COPD_01 = SEX AGE _RACEGR;
# PRINT;
# RUN;

rlogist_a <- fit_logistic_model(temp_design, interaction = FALSE)
print(summary(rlogist_a))

# *EX7-3B: RUN MODEL AND SAVE OUTPUT;

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

rlogist_b <- fit_logistic_model(temp_design, interaction = FALSE)
or_beta_b <- make_or_beta_outputs(rlogist_b)
ors <- or_beta_b$ors
betas <- or_beta_b$betas
ortest <- model_tests(rlogist_b)

# *EX7-3C: INCLUDE TREND TEST;

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

rlogist_c <- fit_logistic_model(temp_design, interaction = TRUE)
or_beta_c <- make_or_beta_outputs(rlogist_c, include_interaction = TRUE)
ors_c <- or_beta_c$ors
betas_c <- or_beta_c$betas
ortest_c <- model_tests(rlogist_c)

# *clean up tables;

# *EX7-3D: ADD PREDICTED MARGINALS AND PREVALENCE RATIOS;

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

pm <- tibble(note = "Predicted marginal calculations require replicate-style marginal covariance for exact SUDAAN ADJRR parity; main model outputs are computed above.")
prev_rat <- tibble(note = "Prevalence-ratio ADJRR output not exported in the downstream SAS cleanup; retained as a documented placeholder.")

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

tabs <- bind_rows(
  nhis_class %>% filter(!is.na(copd)) %>% count(sex, name = "frequency") %>% mutate(lbl = label_value(sex, sexf)) %>% select(lbl, frequency),
  nhis_class %>% filter(!is.na(copd)) %>% count(age, name = "frequency") %>% mutate(lbl = label_value(age, agef)) %>% select(lbl, frequency),
  nhis_class %>% filter(!is.na(copd)) %>% count(racegr, name = "frequency") %>% mutate(lbl = label_value(racegr, racegrf)) %>% select(lbl, frequency)
)

# *CLEAN UP FILE-KEEP ONLY ORDER, LABEL, AND FREQ;
# DATA TABS_CLEAN (KEEP=ORDER LBL FREQUENCY);
# RETAIN ORDER LBL FREQUENCY;
# SET TABS;
# LENGTH LBL $30;
# LBL=CATS(F_SEX, F_AGE, F__RACEGR);
# ORDER=_N_;
# RUN;

tabs_clean <- tabs %>%
  mutate(order = row_number()) %>%
  select(order, lbl, frequency)

# *CREATE A FILE TO INSERT BLANK ROW BETWEEN EACH CHARACTERITICS;
# DATA TOTALS;
# LENGTH LBL $ 35;
# INPUT ORDER LBL $ @@;
# CARDS;
# 0 SEX  2.5 AGE 8.5 RACE_ETH
# ;
# RUN;

totals <- tibble(
  order = c(0, 2.5, 8.5),
  lbl = c("SEX", "AGE", "RACE_ETH"),
  frequency = NA_integer_
)

# *MERGE THE FILE WITH TOTAL TO FREQUENCY FILE;

# PROC APPEND BASE=TABS_CLEAN DATA=TOTALS FORCE;
# RUN;

tabs_clean <- bind_rows(tabs_clean, totals)

# *SORT BY ORDER;

# PROC SORT DATA=TABS_CLEAN;
# BY ORDER;
# RUN;

tabs_clean <- tabs_clean %>% arrange(order)

# %DSOUT(VAR1=TABS_CLEAN, VAR2=SS); *UNWEIGHTED SAMPLE SIZES;

dsout(tabs_clean, "SS")

# PROC SORT DATA=NHIS.NHIS_CLASS;
# BY NSTRATUM NPSU;
# RUN;

nhis_class <- nhis_class %>% arrange(nstratum, npsu)

# *EX 7-1B: ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD BY DEMOGRAPHIC
# CHARACTERISTICS AND SAVE AS SAS DATATFILE--NOTE YOU CAN ALSO GET SAMPLE SIZE HERE INSTEAD OF PROC FREQ AT BEGINNIING;

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

# *CLEAN UP THE DATA FILE AND SAVE EACH OUTCOME (TOTAL, NO COPD, YES COPD) AS SEPARATE TEMPORARY
# FILES;

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

# %ROWTOCOL(3);

temp1 <- ex7_1 %>%
  filter(copd + 1 == 1) %>%
  transmute(row_name = row_label(sex, age, racegr), nsum1 = nsum, per_951 = "   ")

temp2 <- ex7_1 %>%
  filter(copd + 1 == 2) %>%
  transmute(row_name = row_label(sex, age, racegr), nsum2 = nsum, per_952 = fmt_ci(rowper, lowrow, uprow, 1))

temp3 <- ex7_1 %>%
  filter(copd + 1 == 3) %>%
  transmute(row_name = row_label(sex, age, racegr), nsum3 = nsum, per_953 = fmt_ci(rowper, lowrow, uprow, 1))

# *MERGE FILES TOGETHER;

# DATA TABLE1 (DROP=PER_951);
# MERGE TEMP1 TEMP2 TEMP3;
# RUN;

table1 <- bind_cols(
  temp1 %>% select(row_name, nsum1),
  temp2 %>% select(nsum2, per_952),
  temp3 %>% select(nsum3, per_953)
)

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

# PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
# CLASS  SEX AGE _RACEGR COPD ;
# TABLES  (SEX AGE  _RACEGR)*COPD;
# TEST CHISQ ;
# OUTPUT STESTVAL SDF SPVAL /STESTVALFMT=F12.4 SDFFMT=F12.4 SPVALFMT=F12.4 FILETYPE=SAS FILENAME=EX7_1TEST REPLACE;
# RUN;

# DATA T1_TEST (KEEP=VAR_NAME STESTVAL SDF SPVAL) ;
# RETAIN VAR_NAME STESTVAL SDF SPVAL;
# SET EX7_1TEST;
# IF TABLENO=1 THEN VAR_NAME='SEX   ';
# IF TABLENO=2 THEN VAR_NAME='AGE   ';
# IF TABLENO=3 THEN VAR_NAME='RACE   ';
# RUN;

t1_test <- ex7_1test %>%
  mutate(
    var_name = case_when(
      tableno == 1 ~ "SEX   ",
      tableno == 2 ~ "AGE   ",
      tableno == 3 ~ "RACE   ",
      TRUE ~ NA_character_
    )
  ) %>%
  select(var_name, stestval, sdf, spval)

# %DSOUT(VAR1=T1_TEST, VAR2=T1_TEST);

dsout(t1_test, "T1_TEST")

# **EX 7-2: NOW USE PROC DESCRIPT;
# **EX 7-2A: INLCUDE ALL OUTCOMES IN ONE COMMAND;

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

# DATA EX7_2A_CLEAN (KEEP=VAR_NAME NSUM PERCENT SEPERCENT LOWPCT UPPCT);
# RETAIN VAR_NAME NSUM PERCENT SEPERCENT LOWPCT UPPCT;
# SET EX7_2A;
# VAR_NAME=TRIM(PUT(SEX,SEXF.))||TRIM(PUT(AGE,AGEF.))||TRIM(PUT(_RACEGR,_RACEGRF.));
# RUN;

ex7_2a_clean <- ex7_2a %>%
  mutate(var_name = row_label(sex, age, racegr)) %>%
  select(var_name, nsum, percent, sepercent, lowpct, uppct)

# *NOW CREATE A MACRO TO ADD EACH OUTCOME AS A NEW COLUMN TO THE TABLE;
# *EX7-2B-JUST A DIFFERENT APPROACH FROM ABOVE-INSTEAD OF INCLUDING ALL OUTCOMES AT ONCE,
# CREATED A MACRO DO EACH OUTCOME SEPRATELY.  ADVANTAGE-MAY BE EASIER TO CLEAN DATA;

# %MACRO DESC(VAR1=, VAR2=);

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

desc_clean <- function(var1 = "copd", var2 = 1) {
  survey_descript_cat(nhis_design, var1, var2) %>%
    mutate(
      var_name = row_label(sex, age, racegr),
      per_95 = fmt_ci(percent, lowpct, uppct, 1)
    ) %>%
    select(var_name, nsum, per_95, percent, sepercent, lowpct, uppct)
}

# DATA EX7_2B_CLEAN&VAR2. (KEEP= VAR_NAME NSUM&VAR2. PER_95_&VAR2. PERCENT&VAR2. SEPERCENT&VAR2.
# LOWPCT&VAR2. UPPCT&VAR2.);
# RETAIN VAR_NAME NSUM&VAR2. PER_95_&VAR2. PERCENT&VAR2. SEPERCENT&VAR2.
# LOWPCT&VAR2. UPPCT&VAR2.;
# SET EX7_2B (RENAME=(NSUM=NSUM&VAR2. PERCENT=PERCENT&VAR2. SEPERCENT=SEPERCENT&VAR2.
# LOWPCT=LOWPCT&VAR2.  UPPCT=UPPCT&VAR2.)) ;
# VAR_NAME=TRIM(PUT(SEX,SEXF.))||TRIM(PUT(AGE,AGEF.))||TRIM(PUT(_RACEGR,_RACEGRF.));
# PER_95_&VAR2.=PUT(PERCENT&VAR2. ,4.1)||'('||PUT(LOWPCT&VAR2.,4.1)||','||PUT(UPPCT&VAR2.,4.1)||')';
# RUN;

# %MEND;

# %DESC(VAR1=COPD, VAR2=1);
# %DESC(VAR1=COPD, VAR2=2);

ex7_2b_clean1 <- desc_clean("copd", 1) %>%
  rename(nsum1 = nsum, per_95_1 = per_95, percent1 = percent, sepercent1 = sepercent, lowpct1 = lowpct, uppct1 = uppct)

ex7_2b_clean2 <- desc_clean("copd", 2) %>%
  rename(nsum2 = nsum, per_95_2 = per_95, percent2 = percent, sepercent2 = sepercent, lowpct2 = lowpct, uppct2 = uppct)

# *NOW CREATE YOUR OUTPUT TABLE;
# DATA EX7_2B_TABLE (KEEP=VAR_NAME NSUM1 PER_95_1 NSUM2 PER_95_2 ) ;
# MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
# RUN;

ex7_2b_table <- ex7_2b_clean1 %>%
  select(var_name, nsum1, per_95_1) %>%
  bind_cols(ex7_2b_clean2 %>% select(nsum2, per_95_2))

# %DSOUT(VAR1=EX_7B_TABLE, VAR2=TABLE2);

# The SAS DSOUT call references EX_7B_TABLE, while the DATA step creates EX7_2B_TABLE.
# The created table is exported here to preserve the intended workbook sheet.
dsout(ex7_2b_table, "TABLE2")

# *CREATE TABLE FOR GRAPHING;
# DATA EX7_2B_GRAPH (KEEP=VAR_NAME PERCENT1 SEPERCENT1 PERCENT2 SEPERCENT2)  ;
# MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
# RUN;

ex7_2b_graph <- ex7_2b_clean1 %>%
  select(var_name, percent1, sepercent1) %>%
  bind_cols(ex7_2b_clean2 %>% select(percent2, sepercent2))

# %DSOUT(VAR1=EX_7B_GRAPH, VAR2=T2_GRAPH);

# The SAS DSOUT call references EX_7B_GRAPH, while the DATA step creates EX7_2B_GRAPH.
# The created graphing table is exported here to preserve the intended workbook sheet.
dsout(ex7_2b_graph, "T2_GRAPH")

# **EX7_2C: PERFORM PAIRWISE TESTING AND TREND TESTS;

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

labels <- tibble(
  contrast = seq_len(28),
  contrast_name = c(
    "M-F", "A1-A2", "A1-A3", "A1-A4", "A1-A5", "A1-A6", "A2-A3", "A2-A4", "A2-A5",
    "A2-A6", "A3-A4", "A3-A5", "A3-A6", "A4-A5", "A4-A6", "A5-A6", "R1-R2", "R1-R3",
    "R1-R4", "R2-R3", "R2-R4", "R3-R4", "AGE-LINEAR", "AGE-QUAD",
    "WHITE-HISP-CONTRAST",
    "DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES",
    "WH-HIS-MALE",
    "WH-HISP-FEMALE"
  )
)

# DATA EX7_2C_CLEAN (KEEP=CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT);
# RETAIN CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT;
# MERGE EX7_2C (WHERE=(_ONE_=0)) LABELS;
# RUN;

ex7_2c_clean <- ex7_2c %>%
  filter(one == 0) %>%
  select(contrast, percent, sepercent, p_pct) %>%
  left_join(labels, by = "contrast") %>%
  select(contrast, contrast_name, percent, sepercent, p_pct)

# %DSOUT(VAR1=EX7_2C_CLEAN, VAR2=T1_TESTS);

dsout(ex7_2c_clean, "T1_TESTS")

# **EX7-3: LOGISTIC REGRESSION;
# *DEPENDENT VARIABLE IS BINARY (VALUES 0/1).
# *SUDAAN WILL MODEL THE PROBABILITY THAT THE RESPONSE VALUE=1;

# DATA TEMP;
# SET NHIS.NHIS_CLASS;
# IF COPD=1 THEN COPD_01=0;
# IF COPD=2 THEN COPD_01=1;
# RUN;

# *EX7-3B: RUN MODEL AND SAVE OUTPUT;

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

# data ORS_r;
# set ORS;
# RHS=_n_;
# run;

ors_r <- ors %>%
  mutate(rhs = row_number())

# data betas_r;
# set betas;
# RHS=_n_;
# run;

betas_r <- betas %>%
  mutate(rhs = row_number())

# *CLEAN UP OUTPUT FROM MODEL #1-MERGING THE P-VALUE TO THE ODDS RATIOS DATA FILE;

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

orbeta <- ors_r %>%
  select(rhs, lbl, or, lowor, upor) %>%
  left_join(betas_r %>% select(rhs, p_beta), by = "rhs") %>%
  mutate(or_95 = fmt_ci(or, lowor, upor, 2)) %>%
  select(rhs, lbl, or_95, p_beta)

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

ortest_r <- ortest %>%
  mutate(
    lbl = case_when(
      contrast == 1 ~ "OVERALL MODEL",
      contrast == 2 ~ "MODEL MINUS INTERCEPT",
      contrast == 3 ~ "INTERCEPT",
      contrast == 4 ~ "SEX",
      contrast == 5 ~ "AGE",
      contrast == 6 ~ "RACE_ETHNICITY",
      TRUE ~ lbl
    )
  ) %>%
  select(contrast, lbl, waldchi, waldchp)

# %DSOUT(VAR1=ORBETA, VAR2=OR_WITH_PVALUE); *ORS WITH P-VALUE;

dsout(orbeta, "OR_WITH_PVALUE")

# %DSOUT(VAR1=ORTEST_R, VAR2=OVERALL_MODEL_TESTS); *OVERALL MODEL TESTS-GLOBAL TEST;

dsout(ortest_r, "OVERALL_MODEL_TESTS")

openxlsx::saveWorkbook(wb, workbook_path, overwrite = TRUE)

print(paste("Saved Excel workbook:", workbook_path))