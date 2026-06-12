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
  library(rlang)
  library(stringr)
  library(survey)
  library(tibble)
  library(tidyr)
})

options(survey.lonely.psu = "adjust")

canonicalize_names <- function(x) {
  y <- tolower(x)
  y <- gsub("[^a-z0-9]+", "_", y)
  y <- gsub("^_+", "", y)
  y <- gsub("_+$", "", y)
  y <- ifelse(grepl("^[0-9]", y), paste0("x", y), y)
  y <- ifelse(y == "", "x", y)
  make.unique(y, sep = "_")
}

to_num <- function(x) {
  if (inherits(x, "haven_labelled")) {
    x <- haven::zap_labels(x)
  }
  suppressWarnings(as.numeric(x))
}

fmt_num <- function(x, digits = 1) {
  ifelse(is.na(x), "", sprintf(paste0("%.", digits, "f"), x))
}

fmt_ci <- function(est, lo, hi, digits = 1, sep = ",") {
  ifelse(
    is.na(est) | is.na(lo) | is.na(hi),
    "",
    paste0(fmt_num(est, digits), "(", fmt_num(lo, digits), sep, fmt_num(hi, digits), ")")
  )
}

# LIBNAME NHIS "C:\USERS\IYR4\ONEDRIVE - CDC\+MY_LARGE_WORKSPACE\DPH\NHIS_CLASS";*ASSIGN LIBRARY
# LOCATION FOR SAS DATA;
# SAS LIBNAME paths are treated as source-data hints. Runtime input is read from
# SAS2PY_INPUT_DIR, falling back to the current working directory.

input_dir <- Sys.getenv("SAS2PY_INPUT_DIR", unset = getwd())
nhis_path <- file.path(input_dir, "nhis_class.sas7bdat")

if (!file.exists(nhis_path)) {
  stop("Input file not found: ", nhis_path)
}

nhis_class <- haven::read_sas(nhis_path)
names(nhis_class) <- canonicalize_names(names(nhis_class))

required_vars <- c("nstratum", "npsu", "wtfa_sa", "sex", "age", "nchsage", "racegr", "copd")
missing_vars <- setdiff(required_vars, names(nhis_class))
if (length(missing_vars) > 0) {
  stop("Missing required columns after canonicalization: ", paste(missing_vars, collapse = ", "))
}

nhis_class <- nhis_class %>%
  mutate(
    across(all_of(required_vars), to_num)
  )

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
# R writes a single Excel workbook with matching worksheet names. The legacy SAS
# target used .XLS; openxlsx writes Office Open XML workbooks, so the relative
# artifact is saved as .xlsx.

sysdate_sas <- toupper(format(Sys.Date(), "%d%b%Y"))
output_workbook <- paste0("TABLES", sysdate_sas, ".xlsx")
wb <- openxlsx::createWorkbook()

dsout <- function(data, sheet_name) {
  sheet_name <- as.character(sheet_name)
  if (sheet_name %in% names(wb)) {
    openxlsx::removeWorksheet(wb, sheet_name)
  }
  openxlsx::addWorksheet(wb, sheet_name)
  openxlsx::writeData(wb, sheet = sheet_name, x = data)
  invisible(data)
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

nchsagef <- function(x) {
  dplyr::case_when(
    x == -2 ~ " ",
    x == 1 ~ "18-24",
    x == 2 ~ "25-34",
    x == 3 ~ "35-44",
    x == 4 ~ "45-64",
    x == 5 ~ "65+",
    TRUE ~ ""
  )
}

racegrf <- function(x) {
  dplyr::case_when(
    x == -2 ~ " ",
    x == 1 ~ "WHITE",
    x == 2 ~ "BLACK",
    x == 3 ~ "HISPANIC",
    x == 4 ~ "OTHER",
    TRUE ~ ""
  )
}

sexf <- function(x) {
  dplyr::case_when(
    x == -2 ~ " ",
    x == 1 ~ "MALE",
    x == 2 ~ "FEMALE",
    TRUE ~ ""
  )
}

agef <- function(x) {
  dplyr::case_when(
    x == -2 ~ " ",
    x == 1 ~ "18-24",
    x == 2 ~ "25-44",
    x == 3 ~ "45-54",
    x == 4 ~ "55-64",
    x == 5 ~ "65-74",
    x == 6 ~ "75+",
    TRUE ~ ""
  )
}

copdf <- function(x) {
  dplyr::case_when(
    x == -2 ~ " ",
    x == 1 ~ "NO COPD",
    x == 2 ~ "HAS COPD",
    TRUE ~ ""
  )
}

var_label <- function(var, value) {
  if (var == "sex") {
    sexf(value)
  } else if (var == "age") {
    agef(value)
  } else if (var == "racegr") {
    racegrf(value)
  } else if (var == "nchsage") {
    nchsagef(value)
  } else if (var == "copd") {
    copdf(value)
  } else {
    as.character(value)
  }
}

make_var_name <- function(sex = NA_real_, age = NA_real_, racegr = NA_real_) {
  paste0(sexf(sex), agef(age), racegrf(racegr))
}

survey_base_data <- nhis_class %>%
  filter(
    !is.na(.data[["nstratum"]]),
    !is.na(.data[["npsu"]]),
    !is.na(.data[["wtfa_sa"]]),
    .data[["wtfa_sa"]] > 0
  )

nhis_design <- survey::svydesign(
  ids = ~npsu,
  strata = ~nstratum,
  weights = ~wtfa_sa,
  data = survey_base_data,
  nest = TRUE
)

survey_prop <- function(data, domain_mask, indicator_mask, denominator_mask = rep(TRUE, nrow(data))) {
  stopifnot(length(domain_mask) == nrow(data))
  stopifnot(length(indicator_mask) == nrow(data))
  stopifnot(length(denominator_mask) == nrow(data))

  tmp <- data %>%
    mutate(
      domain_tmp = as.logical(domain_mask) & as.logical(denominator_mask),
      indicator_tmp = dplyr::if_else(as.logical(indicator_mask), 1, 0, missing = 0)
    )

  tmp <- tmp %>%
    filter(
      !is.na(.data[["nstratum"]]),
      !is.na(.data[["npsu"]]),
      !is.na(.data[["wtfa_sa"]]),
      .data[["wtfa_sa"]] > 0
    )

  nsum <- sum(tmp$domain_tmp & tmp$indicator_tmp == 1, na.rm = TRUE)
  nden <- sum(tmp$domain_tmp, na.rm = TRUE)

  if (nrow(tmp) == 0 || nden == 0) {
    return(tibble(nsum = nsum, percent = NA_real_, sepercent = NA_real_, lowpct = NA_real_, uppct = NA_real_))
  }

  des <- survey::svydesign(
    ids = ~npsu,
    strata = ~nstratum,
    weights = ~wtfa_sa,
    data = tmp,
    nest = TRUE
  )

  subdes <- subset(des, domain_tmp)
  est <- tryCatch(
    survey::svymean(~indicator_tmp, subdes, na.rm = TRUE),
    error = function(e) NULL
  )

  if (is.null(est)) {
    return(tibble(nsum = nsum, percent = NA_real_, sepercent = NA_real_, lowpct = NA_real_, uppct = NA_real_))
  }

  p <- as.numeric(stats::coef(est)[1])
  se <- as.numeric(survey::SE(est)[1])
  df <- survey::degf(subdes)
  crit <- if (is.finite(df) && df > 0) stats::qt(0.975, df = df) else stats::qnorm(0.975)

  tibble(
    nsum = nsum,
    percent = 100 * p,
    sepercent = 100 * se,
    lowpct = pmax(0, 100 * (p - crit * se)),
    uppct = pmin(100, 100 * (p + crit * se))
  )
}

survey_total <- function(data, domain_mask, indicator_mask) {
  tmp <- data %>%
    mutate(
      domain_tmp = as.logical(domain_mask),
      indicator_tmp = dplyr::if_else(as.logical(indicator_mask), 1, 0, missing = 0)
    ) %>%
    filter(
      !is.na(.data[["nstratum"]]),
      !is.na(.data[["npsu"]]),
      !is.na(.data[["wtfa_sa"]]),
      .data[["wtfa_sa"]] > 0
    )

  nsum <- sum(tmp$domain_tmp & tmp$indicator_tmp == 1, na.rm = TRUE)

  if (nrow(tmp) == 0 || !any(tmp$domain_tmp, na.rm = TRUE)) {
    return(tibble(nsum = nsum, wsum = NA_real_, sewgt = NA_real_, lowtot = NA_real_, uptot = NA_real_))
  }

  des <- survey::svydesign(
    ids = ~npsu,
    strata = ~nstratum,
    weights = ~wtfa_sa,
    data = tmp,
    nest = TRUE
  )
  subdes <- subset(des, domain_tmp)
  est <- tryCatch(survey::svytotal(~indicator_tmp, subdes, na.rm = TRUE), error = function(e) NULL)

  if (is.null(est)) {
    return(tibble(nsum = nsum, wsum = NA_real_, sewgt = NA_real_, lowtot = NA_real_, uptot = NA_real_))
  }

  total <- as.numeric(stats::coef(est)[1])
  se <- as.numeric(survey::SE(est)[1])
  df <- survey::degf(subdes)
  crit <- if (is.finite(df) && df > 0) stats::qt(0.975, df = df) else stats::qnorm(0.975)

  tibble(
    nsum = nsum,
    wsum = total,
    sewgt = se,
    lowtot = pmax(0, total - crit * se),
    uptot = total + crit * se
  )
}

# PROC CONTENTS DATA=NHIS.NHIS_CLASS;
# RUN;

contents_nhis_class <- tibble(
  name = names(nhis_class),
  class = vapply(nhis_class, function(x) paste(class(x), collapse = ","), character(1)),
  n_missing = vapply(nhis_class, function(x) sum(is.na(x)), integer(1))
)
print(contents_nhis_class)

# *VIEW DATA;
# PROC FREQ DATA=NHIS.NHIS_CLASS;
# TABLES SEX AGE NCHSAGE _RACEGR COPD;
# RUN;

view_freq <- bind_rows(
  nhis_class %>% count(sex, name = "frequency") %>% transmute(variable = "sex", value = sex, label = sexf(sex), frequency),
  nhis_class %>% count(age, name = "frequency") %>% transmute(variable = "age", value = age, label = agef(age), frequency),
  nhis_class %>% count(nchsage, name = "frequency") %>% transmute(variable = "nchsage", value = nchsage, label = nchsagef(nchsage), frequency),
  nhis_class %>% count(racegr, name = "frequency") %>% transmute(variable = "racegr", value = racegr, label = racegrf(racegr), frequency),
  nhis_class %>% count(copd, name = "frequency") %>% transmute(variable = "copd", value = copd, label = copdf(copd), frequency)
)
print(view_freq)

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

ex7_1a <- map_dfr(c(1, 2), function(level) {
  domain_mask <- nhis_class$copd %in% c(1, 2)
  indicator_mask <- nhis_class$copd == level
  pct <- survey_prop(nhis_class, domain_mask, indicator_mask)
  tot <- survey_total(nhis_class, domain_mask, indicator_mask)
  tibble(
    copd = level,
    copd_label = copdf(level),
    nsum = pct$nsum,
    wsum = tot$wsum,
    sewgt = tot$sewgt,
    totper = pct$percent,
    setot = pct$sepercent,
    lowtot = pct$lowpct,
    uptot = pct$uppct
  )
})
print(ex7_1a)

# *EX 7-1B: SAVE RESULTS TO SAS DATA FILE
# ESTIMATE PROPORTION (OR PERCENTAGE) OF ADULTS WITH COPD BY DEMOGRAPHIC
# CHARACTERISTICS AND SAVE AS SAS DATATFILE;

make_crosstab_group <- function(data, group_var, levels_vec, copd_levels = c(1, 2)) {
  map_dfr(levels_vec, function(group_level) {
    domain_mask <- data[[group_var]] == group_level & data$copd %in% copd_levels

    total_row <- tibble(
      sex = NA_real_,
      age = NA_real_,
      racegr = NA_real_,
      copd = 0,
      nsum = sum(domain_mask, na.rm = TRUE),
      rowper = 100,
      serow = 0,
      lowrow = 100,
      uprow = 100
    )
    total_row[[group_var]] <- group_level

    level_rows <- map_dfr(copd_levels, function(copd_level) {
      est <- survey_prop(
        data = data,
        domain_mask = domain_mask,
        indicator_mask = data$copd == copd_level,
        denominator_mask = rep(TRUE, nrow(data))
      )
      row <- tibble(
        sex = NA_real_,
        age = NA_real_,
        racegr = NA_real_,
        copd = copd_level,
        nsum = est$nsum,
        rowper = est$percent,
        serow = est$sepercent,
        lowrow = est$lowpct,
        uprow = est$uppct
      )
      row[[group_var]] <- group_level
      row
    })

    bind_rows(total_row, level_rows)
  })
}

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
  make_crosstab_group(nhis_class, "sex", 1:2),
  make_crosstab_group(nhis_class, "age", 1:6),
  make_crosstab_group(nhis_class, "racegr", 1:4)
)
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

svy_chisq_test <- function(data, group_var, tableno) {
  tmp <- data %>%
    filter(
      !is.na(.data[[group_var]]),
      .data[["copd"]] %in% c(1, 2),
      !is.na(.data[["nstratum"]]),
      !is.na(.data[["npsu"]]),
      !is.na(.data[["wtfa_sa"]]),
      .data[["wtfa_sa"]] > 0
    ) %>%
    mutate(
      group_tmp = factor(.data[[group_var]]),
      copd_tmp = factor(.data[["copd"]])
    )

  if (nrow(tmp) == 0 || n_distinct(tmp$group_tmp) < 2 || n_distinct(tmp$copd_tmp) < 2) {
    return(tibble(tableno = tableno, stestval = NA_real_, sdf = NA_real_, spval = NA_real_))
  }

  des <- survey::svydesign(
    ids = ~npsu,
    strata = ~nstratum,
    weights = ~wtfa_sa,
    data = tmp,
    nest = TRUE
  )

  tst <- tryCatch(
    survey::svychisq(~group_tmp + copd_tmp, des, statistic = "F"),
    error = function(e) NULL
  )

  if (is.null(tst)) {
    tibble(tableno = tableno, stestval = NA_real_, sdf = NA_real_, spval = NA_real_)
  } else {
    tibble(
      tableno = tableno,
      stestval = unname(as.numeric(tst$statistic)),
      sdf = unname(as.numeric(tst$parameter[1])),
      spval = unname(as.numeric(tst$p.value))
    )
  }
}

# PROC CROSSTAB DATA=NHIS.NHIS_CLASS  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT */
# CLASS  SEX AGE _RACEGR COPD ;
# TABLES  (SEX AGE  _RACEGR)*COPD;
# TEST CHISQ ;
# OUTPUT STESTVAL SDF SPVAL /STESTVALFMT=F12.4 SDFFMT=F12.4 SPVALFMT=F12.4 FILETYPE=SAS FILENAME=EX7_1TEST REPLACE;
# RUN;

ex7_1test <- bind_rows(
  svy_chisq_test(nhis_class, "sex", 1),
  svy_chisq_test(nhis_class, "age", 2),
  svy_chisq_test(nhis_class, "racegr", 3)
)
print(ex7_1test)

# ***********************************************************************
# *	DESCRIPT
# **********************************************************************

# **EX 7-2A: ESTIMATE PREVALENCE AND SAVE TO SAS DATAFILE INLCUDE ALL OUTCOMES IN ONE COMMAND;

make_descript_group <- function(data, group_var, levels_vec, catlevels = c(1, 2), var_name = "copd") {
  map_dfr(levels_vec, function(group_level) {
    map_dfr(catlevels, function(catlevel) {
      domain_mask <- data[[group_var]] == group_level & !is.na(data[[var_name]])
      est <- survey_prop(data, domain_mask, data[[var_name]] == catlevel)
      row <- tibble(
        sex = NA_real_,
        age = NA_real_,
        racegr = NA_real_,
        copd = catlevel,
        nsum = est$nsum,
        percent = est$percent,
        sepercent = est$sepercent,
        lowpct = est$lowpct,
        uppct = est$uppct
      )
      row[[group_var]] <- group_level
      row
    })
  })
}

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
  make_descript_group(nhis_class, "sex", 1:2),
  make_descript_group(nhis_class, "age", 1:6),
  make_descript_group(nhis_class, "racegr", 1:4)
)
print(ex7_2a)

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

# SUDAAN pairwise, POLY, and CONTRAST statements do not have a one-line exact R
# equivalent. The code below uses design-based prevalence estimates and
# linearized survey GLMs. Pairwise SEs are computed from model/design covariance
# where feasible; empty or non-estimable contrasts return NA.

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
      .data[["copd"]] == 1 ~ 0,
      .data[["copd"]] == 2 ~ 1,
      TRUE ~ NA_real_
    )
  )

safe_factor_ref <- function(x, requested_ref) {
  vals <- sort(unique(x[!is.na(x)]))
  lev <- as.character(vals)
  f <- factor(as.character(x), levels = lev)
  ref_chr <- as.character(requested_ref)
  if (ref_chr %in% levels(f)) {
    stats::relevel(f, ref = ref_chr)
  } else {
    # Requested REFLEVEL is not present after input normalization; keep the first
    # observed level to avoid hard-coding a nonexistent reference.
    f
  }
}

model_data <- temp %>%
  filter(
    !is.na(.data[["copd_01"]]),
    !is.na(.data[["sex"]]),
    !is.na(.data[["age"]]),
    !is.na(.data[["racegr"]]),
    !is.na(.data[["nstratum"]]),
    !is.na(.data[["npsu"]]),
    !is.na(.data[["wtfa_sa"]]),
    .data[["wtfa_sa"]] > 0
  ) %>%
  mutate(
    sex_f = safe_factor_ref(.data[["sex"]], 1),
    age_f = safe_factor_ref(.data[["age"]], 1),
    racegr_f = safe_factor_ref(.data[["racegr"]], 1)
  )

model_design <- survey::svydesign(
  ids = ~npsu,
  strata = ~nstratum,
  weights = ~wtfa_sa,
  data = model_data,
  nest = TRUE
)

# *EX7-3A: RUN MODEL AND PRINT ODDS RATIOS;

# PROC RLOGIST DATA=TEMP  DESIGN = WR  NOTSORTED;
# NEST   NSTRATUM NPSU / MISSUNIT PSULEV=2  ;
# WEIGHT  WTFA_SA  ;   /* WEIGHT VARIABLE FOR SAMPLE ADULT*/
# CLASS  SEX AGE _RACEGR ;
# MODEL COPD_01 = SEX AGE _RACEGR;
# PRINT;
# RUN;

rlogist_fit <- survey::svyglm(
  copd_01 ~ sex_f + age_f + racegr_f,
  design = model_design,
  family = quasibinomial()
)

print(summary(rlogist_fit))

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

make_logistic_outputs <- function(fit) {
  co <- stats::coef(fit)
  vc <- stats::vcov(fit)
  se <- sqrt(diag(vc))
  df <- survey::degf(fit$survey.design)
  crit <- if (is.finite(df) && df > 0) stats::qt(0.975, df = df) else stats::qnorm(0.975)

  coef_tbl <- tibble(
    term = names(co),
    beta = as.numeric(co),
    sebeta = as.numeric(se[names(co)]),
    t_beta = beta / sebeta,
    p_beta = 2 * stats::pt(abs(t_beta), df = df, lower.tail = FALSE),
    or = exp(beta),
    lowor = exp(beta - crit * sebeta),
    upor = exp(beta + crit * sebeta)
  )

  get_term <- function(prefix, level) {
    nm <- paste0(prefix, level)
    if (nm %in% coef_tbl$term) {
      coef_tbl %>% filter(.data[["term"]] == nm) %>% slice(1)
    } else {
      tibble(term = nm, beta = NA_real_, sebeta = NA_real_, t_beta = NA_real_, p_beta = NA_real_, or = 1, lowor = NA_real_, upor = NA_real_)
    }
  }

  ors <- bind_rows(
    coef_tbl %>% filter(.data[["term"]] == "(Intercept)"),
    get_term("sex_f", 1),
    get_term("sex_f", 2),
    get_term("age_f", 1),
    get_term("age_f", 2),
    get_term("age_f", 3),
    get_term("age_f", 4),
    get_term("age_f", 5),
    get_term("age_f", 6),
    get_term("racegr_f", 1),
    get_term("racegr_f", 2),
    get_term("racegr_f", 3),
    get_term("racegr_f", 4)
  ) %>%
    mutate(rhs = row_number()) %>%
    select(rhs, term, or, lowor, upor)

  betas <- bind_rows(
    coef_tbl %>% filter(.data[["term"]] == "(Intercept)"),
    get_term("sex_f", 1),
    get_term("sex_f", 2),
    get_term("age_f", 1),
    get_term("age_f", 2),
    get_term("age_f", 3),
    get_term("age_f", 4),
    get_term("age_f", 5),
    get_term("age_f", 6),
    get_term("racegr_f", 1),
    get_term("racegr_f", 2),
    get_term("racegr_f", 3),
    get_term("racegr_f", 4)
  ) %>%
    mutate(rhs = row_number(), deft = NA_real_) %>%
    select(rhs, term, beta, deft, p_beta, sebeta, t_beta)

  list(ors = ors, betas = betas)
}

logistic_outputs <- make_logistic_outputs(rlogist_fit)
ors <- logistic_outputs$ors
betas <- logistic_outputs$betas

term_test <- function(fit, formula_term, contrast_id, label) {
  tst <- tryCatch(survey::regTermTest(fit, formula_term), error = function(e) NULL)
  if (is.null(tst)) {
    return(tibble(contrast = contrast_id, lbl = label, waldchi = NA_real_, waldchp = NA_real_))
  }
  fstat <- as.numeric(tst$Ftest)
  ndf <- as.numeric(tst$df)
  tibble(
    contrast = contrast_id,
    lbl = label,
    waldchi = fstat * ndf,
    waldchp = as.numeric(tst$p)
  )
}

intercept_test <- function(fit) {
  s <- summary(fit)$coefficients
  if (!("(Intercept)" %in% rownames(s))) {
    return(tibble(contrast = 3, lbl = "INTERCEPT", waldchi = NA_real_, waldchp = NA_real_))
  }
  tval <- as.numeric(s["(Intercept)", "t value"])
  pval <- as.numeric(s["(Intercept)", "Pr(>|t|)"])
  tibble(contrast = 3, lbl = "INTERCEPT", waldchi = tval^2, waldchp = pval)
}

ortest <- bind_rows(
  term_test(rlogist_fit, ~sex_f + age_f + racegr_f, 1, "OVERALL MODEL"),
  term_test(rlogist_fit, ~sex_f + age_f + racegr_f, 2, "MODEL MINUS INTERCEPT"),
  intercept_test(rlogist_fit),
  term_test(rlogist_fit, ~sex_f, 4, "SEX"),
  term_test(rlogist_fit, ~age_f, 5, "AGE"),
  term_test(rlogist_fit, ~racegr_f, 6, "RACE_ETHNICITY")
)

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

rlogist_fit_c <- survey::svyglm(
  copd_01 ~ sex_f + age_f * racegr_f,
  design = model_design,
  family = quasibinomial()
)

logistic_outputs_c <- make_logistic_outputs(rlogist_fit_c)
ors_c <- logistic_outputs_c$ors
betas_c <- logistic_outputs_c$betas
ortest_c <- bind_rows(
  term_test(rlogist_fit_c, ~sex_f + age_f * racegr_f, 1, "OVERALL MODEL"),
  term_test(rlogist_fit_c, ~sex_f + age_f * racegr_f, 2, "MODEL MINUS INTERCEPT"),
  intercept_test(rlogist_fit_c),
  term_test(rlogist_fit_c, ~sex_f, 4, "SEX"),
  term_test(rlogist_fit_c, ~age_f, 5, "AGE"),
  term_test(rlogist_fit_c, ~racegr_f, 6, "RACE_ETHNICITY"),
  term_test(rlogist_fit_c, ~age_f:racegr_f, 7, "AGE BY RACE_ETHNICITY")
)

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

pred_margin_one <- function(fit, data, var, level) {
  newdata <- data
  fvar <- paste0(var, "_f")
  newdata[[fvar]] <- factor(as.character(level), levels = levels(data[[fvar]]))
  pred <- stats::predict(fit, newdata = newdata, type = "response", se.fit = FALSE)
  ok <- is.finite(pred) & is.finite(newdata$wtfa_sa)
  if (!any(ok)) {
    return(tibble(variable = var, level = level, predmrg = NA_real_, seprdmrg = NA_real_))
  }

  predmrg <- stats::weighted.mean(pred[ok], w = newdata$wtfa_sa[ok])

  mm <- stats::model.matrix(stats::delete.response(stats::terms(fit)), newdata)
  beta <- stats::coef(fit)
  mm <- mm[, names(beta), drop = FALSE]
  eta <- as.vector(mm %*% beta)
  p <- stats::plogis(eta)
  deriv <- p * (1 - p)
  w <- newdata$wtfa_sa
  ok2 <- ok & complete.cases(mm)
  if (!any(ok2)) {
    seprdmrg <- NA_real_
  } else {
    w_norm <- w[ok2] / sum(w[ok2])
    grad <- colSums(mm[ok2, , drop = FALSE] * (deriv[ok2] * w_norm))
    seprdmrg <- sqrt(as.numeric(t(grad) %*% stats::vcov(fit) %*% grad))
  }

  tibble(variable = var, level = level, predmrg = predmrg, seprdmrg = seprdmrg)
}

pm <- bind_rows(
  map_dfr(levels(model_data$sex_f), ~pred_margin_one(rlogist_fit, model_data, "sex", .x)),
  map_dfr(levels(model_data$age_f), ~pred_margin_one(rlogist_fit, model_data, "age", .x)),
  map_dfr(levels(model_data$racegr_f), ~pred_margin_one(rlogist_fit, model_data, "racegr", .x))
)

prev_rat <- pm %>%
  group_by(.data[["variable"]]) %>%
  mutate(
    ref_pred = first(.data[["predmrg"]]),
    pred_rr = .data[["predmrg"]] / .data[["ref_pred"]],
    pred_serr = NA_real_,
    pred_lowrr = NA_real_,
    pred_uprr = NA_real_
  ) %>%
  ungroup() %>%
  select(variable, level, pred_rr, pred_serr, pred_lowrr, pred_uprr)

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
  nhis_class %>% filter(!is.na(.data[["copd"]])) %>% count(sex, name = "frequency") %>% transmute(variable = "sex", value = sex, f_sex = sexf(sex), f_age = "", f_racegr = "", frequency),
  nhis_class %>% filter(!is.na(.data[["copd"]])) %>% count(age, name = "frequency") %>% transmute(variable = "age", value = age, f_sex = "", f_age = agef(age), f_racegr = "", frequency),
  nhis_class %>% filter(!is.na(.data[["copd"]])) %>% count(racegr, name = "frequency") %>% transmute(variable = "racegr", value = racegr, f_sex = "", f_age = "", f_racegr = racegrf(racegr), frequency)
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
  mutate(
    lbl = paste0(.data[["f_sex"]], .data[["f_age"]], .data[["f_racegr"]]),
    order = row_number()
  ) %>%
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

tabs_clean <- tabs_clean %>% arrange(.data[["order"]])

# %DSOUT(VAR1=TABS_CLEAN, VAR2=SS); *UNWEIGHTED SAMPLE SIZES;

dsout(tabs_clean, "SS")

# PROC SORT DATA=NHIS.NHIS_CLASS;
# BY NSTRATUM NPSU;
# RUN;

nhis_class_sorted <- nhis_class %>% arrange(.data[["nstratum"]], .data[["npsu"]])

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

rowtocol <- function(ex7_1_data, i) {
  copd_value <- i - 1
  out <- ex7_1_data %>%
    filter(.data[["copd"]] == copd_value) %>%
    mutate(
      row_name = make_var_name(.data[["sex"]], .data[["age"]], .data[["racegr"]]),
      nsum_tmp = .data[["nsum"]],
      per_95_tmp = if_else(
        .data[["copd"]] == 0,
        "   ",
        fmt_ci(.data[["rowper"]], .data[["lowrow"]], .data[["uprow"]], digits = 1, sep = ",")
      )
    ) %>%
    select(row_name, nsum_tmp, per_95_tmp)

  names(out) <- c("row_name", paste0("nsum", i), paste0("per_95", i))
  out
}

# %ROWTOCOL(3);

temp1 <- rowtocol(ex7_1, 1)
temp2 <- rowtocol(ex7_1, 2)
temp3 <- rowtocol(ex7_1, 3)

# *MERGE FILES TOGETHER;

# DATA TABLE1 (DROP=PER_951);
# MERGE TEMP1 TEMP2 TEMP3;
# RUN;

table1 <- temp1 %>%
  select(-per_951) %>%
  full_join(temp2, by = "row_name") %>%
  full_join(temp3, by = "row_name")

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
# %DSOUT(VAR1=T1_TEST, VAR2=T1_TEST);

t1_test <- ex7_1test %>%
  mutate(
    var_name = case_when(
      .data[["tableno"]] == 1 ~ "SEX   ",
      .data[["tableno"]] == 2 ~ "AGE   ",
      .data[["tableno"]] == 3 ~ "RACE   ",
      TRUE ~ ""
    )
  ) %>%
  select(var_name, stestval, sdf, spval)

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
  mutate(var_name = make_var_name(.data[["sex"]], .data[["age"]], .data[["racegr"]])) %>%
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

desc <- function(var1, var2) {
  ex7_2b <- bind_rows(
    make_descript_group(nhis_class, "sex", 1:2, catlevels = var2, var_name = var1),
    make_descript_group(nhis_class, "age", 1:6, catlevels = var2, var_name = var1),
    make_descript_group(nhis_class, "racegr", 1:4, catlevels = var2, var_name = var1)
  )

  out <- ex7_2b %>%
    mutate(
      var_name = make_var_name(.data[["sex"]], .data[["age"]], .data[["racegr"]]),
      per_95 = fmt_ci(.data[["percent"]], .data[["lowpct"]], .data[["uppct"]], digits = 1, sep = ",")
    ) %>%
    select(var_name, nsum, per_95, percent, sepercent, lowpct, uppct)

  names(out) <- c(
    "var_name",
    paste0("nsum", var2),
    paste0("per_95_", var2),
    paste0("percent", var2),
    paste0("sepercent", var2),
    paste0("lowpct", var2),
    paste0("uppct", var2)
  )
  out
}

# %DESC(VAR1=COPD, VAR2=1);
# %DESC(VAR1=COPD, VAR2=2);

ex7_2b_clean1 <- desc("copd", 1)
ex7_2b_clean2 <- desc("copd", 2)

# *NOW CREATE YOUR OUTPUT TABLE;
# DATA EX7_2B_TABLE (KEEP=VAR_NAME NSUM1 PER_95_1 NSUM2 PER_95_2 ) ;
# MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
# RUN;

ex7_2b_table <- ex7_2b_clean1 %>%
  full_join(ex7_2b_clean2, by = "var_name") %>%
  select(var_name, nsum1, per_95_1, nsum2, per_95_2)

# %DSOUT(VAR1=EX_7B_TABLE, VAR2=TABLE2);

# The SAS macro call references EX_7B_TABLE, while the created dataset is
# EX7_2B_TABLE. This translation writes the intended table.
dsout(ex7_2b_table, "TABLE2")

# *CREATE TABLE FOR GRAPHING;
# DATA EX7_2B_GRAPH (KEEP=VAR_NAME PERCENT1 SEPERCENT1 PERCENT2 SEPERCENT2)  ;
# MERGE EX7_2B_CLEAN1 EX7_2B_CLEAN2;
# RUN;

ex7_2b_graph <- ex7_2b_clean1 %>%
  full_join(ex7_2b_clean2, by = "var_name") %>%
  select(var_name, percent1, sepercent1, percent2, sepercent2)

# %DSOUT(VAR1=EX_7B_GRAPH, VAR2=T2_GRAPH);

# The SAS macro call references EX_7B_GRAPH, while the created dataset is
# EX7_2B_GRAPH. This translation writes the intended graphing table.
dsout(ex7_2b_graph, "T2_GRAPH")

# **EX7_2C: PERFORM PAIRWISE TESTING AND TREND TESTS;

contrast_labels <- c(
  "M-F", "A1-A2", "A1-A3", "A1-A4", "A1-A5", "A1-A6", "A2-A3", "A2-A4", "A2-A5",
  "A2-A6", "A3-A4", "A3-A5", "A3-A6", "A4-A5", "A4-A6", "A5-A6", "R1-R2", "R1-R3",
  "R1-R4", "R2-R3", "R2-R4", "R3-R4", "AGE-LINEAR", "AGE-QUAD",
  "WHITE-HISP-CONTRAST",
  "DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES",
  "WH-HIS-MALE",
  "WH-HISP-FEMALE"
)

fit_prevalence_model <- function(formula_text) {
  d <- temp %>%
    filter(
      !is.na(.data[["copd_01"]]),
      !is.na(.data[["nstratum"]]),
      !is.na(.data[["npsu"]]),
      !is.na(.data[["wtfa_sa"]]),
      .data[["wtfa_sa"]] > 0
    ) %>%
    mutate(
      sex_f = safe_factor_ref(.data[["sex"]], 1),
      age_f = safe_factor_ref(.data[["age"]], 1),
      racegr_f = safe_factor_ref(.data[["racegr"]], 1)
    )

  des <- survey::svydesign(ids = ~npsu, strata = ~nstratum, weights = ~wtfa_sa, data = d, nest = TRUE)
  survey::svyglm(stats::as.formula(formula_text), design = des, family = quasibinomial())
}

prev_fit_main <- fit_prevalence_model("copd_01 ~ sex_f + age_f + racegr_f")
prev_fit_int <- fit_prevalence_model("copd_01 ~ sex_f * racegr_f + age_f")

make_pairwise_from_estimates <- function(group_var, levels_vec) {
  ests <- map_dfr(levels_vec, function(lv) {
    domain_mask <- nhis_class[[group_var]] == lv & nhis_class$copd %in% c(1, 2)
    est <- survey_prop(nhis_class, domain_mask, nhis_class$copd == 1)
    tibble(level = lv, percent = est$percent, sepercent = est$sepercent)
  })

  pairs <- utils::combn(levels_vec, 2, simplify = FALSE)
  map_dfr(pairs, function(p) {
    a <- ests %>% filter(.data[["level"]] == p[1]) %>% slice(1)
    b <- ests %>% filter(.data[["level"]] == p[2]) %>% slice(1)
    diff <- a$percent - b$percent
    se_diff <- sqrt(a$sepercent^2 + b$sepercent^2)
    df <- survey::degf(nhis_design)
    pval <- ifelse(is.na(se_diff) || se_diff == 0, NA_real_, 2 * stats::pt(abs(diff / se_diff), df = df, lower.tail = FALSE))
    tibble(percent = diff, sepercent = se_diff, p_pct = pval)
  })
}

age_trend_tests <- function() {
  d <- temp %>%
    filter(
      !is.na(.data[["copd_01"]]),
      !is.na(.data[["age"]]),
      !is.na(.data[["nstratum"]]),
      !is.na(.data[["npsu"]]),
      !is.na(.data[["wtfa_sa"]]),
      .data[["wtfa_sa"]] > 0
    ) %>%
    mutate(
      age_linear = case_when(age == 1 ~ -5, age == 2 ~ -3, age == 3 ~ -1, age == 4 ~ 1, age == 5 ~ 3, age == 6 ~ 5, TRUE ~ NA_real_),
      age_quad = case_when(age == 1 ~ 5, age == 2 ~ -1, age == 3 ~ -4, age == 4 ~ -4, age == 5 ~ -1, age == 6 ~ 5, TRUE ~ NA_real_)
    ) %>%
    filter(!is.na(.data[["age_linear"]]), !is.na(.data[["age_quad"]]))

  des <- survey::svydesign(ids = ~npsu, strata = ~nstratum, weights = ~wtfa_sa, data = d, nest = TRUE)
  lin_fit <- survey::svyglm(copd_01 ~ age_linear, design = des, family = quasibinomial())
  quad_fit <- survey::svyglm(copd_01 ~ age_quad, design = des, family = quasibinomial())

  lin <- summary(lin_fit)$coefficients["age_linear", ]
  quad <- summary(quad_fit)$coefficients["age_quad", ]

  tibble(
    percent = c(100 * unname(lin["Estimate"]), 100 * unname(quad["Estimate"])),
    sepercent = c(100 * unname(lin["Std. Error"]), 100 * unname(quad["Std. Error"])),
    p_pct = c(unname(lin["Pr(>|t|)"]), unname(quad["Pr(>|t|)"]))
  )
}

special_contrast_rows <- function() {
  # Design-based model approximations for the explicit SUDAAN CONTRAST statements.
  race_est <- make_pairwise_from_estimates("racegr", c(1, 3)) %>% slice(1)

  interaction_test <- term_test(prev_fit_int, ~sex_f:racegr_f, 1, "interaction")
  df <- survey::degf(prev_fit_int$survey.design)

  mk <- function(name) {
    tibble(percent = NA_real_, sepercent = NA_real_, p_pct = NA_real_, name = name)
  }

  bind_rows(
    race_est %>% mutate(name = "WHITE-HISP-CONTRAST"),
    tibble(percent = NA_real_, sepercent = NA_real_, p_pct = interaction_test$waldchp, name = "DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES"),
    mk("WH-HIS-MALE"),
    mk("WH-HISP-FEMALE")
  ) %>%
    select(percent, sepercent, p_pct)
}

ex7_2c_core <- bind_rows(
  make_pairwise_from_estimates("sex", 1:2),
  make_pairwise_from_estimates("age", 1:6),
  make_pairwise_from_estimates("racegr", 1:4),
  age_trend_tests(),
  special_contrast_rows()
) %>%
  mutate(contrast = row_number())

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
  contrast = seq_along(contrast_labels),
  contrast_name = contrast_labels
)

# DATA EX7_2C_CLEAN (KEEP=CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT);
# RETAIN CONTRAST CONTRAST_NAME PERCENT SEPERCENT P_PCT;
# MERGE EX7_2C (WHERE=(_ONE_=0)) LABELS;
# RUN;

ex7_2c_clean <- labels %>%
  left_join(ex7_2c_core, by = "contrast") %>%
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

ors_r <- ors %>% mutate(rhs = row_number())

# data betas_r;
# set betas;
# RHS=_n_;
# run;

betas_r <- betas %>% mutate(rhs = row_number())

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

or_labels <- c(
  "INTERCEPT",
  "MALE",
  "FEMALE",
  "18-24 YEARS",
  "25-34 YEARS",
  "35-44 YEARS",
  "44-65 YEARS",
  "65-74 YEARS",
  "75+ YEARS",
  "WHITE, NON-",
  "BLACK, NON-",
  "HISPANIC",
  "OTHER, NON-HISP"
)

orbeta <- ors_r %>%
  select(rhs, or, lowor, upor) %>%
  left_join(betas_r %>% select(rhs, p_beta), by = "rhs") %>%
  mutate(
    lbl = or_labels[.data[["rhs"]]],
    or_95 = fmt_ci(.data[["or"]], .data[["lowor"]], .data[["upor"]], digits = 2, sep = ",")
  ) %>%
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
      .data[["contrast"]] == 1 ~ "OVERALL MODEL",
      .data[["contrast"]] == 2 ~ "MODEL MINUS INTERCEPT",
      .data[["contrast"]] == 3 ~ "INTERCEPT",
      .data[["contrast"]] == 4 ~ "SEX",
      .data[["contrast"]] == 5 ~ "AGE",
      .data[["contrast"]] == 6 ~ "RACE_ETHNICITY",
      TRUE ~ .data[["lbl"]]
    )
  ) %>%
  select(contrast, lbl, waldchi, waldchp)

# %DSOUT(VAR1=ORBETA, VAR2=OR_WITH_PVALUE); *ORS WITH P-VALUE;
# %DSOUT(VAR1=ORTEST_R, VAR2=OVERALL_MODEL_TESTS); *OVERALL MODEL TESTS-GLOBAL TEST;

dsout(orbeta, "OR_WITH_PVALUE")
dsout(ortest_r, "OVERALL_MODEL_TESTS")

openxlsx::saveWorkbook(wb, output_workbook, overwrite = TRUE)

print(paste("Saved Excel workbook:", output_workbook))