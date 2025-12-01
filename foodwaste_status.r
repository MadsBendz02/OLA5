library(dplyr)
library(lubridate)
library(DBI)
library(RMariaDB)


# Sikkerhedstjek (stop hvis hentdata ikke er kørt endnu)
if (!file.exists("variable_all.rds")) {
  stop("FEJL: variable_all.rds findes ikke. Kør foodwaste_hentdata.r først.")
}

if (!file.exists("stamdata.rds")) {
  stop("FEJL: stamdata.rds findes ikke. Kør foodwaste_hentdata.r først.")
}

# LÆS RDS-FILERNE (DETTE MANGLER I DIN KODE)
variable_all <- readRDS("variable_all.rds")
stamdata     <- readRDS("stamdata.rds")

# Tilføj snapshot_date og expiry_date
variable <- variable_all %>%
  mutate(
    snapshot_date = as.Date(timestamp_pipeline),
    expiry_date   = as.Date(offer.endTime)
  )

# Find i går og i dag
dates <- sort(unique(variable$snapshot_date))

if (length(dates) < 2) {
  stop("Der er kun én scraping-dag i historikken. Kan ikke beregne status endnu.")
}

yesterday <- dates[length(dates) - 1]
today     <- dates[length(dates)]

key_cols <- c("store.id", "offer.ean", "offer.startTime")

offers_y <- filter(variable, snapshot_date == yesterday)
offers_t <- filter(variable, snapshot_date == today)

# 1: På lager (findes i dag og i går)
on_stock <- inner_join(
  offers_y[, c(key_cols, "expiry_date")],
  offers_t[, key_cols],
  by = key_cols
) %>%
  distinct(store.id, offer.ean, offer.startTime, .keep_all = TRUE) %>%
  mutate(
    snapshot_date = today,
    status = "På lager"
  )

# 2: Forsvundet (var i går men ikke i dag)
disappeared <- anti_join(
  offers_y[, c(key_cols, "expiry_date")],
  offers_t[, key_cols],
  by = key_cols
) %>%
  mutate(
    snapshot_date = today,
    status = if_else(
      expiry_date == today,
      "Smidt ud",
      "Solgt"
    )
  )

# 3: Samlet status
offers_status <- bind_rows(on_stock, disappeared)

# 4: Join status på dagens variable
variable_today <- filter(variable, snapshot_date == today)

variable_today_status <- variable_today %>%
  left_join(
    offers_status %>% select(store.id, offer.ean, offer.startTime, snapshot_date, status),
    by = c("store.id", "offer.ean", "offer.startTime", "snapshot_date")
  )

# --------------------------
# SKRIV TIL MYSQL DATABASE
# --------------------------

con <- dbConnect(
  MariaDB(),
  dbname   = "foodwaste",
  host     = "localhost",
  user     = "root",
  password = "Timmtimm2002!"
)

# Stamdata: overskriv hele tabellen
dbExecute(con, "TRUNCATE TABLE store_static;")
dbWriteTable(con, "store_static", stamdata, append = TRUE, row.names = FALSE)

# Variable: append dagens data (historik i SQL)
dbWriteTable(con, "offer_variable", variable_today_status, append = TRUE, row.names = FALSE)

dbDisconnect(con)

cat("Status beregnet og data skrevet til MySQL.\n")
