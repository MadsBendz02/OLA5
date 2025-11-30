library(dplyr)
library(lubridate)

# antag: variable_all har ALLE kørsler appender efterhånden
# og indeholder mindst:
# store.id, offer.ean, offer.startTime, offer.endTime, timestamp_pipeline

variable <- variable %>%
  mutate(
    snapshot_date = as.Date(timestamp_pipeline),
    expiry_date   = as.Date(offer.endTime)
  )

# find de 2 seneste datoer vi har scraped
dates <- sort(unique(variable$snapshot_date))

if (length(dates) < 2) {
  stop("Har kun én scraping-dag lige nu – kan ikke afgøre solgt/smidt ud endnu.")
}

yesterday <- dates[length(dates) - 1]
today     <- dates[length(dates)]

cat("Bruger disse dage til sammenligning:\n",
    "I går: ", yesterday, "\n",
    "I dag: ", today, "\n")

# nøgle til at identificere et unikt tilbud
key_cols <- c("store.id", "offer.ean", "offer.startTime")

# data for i går og i dag
offers_y <- filter(variable, snapshot_date == yesterday)
offers_t <- filter(variable, snapshot_date == today)

## 1) På lager: fandtes både i går og i dag ------------------------------

on_stock <- inner_join(
  offers_y[, c(key_cols, "expiry_date")],
  offers_t[, key_cols],
  by = key_cols
) %>%
  distinct(store.id, offer.ean, offer.startTime, .keep_all = TRUE) %>%
  mutate(
    snapshot_date = today,
    status        = "På lager"
  )

## 2) Forsvundet siden i går: enten Solgt eller Smidt ud -----------------

disappeared <- anti_join(
  offers_y[, c(key_cols, "expiry_date")],
  offers_t[, key_cols],
  by = key_cols
) %>%
  mutate(
    snapshot_date = today,
    status = if_else(
      expiry_date == today,
      "Smidt ud",   # var i går, væk i dag, udløber i dag
      "Solgt"       # var i går, væk i dag, udløber IKKE i dag
    )
  )

## 3) Samlet status for de varer, der fandtes i går ----------------------

offers_status <- bind_rows(on_stock, disappeared)

## 4) Join status på dagens variable-data -------------------------------

# Alle observationer i dag (inkl. nye varer)
variable_today <- variable %>%
  filter(snapshot_date == today)

# Join status på – nye varer får NA i status
variable_today_status <- variable_today %>%
  left_join(
    offers_status %>%
      select(store.id, offer.ean, offer.startTime, snapshot_date, status),
    by = c("store.id", "offer.ean", "offer.startTime", "snapshot_date")
  )

# nu har du:
# - På lager / Solgt / Smidt ud for varer, der fandtes i går
# - nye varer fra i dag med status = NA (men de kommer stadig i SQL)

stamdata_db <- stamdata %>%
  dplyr::rename(
    store_id             = store.id,
    store_brand          = store.brand,
    store_name           = store.name,
    store_address_street = store.address.street
  )

offer_variable_db <- variable_today_status %>%
  dplyr::rename(
    store_id              = store.id,
    offer_ean             = offer.ean,
    offer_startTime       = offer.startTime,
    offer_endTime         = offer.endTime,
    offer_newPrice        = offer.newPrice,
    offer_originalPrice   = offer.originalPrice,
    offer_percentDiscount = offer.percentDiscount,
    offer_discount        = offer.discount,
    offer_lastUpdate      = offer.lastUpdate,
    product_ean           = product.ean,
    product_description   = product.description,
    product_categories_da = product.categories.da
  )

# --------------------------------------------------
# 6) Skriv til MySQL på Ubuntu
# --------------------------------------------------

con <- dbConnect(
  MariaDB(),
  dbname   = "foodwaste",   # dit DB-navn
  host     = "localhost", # på EC2 typisk localhost
  user     = "root",      # dit DB-brugernavn
  password = "Timmtimm2002!"   # din adgangskode
)

## 6.1) Stamdata – kan bare truncates og indsættes igen
dbExecute(con, "TRUNCATE TABLE store_static;")

dbWriteTable(
  con,
  "store_static",
  stamdata_db,
  append    = TRUE,
  row.names = FALSE
)

## 6.2) Variable – APPEND-ONLY (historisk tidsserie)
dbWriteTable(
  con,
  "offer_variable",
  offer_variable_db,
  append    = TRUE,
  row.names = FALSE
)

dbDisconnect(con)

cat("Status beregnet og data skrevet til MySQL.\n")
