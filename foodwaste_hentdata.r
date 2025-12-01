library(httr)
library(jsonlite)
library(tidyverse)
library(dplyr)
library(lubridate)

api_key <- Sys.getenv("SALLING_API_KEY")
run_timestamp <- Sys.time()
logfile       <- "foodwaste_log.txt"

### 1) POSTNUMRE ###
postnumre <- c("3520", "7080", "1264", "1560")

### 2) HENT-FUNKTION ###
hent_foodwaste <- function(postnummer) {
  foodurl <- "https://api.sallinggroup.com/v1/food-waste"
  
  foodres <- GET(
    url   = foodurl,
    query = list(zip = postnummer),
    add_headers(Authorization = paste("Bearer", api_key))
  )
  
  stop_for_status(foodres)
  
  food_json   <- content(foodres, as = "text", encoding = "UTF-8")
  foodparsed  <- fromJSON(food_json, flatten = TRUE)
  fooddf      <- as.data.frame(foodparsed)
  fooddf$postnummer <- postnummer
  
  fooddf
}

### 3) HENT ALLE BUTIKKER (food_all) ###
food_list <- lapply(postnumre, hent_foodwaste)
food_all  <- do.call(rbind, food_list)

# TJEK:
# View(food_all)
# str(food_all$clearances)

### 4) FILTER BUTIKKER MED RIGTIGE CLEARANCES + UNNEST ###

# vælg kun rækker hvor clearances er et data.frame med mindst 1 række
valid_rows <- sapply(
  food_all$clearances,
  function(x) is.data.frame(x) && nrow(x) > 0
)

food_all_nonempty <- food_all[valid_rows, ]

offers_all <- food_all_nonempty %>%
  tidyr::unnest(clearances) %>%   # folder alle ikke-tomme clearances ud
  mutate(
    timestamp_pipeline = run_timestamp
  )

# TJEK:
# View(offers_all)
# nrow(offers_all)

### 5) STAMDATA FRA food_all (ALLE BUTIKKER) ###

stamdata <- food_all %>%
  select(
    store.id,
    store.brand,
    store.name,
    store.address.street,
    postnummer
  ) %>%
  distinct()

stamdata <- stamdata %>%
  rename(
    store_id             = store.id,
    store_brand          = store.brand,
    store_name           = store.name,
    store_address_street = store.address.street
    # postnummer er OK
  )

# View(stamdata)

### 6) VARIABLE FRA offers_all (ALLE TILBUD) ###

variable <- offers_all %>%
  select(
    store.id,
    offer.ean,
    offer.startTime,
    offer.endTime,
    offer.newPrice,
    offer.originalPrice,
    offer.percentDiscount,
    offer.discount,
    offer.lastUpdate,
    offer.stock,
    product.ean,
    product.description,
    product.categories.da,
    timestamp_pipeline
  )

variable <- variable %>%
  mutate(snapshot_date = as.Date(timestamp_pipeline)) %>%
  rename(
    store_id              = store.id,
    offer_ean             = offer.ean,
    offer_startTime       = offer.startTime,
    offer_endTime         = offer.endTime,
    offer_newPrice        = offer.newPrice,
    offer_originalPrice   = offer.originalPrice,
    offer_percentDiscount = offer.percentDiscount,
    offer_discount        = offer.discount,
    offer_lastUpdate      = offer.lastUpdate,
    offer_stock           = offer.stock,
    product_ean           = product.ean,
    product_description   = product.description,
    product_categories_da = product.categories.da
  )



library(DBI)
library(RMariaDB)

con <- dbConnect(
  MariaDB(),
  user = "root",          # eller den bruger du lavede
  password = "Timmtimm2002!",
  dbname = "foodwaste",
  host = "localhost"
)

rows_in_store <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM store_static")$n

if (rows_in_store == 0) {
  cat("Uploader stores første gang...\n")
  
  dbWriteTable(
    con,
    name = "store_static",
    value = stamdata,
    append = TRUE,
    row.names = FALSE
  )
  
} else {
  cat("Springer upload af stores over – de findes allerede.\n")
}

dbWriteTable(
  con,
  name = "offer_variable",
  value = variable,
  append = TRUE,
  row.names = FALSE
)

dbDisconnect(con)


