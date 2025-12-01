###############################################
### FOODWASTE PIPELINE – ENDVERSION          ###
### Robust til SQL + API fejlhåndtering      ###
###############################################

library(httr)
library(jsonlite)
library(tidyverse)
library(lubridate)
library(DBI)
library(RMariaDB)

api_key <- Sys.getenv("SALLING_API_KEY")
run_timestamp <- Sys.time()

### 1) POSTNUMRE ###
postnumre <- c("3520", "7080", "1264", "1560")

########################################################
### 2) ROBUST API-FUNKTION (HÅNDTERER 400 / 500)      ###
########################################################

hent_foodwaste <- function(postnummer) {
  url <- "https://api.sallinggroup.com/v1/food-waste"
  
  res <- GET(
    url,
    query = list(zip = postnummer),
    add_headers(Authorization = paste("Bearer", api_key))
  )
  
  sc <- status_code(res)
  
  # 200 OK → Parse
  if (sc == 200) {
    txt <- content(res, as = "text", encoding = "UTF-8")
    
    if (txt == "" || txt == "null") {
      cat("Tom response ved ZIP:", postnummer, "\n")
      return(NULL)
    }
    
    parsed <- fromJSON(txt, flatten = TRUE)
    df <- as.data.frame(parsed)
    df$postnummer <- postnummer
    return(df)
  }
  
  # 4xx fejl → stop (critcal)
  if (sc >= 400 && sc < 500) {
    stop(paste("Fatal API fejl ved ZIP", postnummer, "status", sc,
               "- sandsynligvis ugyldig API-nøgle eller request."))
  }
  
  # 5xx fejl → skip
  if (sc >= 500) {
    cat("Serverfejl 500 ved ZIP", postnummer, "– springer over.\n")
    return(NULL)
  }
}

########################################################
### 3) HENT BUTIKKER                                   ###
########################################################

food_list <- lapply(postnumre, hent_foodwaste)
food_list <- Filter(Negate(is.null), food_list)   # fjern NULL
food_all  <- do.call(rbind, food_list)

########################################################
### 4) UNNEST + FILTER CLEARANCES                     ###
########################################################

valid <- sapply(food_all$clearances, function(x) {
  is.data.frame(x) && nrow(x) > 0
})

food_all_nonempty <- food_all[valid, ]

offers_all <- food_all_nonempty %>%
  tidyr::unnest(clearances) %>%
  mutate(timestamp_pipeline = run_timestamp)

########################################################
### 5) STAMDATA                                        ###
########################################################

stamdata <- food_all %>%
  select(
    store.id,
    store.brand,
    store.name,
    store.address.street,
    postnummer
  ) %>%
  distinct() %>%
  rename(
    store_id              = store.id,
    store_brand           = store.brand,
    store_name            = store.name,
    store_address_street  = store.address.street
  )


########################################################
### 6) VARIABLE (TILBUD) + DATO-KONVERTERING           ###
########################################################

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
  ) %>%
  mutate(
    offer.startTime = ymd_hms(offer.startTime, tz = "UTC"),
    offer.endTime   = ymd_hms(offer.endTime,   tz = "UTC"),
    offer.lastUpdate = ymd_hms(offer.lastUpdate, tz = "UTC"),
    snapshot_date = as.Date(timestamp_pipeline)
  ) %>%
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

########################################################
### 7) SQL FORBINDELSE                                ###
########################################################

con <- dbConnect(
  MariaDB(),
  user = "root",
  password = "Timmtimm2002!",
  dbname = "foodwaste",
  host = "localhost"
)

########################################################
### 8) UPLOAD STAMDATA (KUN FØRSTE GANG)              ###
########################################################

rows <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM store_static")$n

if (rows == 0) {
  cat("Uploader stores første gang...\n")
  dbWriteTable(con, "store_static", stamdata, append = TRUE, row.names = FALSE)
} else {
  cat("Springer upload af stores over – findes allerede.\n")
}

########################################################
### 9) UPLOAD OFFERS (HVER GANG)                      ###
########################################################

dbWriteTable(con, "offer_variable", variable, append = TRUE, row.names = FALSE)

dbDisconnect(con)

cat("\n--- FOODWASTE PIPELINE KØRT FÆRDIGT ---\n")
cat("Timestamp:", run_timestamp, "\n")
