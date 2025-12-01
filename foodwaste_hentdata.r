library(httr)
library(jsonlite)
library(tidyverse)
library(dplyr)
library(lubridate)
library(DBI)
library(RMariaDB)  # først når du er klar til at skrive til MySQL

api_key <- Sys.getenv("SALLING_API_KEY")
run_timestamp <- Sys.time()
logfile       <- "foodwaste_log.txt"

### FOOD ###
postnumre <- c("3520", "7080", "1264", "1560")

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


# 1) vælg kun rækker hvor clearances er et data.frame med mindst 1 række
valid_rows <- sapply(food_all$clearances, function(x) is.data.frame(x) && nrow(x) > 0)

food_all_nonempty <- food_all[valid_rows, ]

# 2) unnest kun på de rækker
offers_all <- food_all_nonempty %>%
  unnest(clearances) %>%
  mutate(timestamp_pipeline = run_timestamp)


stamdata <- food_all %>%
  distinct(
    store.id,
    store.brand,
    store.name,
    store.address.street,
    postnummer
  )

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

con <- dbConnect(
  MariaDB(),
  dbname   = "foodwaste",    # din DB
  host     = "localhost",  # på EC2
  user     = "root",       # dit brugernavn
  password = "Timmtimm2002!"    # skift til din rigtige kode
)

# Stamdata: overskriv hele tabellen hver gang
dbExecute(con, "TRUNCATE TABLE store_static;")
dbWriteTable(con, "store_static", stamdata, append = TRUE, row.names = FALSE)

# Variable: APPEND – så hver scrape giver nye rækker med nyt timestamp_pipeline
dbWriteTable(con, "offer_variable", variable, append = TRUE, row.names = FALSE)

dbDisconnect(con)

cat("Scrape gemt i MySQL.\n")

