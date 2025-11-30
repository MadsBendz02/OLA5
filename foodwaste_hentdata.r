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

food_list <- lapply(postnumre, hent_foodwaste)
food_all  <- do.call(rbind, food_list)


#fold clearences ud

akaciatorvet <- (food_all[[1]][[1]])
akaciatorvet <- akaciatorvet[, -c(1, 11, 14, 16)]

farum_bytorv <- (food_all[[1]][[2]])
farum_bytorv <- farum_bytorv[, -c(1, 11, 14, 16)]

ny_boder <- (food_all[[1]][[3]])
ny_boder <- ny_boder[, -c(1, 11, 14, 16)]

store_kongensgade <- (food_all[[1]][[4]])
store_kongensgade <- store_kongensgade[, -c(1, 11, 14, 16)]

st_kongensgade <- (food_all[[1]][[5]])
st_kongensgade <- st_kongensgade[, -c(1, 11, 14, 16)]

kalvebod <- (food_all[[1]][[6]])
kalvebod <- kalvebod[, -c(1, 11, 14, 16)]



# butiksinfo ligger i food_all på samme rækkenummer
akaciatorvet$store.id           <- food_all$store.id[1]
akaciatorvet$store.brand        <- food_all$store.brand[1]
akaciatorvet$store.name         <- food_all$store.name[1]
akaciatorvet$postnummer         <- food_all$postnummer[1]
akaciatorvet$timestamp_pipeline <- run_timestamp
akaciatorvet$store.address.street <- food_all$store.address.street[1]


farum_bytorv$store.id           <- food_all$store.id[2]
farum_bytorv$store.brand        <- food_all$store.brand[2]
farum_bytorv$store.name         <- food_all$store.name[2]
farum_bytorv$postnummer         <- food_all$postnummer[2]
farum_bytorv$timestamp_pipeline <- run_timestamp
farum_bytorv$store.address.street <- food_all$store.address.street[2]


ny_boder$store.id               <- food_all$store.id[3]
ny_boder$store.brand            <- food_all$store.brand[3]
ny_boder$store.name             <- food_all$store.name[3]
ny_boder$postnummer             <- food_all$postnummer[3]
ny_boder$timestamp_pipeline     <- run_timestamp
ny_boder$store.address.street <- food_all$store.address.street[3]


store_kongensgade$store.id           <- food_all$store.id[4]
store_kongensgade$store.brand        <- food_all$store.brand[4]
store_kongensgade$store.name         <- food_all$store.name[4]
store_kongensgade$postnummer         <- food_all$postnummer[4]
store_kongensgade$timestamp_pipeline <- run_timestamp
store_kongensgade$store.address.street <- food_all$store.address.street[4]


st_kongensgade$store.id           <- food_all$store.id[5]
st_kongensgade$store.brand        <- food_all$store.brand[5]
st_kongensgade$store.name         <- food_all$store.name[5]
st_kongensgade$postnummer         <- food_all$postnummer[5]
st_kongensgade$timestamp_pipeline <- run_timestamp
st_kongensgade$store.address.street <- food_all$store.address.street[5]


kalvebod$store.id           <- food_all$store.id[6]
kalvebod$store.brand        <- food_all$store.brand[6]
kalvebod$store.name         <- food_all$store.name[6]
kalvebod$postnummer         <- food_all$postnummer[6]
kalvebod$timestamp_pipeline <- run_timestamp
kalvebod$store.address.street <- food_all$store.address.street[6]


allebutikker <- rbind(
  akaciatorvet,
  farum_bytorv,
  ny_boder,
  store_kongensgade,
  st_kongensgade,
  kalvebod
)

stamdata <- unique(
  allebutikker[, c(
    "store.id",
    "store.brand",
    "store.name",
    "store.address.street",
    "postnummer"
  )]
)

variable <- allebutikker[, c(
  "store.id",
  "offer.ean",
  "offer.startTime",
  "offer.endTime",
  "offer.newPrice",
  "offer.originalPrice",
  "offer.percentDiscount",
  "offer.discount",
  "offer.lastUpdate",
  "offer.stock",
  "product.ean",
  "product.description",
  "product.categories.da",
  "timestamp_pipeline"
)]

