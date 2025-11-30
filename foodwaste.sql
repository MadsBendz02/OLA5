CREATE TABLE store_static (
  store_id             INT PRIMARY KEY,
  store_brand          VARCHAR(50),
  store_name           VARCHAR(255),
  store_address_street VARCHAR(255),
  postnummer           VARCHAR(10)
);

CREATE TABLE offer_variable (
  store_id              INT,
  offer_ean             VARCHAR(50),
  offer_startTime       DATETIME,
  offer_endTime         DATETIME,
  offer_newPrice        DECIMAL(10,2),
  offer_originalPrice   DECIMAL(10,2),
  offer_percentDiscount DECIMAL(5,2),
  offer_discount        DECIMAL(10,2),
  offer_lastUpdate      DATETIME,
  offer_stock           INT,
  product_ean           VARCHAR(50),
  product_description   TEXT,
  product_categories_da TEXT,
  timestamp_pipeline    DATETIME,
  snapshot_date         DATE,
  status                VARCHAR(20),
  PRIMARY KEY (store_id, offer_ean, offer_startTime, snapshot_date)
);
