CREATE TABLE `store` (
  `Make` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL,
  `Cartype` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
  `KmPL` float DEFAULT NULL,
  `Caryear` int NOT NULL,
  `Color` varchar(45) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL,
  `Cardescription` mediumtext CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,
  `Image` varchar(45) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL,
  `Store_id` int NOT NULL,
  `Price_id` int DEFAULT NULL,
  `Region` varchar(45) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL,
  `Seller_id` int DEFAULT NULL,
  `kmstand` int DEFAULT NULL,
  PRIMARY KEY (`Store_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin