DROP TABLE IF EXISTS cdm.user_product_counters CASCADE;
DROP TABLE IF EXISTS cdm.user_category_counters CASCADE;
DROP TABLE IF EXISTS stg.order_events CASCADE;
DROP TABLE IF EXISTS dds.h_user CASCADE;
DROP TABLE IF EXISTS dds.h_product CASCADE;
DROP TABLE IF EXISTS dds.h_order CASCADE;
DROP TABLE IF EXISTS dds.h_category CASCADE;
DROP TABLE IF EXISTS dds.h_restaurant CASCADE;
DROP TABLE IF EXISTS dds.l_order_product CASCADE;
DROP TABLE IF EXISTS dds.l_order_product CASCADE;
DROP TABLE IF EXISTS dds.l_order_user CASCADE;
DROP TABLE IF EXISTS dds.l_product_category CASCADE;
DROP TABLE IF EXISTS dds.l_product_restaurant CASCADE;
DROP TABLE IF EXISTS dds.s_order_cost CASCADE;
DROP TABLE IF EXISTS dds.s_order_product_details CASCADE;
DROP TABLE IF EXISTS dds.s_order_status CASCADE;
DROP TABLE IF EXISTS dds.s_order_cost CASCADE;
DROP TABLE IF EXISTS dds.s_restaurant_names CASCADE;
DROP TABLE IF EXISTS dds.s_user_names CASCADE;
DROP TABLE IF EXISTS dds.s_product_names CASCADE;

-- Витрины
CREATE TABLE cdm.user_product_counters
(
	  id            serial4 NOT NULL PRIMARY KEY
	, user_id       uuid NOT NULL
	, product_id    uuid NOT NULL
	, product_name  varchar NOT NULL
	, order_cnt     int NOT NULL CHECK (order_cnt>=0)
);
CREATE UNIQUE INDEX ix_cdm_user_product_counters ON cdm.user_product_counters(user_id, product_id);

CREATE TABLE cdm.user_category_counters
(
	  id            serial4 NOT NULL PRIMARY key
	, user_id       uuid NOT NULL
	, category_id   uuid NOT NULL
	, category_name varchar NOT NULL
	, order_cnt     int NOT NULL CHECK (order_cnt>=0)
);
CREATE UNIQUE INDEX ix_cdm_user_category_counters ON cdm.user_category_counters(user_id, category_id);

-- STG
CREATE TABLE stg.order_events
(
      id            serial4 NOT NULL PRIMARY KEY
	, object_id     int NOT NULL unique
	, payload       json  NOT null
	, object_type   varchar NOT null
	, sent_dttm     timestamp NOT null
);
-- DDS
-- хабы
CREATE TABLE dds.h_user
(
	  h_user_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, user_id VARCHAR NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (user_id)
);

CREATE TABLE dds.h_product
(
	  h_product_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, product_id VARCHAR NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (product_id)
);

CREATE TABLE dds.h_category
(
	  h_category_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, category_name VARCHAR NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (category_name)
);

CREATE TABLE dds.h_restaurant
(
	  h_restaurant_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, restaurant_id VARCHAR NOT NULL 
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (restaurant_id)
);

CREATE TABLE dds.h_order
(
	  h_order_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, order_id int NOT NULL
	, order_dt timestamp NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (order_id)
);
-- линки
CREATE TABLE dds.l_order_product
(
  	  hk_order_product_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, h_order_pk uuid NOT NULL REFERENCES dds.h_order(h_order_pk)
	, h_product_pk uuid NOT NULL REFERENCES dds.h_product(h_product_pk)
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE(h_order_pk, h_product_pk)
);
CREATE TABLE dds.l_product_restaurant
(
  	  hk_product_restaurant_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, h_product_pk uuid NOT NULL REFERENCES dds.h_product(h_product_pk)
	, h_restaurant_pk uuid NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk)	
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (h_product_pk, h_restaurant_pk)
);
CREATE TABLE dds.l_product_category
(
  	  hk_product_category_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, h_product_pk uuid NOT NULL REFERENCES dds.h_product(h_product_pk)
	, h_category_pk uuid NOT NULL REFERENCES dds.h_category(h_category_pk)	
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (h_product_pk, h_category_pk)
);
CREATE TABLE dds.l_order_user
(
  	  hk_order_user_pk uuid NOT NULL PRIMARY KEY DEFAULT (gen_random_uuid())
	, h_order_pk uuid NOT NULL REFERENCES dds.h_order(h_order_pk)
	, h_user_pk uuid NOT NULL REFERENCES dds.h_user(h_user_pk)	
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, UNIQUE (h_order_pk, h_user_pk)
);
-- сателлиты
CREATE TABLE dds.s_user_names
(
	  h_user_pk uuid NOT NULL REFERENCES dds.h_user(h_user_pk)
	, username varchar NOT NULL
	, userlogin varchar NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, hk_user_names_hashdiff uuid NOT NULL 
);
CREATE TABLE dds.s_product_names
(
	  h_product_pk uuid NOT NULL REFERENCES dds.h_product(h_product_pk)
	, name varchar NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, hk_product_names_hashdiff uuid NOT NULL 
);
CREATE TABLE dds.s_restaurant_names
(
	  h_restaurant_pk uuid NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk)
	, name varchar NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, hk_restaurant_names_hashdiff uuid NOT NULL 
);
CREATE TABLE dds.s_order_cost
(
	  h_order_pk uuid NOT NULL REFERENCES dds.h_order(h_order_pk)
	, cost decimal(19, 5) NOT NULL
	, payment decimal(19, 5) NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, hk_order_cost_hashdiff uuid NOT NULL 
);
CREATE TABLE dds.s_order_status
(
	  h_order_pk uuid NOT NULL REFERENCES dds.h_order(h_order_pk)
	, status varchar NOT NULL
	, load_dt timestamp NOT NULL DEFAULT (current_timestamp)
	, load_src varchar NOT NULL DEFAULT('orders-system-kafka')
	, hk_order_status_hashdiff uuid NOT NULL 
);

