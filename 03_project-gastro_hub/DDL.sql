/*Добавьте в этот файл все запросы, для создания схемы сafe и
 таблиц в ней в нужном порядке*/

CREATE SCHEMA IF NOT EXISTS cafe;


CREATE TYPE cafe.restaurant_type AS ENUM
	('coffee_shop', 'restaurant', 'bar', 'pizzeria');


CREATE TABLE cafe.restaurants (
	restaurant_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	cafe_name character varying,
	type cafe.restaurant_type,
	menu jsonb
);



CREATE TABLE cafe.managers (
	manager_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	manager CHARACTER VARYING,
	manager_phone CHARACTER VARYING
);



CREATE TABLE cafe.restaurant_manager_work_dates (
	restaurant_uuid uuid,
	manager_uuid uuid,
	start_date DATE,
	end_date DATE,
	PRIMARY KEY (restaurant_uuid, manager_uuid)	
);



CREATE TABLE cafe.sales (
	date date,
	restaurant_uuid uuid,
	avg_check numeric(6, 2),
	PRIMARY KEY (date, restaurant_uuid)	
);