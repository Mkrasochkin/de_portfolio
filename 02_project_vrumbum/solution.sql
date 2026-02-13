-- Этап 1. Создание и заполнение БД

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE raw_data.sales (
	id int4 NOT NULL,
	auto varchar,
	gasoline_consumption varchar(50) NULL,
	price numeric,
	date date,
	person_name varchar(50),
	phone varchar,
	discount int4,
	brand_origin varchar NULL		
);

/*Заполнил таблицу raw_data.sales данными через импорт DBeaver*/

CREATE SCHEMA IF NOT EXISTS car_shop;



CREATE TABLE car_shop.brand_origin (
	id SERIAL PRIMARY KEY,
	country TEXT,	
);


CREATE TABLE car_shop.cars (
	id SERIAL PRIMARY KEY,
	car_name VARCHAR, /*Строки переменной длины с максимальной длиной n*/
	brand_origin_id INTEGER REFERENCES car_shop.brand_origin
);


CREATE TABLE car_shop.models (
	id SERIAL PRIMARY KEY,
	model_name VARCHAR, /*Строки переменной длины с максимальной длиной n*/
	car_id INTEGER REFERENCES car_shop.cars,
	gasoline_consumption VARCHAR(50)
);


CREATE TABLE car_shop.colors (
	id SERIAL PRIMARY KEY,
	color_name VARCHAR /*Строки переменной длины с максимальной длиной n*/
);


CREATE TABLE car_shop.clients (
	id SERIAL PRIMARY KEY,
	person_name VARCHAR NOT NULL,
	phone VARCHAR
);


CREATE TABLE car_shop.sales_cs (
	id SERIAL PRIMARY KEY,
	model_id INTEGER REFERENCES car_shop.models,
	color_id INTEGER REFERENCES car_shop.colors,
	price numeric(7, 2) NOT NULL, /*цена может содержать только сотые и не может быть больше семизначной суммы*/
	date date NOT NULL,
	client_id INTEGER REFERENCES car_shop.clients,
	discount INTEGER
);



INSERT INTO car_shop.brand_origin (country)
SELECT 
	distinct brand_origin from raw_data.sales;


INSERT INTO car_shop.cars (car_name, brand_origin_id)
SELECT 
	distinct split_part(auto, ' ', 1), 
	bo.id
FROM raw_data.sales as s
LEFT JOIN car_shop.brand_origin as bo 
ON s.brand_origin = bo.country;


INSERT INTO car_shop.models (model_name, car_id, gasoline_consumption)
SELECT 
	distinct substr(auto, strpos(auto, split_part(auto, ' ', 2)), strpos(auto, ',') - strpos(auto, split_part(auto, ' ', 2))), 
	c.id,
	gasoline_consumption
FROM raw_data.sales as s
LEFT JOIN car_shop.cars as c 
ON SPLIT_PART(auto, ' ', 1) = c.car_name;


INSERT INTO into car_shop.colors (color_name)
SELECT 
	distinct SPLIT_PART(auto, ',', -1) from raw_data.sales;


INSERT INTO car_shop.clients (person_name, phone)
SELECT 
	distinct person_name, phone from raw_data.sales;


INSERT INTO car_shop.sales_cs (model_id, color_id, price, date, client_id, discount)
SELECT 
	m.id,
	col.id,
	price,
	date,
	cl.id,
	discount	
FROM raw_data.sales s
LEFT JOIN car_shop.models m
ON substr(auto, strpos(auto, split_part(auto, ' ', 2)), strpos(auto, ',') - strpos(auto, split_part(auto, ' ', 2))) = m.model_name
LEFT JOIN car_shop.colors col
ON SPLIT_PART(auto, ',', -1) = col.color_name
LEFT JOIN car_shop.gasoline_consumption gc
ON s.gasoline_consumption = gc.consumption
LEFT JOIN car_shop.clients cl
ON s.person_name = cl.person_name;


-- Этап 2. Создание выборок

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.

SELECT
	count(gasoline_consumption)*100 /max(cs.id) ::real as nulls_percentage_gasoline_consumption
FROM car_shop.sales_cs cs
LEFT JOIN car_shop.models m
ON cs.model_id = m.id
WHERE gasoline_consumption = 'null';


---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки. 


SELECT  
	car_name as brand_name, 
	EXTRACT(YEAR FROM date) as year,  
	ROUND(AVG(price), 2) as price_avg 
FROM car_shop.sales_cs scs 
LEFT JOIN car_shop.models m ON scs.model_id = m.id
LEFT JOIN car_shop.cars c ON m.car_id = c.id
GROUP BY car_name, EXTRACT(YEAR FROM date) 
ORDER BY car_name;



---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки. 

 
SELECT  
	EXTRACT(MONTH FROM date) as month, 
	EXTRACT(YEAR FROM date) as year, 
	ROUND(SUM(price) / COUNT(*), 2)  as price_avg 
FROM car_shop.sales_cs scs 
LEFT JOIN car_shop.models m ON scs.model_id = m.id
LEFT JOIN car_shop.cars c ON m.car_id = c.id 
WHERE EXTRACT(YEAR FROM date) = '2022' 
GROUP BY EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date) 
ORDER BY EXTRACT(MONTH FROM date);



---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя. 


SELECT  
	person_name as person, 
	STRING_AGG(CONCAT_WS (' ', car_name, model_name), ', ') as cars 
FROM car_shop.sales_cs scs
LEFT JOIN car_shop.models m ON scs.model_id = m.id
LEFT JOIN car_shop.cars c ON m.car_id = c.id  
LEFT JOIN car_shop.clients cl ON scs.client_id = cl.id 
GROUP BY person_name 
ORDER BY person_name;



---- Задание 5. Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля с разбивкой по стране без учёта скидки.

SELECT 
	country as brand_origin, 
	MAX(price / (1 - (discount/100))) as price_max, 
	MIN(price / (1 - (discount/100))) as price_min 
FROM car_shop.sales_cs scs
LEFT JOIN car_shop.models m ON scs.model_id = m.id
LEFT JOIN car_shop.cars c ON m.car_id = c.id
LEFT JOIN car_shop.brand_origin b ON c.brand_origin_id = b.id
GROUP by country;




---- Задание 6. Напишите запрос, который покажет количество всех пользователей из США. 
 

SELECT  
	COUNT (*) as persons_from_usa_count 
FROM car_shop.sales_cs scs 
LEFT JOIN car_shop.clients cl ON scs.client_id = cl.id 
WHERE phone LIKE '%+1%'; 




