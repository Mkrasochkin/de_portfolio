/*Добавьте в этот файл запросы, которые наполняют данными таблицы в схеме cafe данными*/

INSERT INTO cafe.restaurants (cafe_name, type, menu)
SELECT 
	distinct s.cafe_name,
	type::cafe.restaurant_type,
	menu
FROM raw_data.sales  s
LEFT JOIN raw_data.menu m
ON s.cafe_name = m.cafe_name;


INSERT INTO cafe.managers (manager, manager_phone)
SELECT 
	distinct manager,
	manager_phone
FROM raw_data.sales;


INSERT INTO cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, start_date, end_date)
SELECT 
	restaurant_uuid,
	manager_uuid,
	min(s.report_date),
	max(s.report_date)
FROM cafe.managers m
LEFT JOIN raw_data.sales s
ON m.manager = s.manager
LEFT JOIN cafe.restaurants r
ON s.cafe_name = r.cafe_name
GROUP BY restaurant_uuid, manager_uuid;


INSERT INTO cafe.sales (date, restaurant_uuid, avg_check)
SELECT
	report_date,
	restaurant_uuid,
	avg_check
FROM raw_data.sales s
LEFT JOIN cafe.restaurants r
ON s.cafe_name = r.cafe_name;