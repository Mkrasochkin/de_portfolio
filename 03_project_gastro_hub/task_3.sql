/*добавьте сюда запрос для решения задания 3*/

SELECT 
	cafe_name,
	ROW_NUMBER() OVER (PARTITION BY cafe_name) as times_manager_changed
FROM cafe.restaurant_manager_work_dates rm
LEFT JOIN cafe.restaurants r on rm.restaurant_uuid = r.restaurant_uuid 
LEFT JOIN cafe.managers m on rm.manager_uuid = m.manager_uuid 
GROUP BY rm.manager_uuid, rm.restaurant_uuid, m.manager_uuid, r.cafe_name
ORDER BY times_manager_changed desc
LIMIT 3;
