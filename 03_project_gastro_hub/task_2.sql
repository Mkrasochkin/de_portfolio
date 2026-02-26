/*добавьте сюда запрос для решения задания 2*/

CREATE MATERIALIZED VIEW v_percentage_change_sales AS
	SELECT
		extract(year from date) as year,
		cafe_name, 
		type,
		ROUND(AVG(avg_check), 2) as average_check_current_year,
		ROUND(LAG(AVG(avg_check)) over (PARTITION BY cafe_name order by extract(year from date)), 2) as average_check_previous_year,
		ROUND(((AVG(avg_check) - LAG(AVG(avg_check)) over (PARTITION by cafe_name order by extract(year from date))) / LAG(AVG(avg_check)) over (PARTITION by cafe_name order by extract(year from date))) * 100, 2) as percentage_change
	FROM cafe.sales s
	LEFT JOIN cafe.restaurants r
	ON s.restaurant_uuid = r.restaurant_uuid
	WHERE extract(year from date) < 2023
	GROUP BY cafe_name, type, year
	ORDER BY cafe_name, year;