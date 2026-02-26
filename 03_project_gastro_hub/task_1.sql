/*добавьте сюда запрос для решения задания 1*/

CREATE VIEW cafe.v_sales_top_3 AS
WITH
s AS (
	SELECT
		AVG(avg_check) as average_check,
		r.restaurant_uuid,
		cafe_name, 
		type,
		ROW_NUMBER() OVER (PARTITION BY type order by AVG(avg_check) DESC)
	FROM cafe.sales s
	LEFT JOIN cafe.restaurants r
	ON s.restaurant_uuid = r.restaurant_uuid
	GROUP BY r.restaurant_uuid 
)
SELECT
	cafe_name,
	type,
	round(average_check, 2)
FROM s
WHERE row_number IN (1, 2, 3);
