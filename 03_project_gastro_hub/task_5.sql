/*добавьте сюда запрос для решения задания 5*/

WITH
menu_cte AS (
	SELECT
		cafe_name,
		'Пицца' as dish_type, 
		(jsonb_each((menu->> 'Пицца')::jsonb)).key as pizza,
		(jsonb_each((menu->> 'Пицца')::jsonb)).value::int as price
	FROM cafe.restaurants
	WHERE type = 'pizzeria'
),
menu_with_rank AS (
	SELECT
		cafe_name,
		dish_type, 
		pizza,
		price,
		ROW_NUMBER() OVER (PARTITION BY cafe_name ORDER BY price DESC) as rn
	FROM menu_cte
)
SELECT
	cafe_name,
	dish_type, 
	pizza,
	price
FROM menu_with_rank
WHERE rn = 1
ORDER BY cafe_name;