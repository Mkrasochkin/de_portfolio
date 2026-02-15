/*добавьте сюда запрос для решения задания 4*/

WITH
res AS (
	SELECT 
    	cafe_name, 
    	(jsonb_each((menu->> 'Пицца')::jsonb)).key as pizza
	FROM cafe.restaurants
	WHERE type = 'pizzeria'
),
res_rank as (
	SELECT
		cafe_name,
		count(pizza) as number_pizzas_menu,
		DENSE_RANK() OVER (ORDER BY count(pizza) DESC) as dr
	FROM res
	GROUP BY cafe_name
)
SELECT
	cafe_name,
	number_pizzas_menu
FROM res_rank
WHERE dr in (1);
