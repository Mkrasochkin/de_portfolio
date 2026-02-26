/*добавьте сюда запросы для решения задания 6*/

BEGIN;

	SELECT
		cafe_name,
		(menu->> 'Кофе')::jsonb
	FROM cafe.restaurants
	WHERE type = 'coffee_shop' and (menu #>> '{Кофе, Капучино}')::int > 0
	FOR NO KEY UPDATE;

	WITH
	np_capuchino AS (
		SELECT
			cafe_name,
			(menu #>> '{Кофе, Капучино}')::int as price,
			((menu #>> '{Кофе, Капучино}')::int) * 1.2 as new_price
		FROM cafe.restaurants
		WHERE type = 'coffee_shop' and (menu #>> '{Кофе, Капучино}')::int > 0
		)
	UPDATE cafe.restaurants as r
	SET menu = JSONB_SET(menu, '{Кофе, Капучино}', to_jsonb(new_price))	
	FROM np_capuchino
	WHERE r.cafe_name =  np_capuchino.cafe_name;

COMMIT;