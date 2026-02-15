/*добавьте сюда запросы для решения задания 7*/

BEGIN;
	LOCK TABLE cafe.managers in exclusive mode;	
	ALTER TABLE cafe.managers  ADD COLUMN add_phones text[];

	WITH 
	rn_manager AS (
		SELECT *,
		ROW_NUMBER() OVER (ORDER BY manager) + 100 as add
		FROM cafe.managers
	),
	nn_manager AS (
	SELECT *,
		CONCAT('8-800-2500-', add) as new_number
	FROM rn_manager
	)
	UPDATE cafe.managers m
	SET add_phones = ARRAY[nn.new_number, m.manager_phone]
	FROM nn_manager nn
	WHERE m.manager_uuid = nn.manager_uuid;

	ALTER TABLE cafe.managers DROP COLUMN manager_phone;

COMMIT;