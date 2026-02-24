DROP PROCEDURE IF EXISTS cdm.user_category_counters_refresh;
CREATE OR REPLACE PROCEDURE cdm.user_category_counters_refresh()
/*
Обновление витрины по категориям
CALL cdm.user_category_counters_refresh();
*/
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM cdm.user_category_counters;
	
	INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
	SELECT lou.h_user_pk, hc.h_category_pk, hc.category_name, count(*)
	FROM       dds.l_order_product lop
	INNER JOIN dds.l_order_user lou ON lou.h_order_pk = lop.h_order_pk 
	INNER JOIN dds.l_product_category lpc ON lpc.h_product_pk = lop.h_product_pk
	INNER JOIN dds.h_category hc ON hc.h_category_pk  = lpc.h_category_pk
	GROUP BY lou.h_user_pk, hc.h_category_pk, hc.category_name;
END;
$$;
