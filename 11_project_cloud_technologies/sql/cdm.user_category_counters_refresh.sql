DROP PROCEDURE IF EXISTS cdm.user_product_counters_refresh;
CREATE OR REPLACE PROCEDURE cdm.user_product_counters_refresh()
/*
Обновление витрины по категориям
CALL cdm.user_product_counters_refresh();
*/
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM cdm.user_product_counters;
	
	INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
	SELECT lou.h_user_pk, hp.h_product_pk, spn.name, count(*)
	FROM       dds.l_order_product lop
	INNER JOIN dds.l_order_user lou ON lou.h_order_pk = lop.h_order_pk 
	INNER JOIN dds.l_product_category lpc ON lpc.h_product_pk = lop.h_product_pk
	INNER JOIN dds.h_product hp ON hp.h_product_pk = lpc.h_product_pk
	INNER JOIN dds.s_product_names spn on spn.h_product_pk = hp.h_product_pk
	GROUP BY lou.h_user_pk, hp.h_product_pk, spn.name;
END;
$$;
