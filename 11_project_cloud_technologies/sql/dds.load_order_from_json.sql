DROP PROCEDURE IF EXISTS dds.load_order_from_json;
CREATE OR REPLACE PROCEDURE dds.load_order_from_json(p_order JSONB)
/*
Загрузка заказа из JSON
*/
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id        INT;
    v_order_dt        TIMESTAMP;
    v_cost            NUMERIC(19,5);
    v_payment         NUMERIC(19,5);
    v_status          TEXT;
    v_restaurant_id   TEXT;
    v_restaurant_name TEXT;
    v_user_id         TEXT;
    v_user_name       TEXT;
	v_hk_order_product_pk UUID;

    v_h_order_pk      UUID;
    v_h_user_pk       UUID;
    v_h_restaurant_pk UUID;
    v_h_product_pk    UUID;
    v_h_category_pk   UUID;

    v_hashdiff        UUID;
    v_product         JSONB;
	v_payload         JSONB;
BEGIN
	v_payload         := p_order#>>'{payload}';
    v_order_id        := (v_payload->>'id')::INT;
    v_order_dt        := (v_payload->>'date')::TIMESTAMP;
    v_cost            := (v_payload->>'cost')::NUMERIC(19,5);
    v_payment         := (v_payload->>'payment')::NUMERIC(19,5);
    v_status          := v_payload->>'status';
    v_restaurant_id   := v_payload#>>'{restaurant,id}';
    v_restaurant_name := v_payload#>>'{restaurant,name}';
    v_user_id         := v_payload#>>'{user,id}';
    v_user_name       := v_payload#>>'{user,name}';

    -- h_order
    INSERT INTO dds.h_order (order_id, order_dt)
    VALUES (v_order_id, v_order_dt)
    ON CONFLICT (order_id) DO NOTHING
    RETURNING h_order_pk INTO v_h_order_pk;

    IF v_h_order_pk IS NULL THEN
        SELECT h_order_pk INTO v_h_order_pk
        FROM dds.h_order
        WHERE order_id = v_order_id;
    END IF;

    -- h_user
    INSERT INTO dds.h_user (user_id)
    VALUES (v_user_id)
    ON CONFLICT (user_id) DO NOTHING
    RETURNING h_user_pk INTO v_h_user_pk;

    IF v_h_user_pk IS NULL THEN
        SELECT h_user_pk INTO v_h_user_pk
        FROM dds.h_user
        WHERE user_id = v_user_id;
    END IF;

    -- h_restaurant
    INSERT INTO dds.h_restaurant (restaurant_id)
    VALUES (v_restaurant_id)
    ON CONFLICT(restaurant_id) DO NOTHING
    RETURNING h_restaurant_pk INTO v_h_restaurant_pk;

    IF v_h_restaurant_pk IS NULL THEN
        SELECT h_restaurant_pk INTO v_h_restaurant_pk
        FROM dds.h_restaurant
        WHERE restaurant_id = v_restaurant_id;
    END IF;

    -- l_order_user
    INSERT INTO dds.l_order_user (h_order_pk, h_user_pk)
    VALUES (v_h_order_pk, v_h_user_pk)
    ON CONFLICT (h_order_pk, h_user_pk) DO NOTHING;

    -- продукты и категории
    FOR v_product IN SELECT * FROM jsonb_array_elements(v_payload->'products')
    LOOP
        DECLARE
            v_product_id      TEXT := v_product->>'id';
            v_product_name    TEXT := v_product->>'name';
            v_category_name   TEXT := v_product->>'category';
        BEGIN
            -- h_product
            INSERT INTO dds.h_product (product_id)
            VALUES (v_product_id)
            ON CONFLICT (product_id) DO NOTHING
            RETURNING h_product_pk INTO v_h_product_pk;

            IF v_h_product_pk IS NULL THEN
                SELECT h_product_pk INTO v_h_product_pk
                FROM dds.h_product
                WHERE product_id = v_product_id;
            END IF;

            -- h_category
            INSERT INTO dds.h_category (category_name)
            VALUES (v_category_name)
            ON CONFLICT (category_name) DO NOTHING
            RETURNING h_category_pk INTO v_h_category_pk;

            IF v_h_category_pk IS NULL THEN
                SELECT h_category_pk INTO v_h_category_pk
                FROM dds.h_category
                WHERE category_name = v_category_name;
            END IF;

            -- l_order_product
            INSERT INTO dds.l_order_product (h_order_pk, h_product_pk)
            VALUES (v_h_order_pk, v_h_product_pk)
            ON CONFLICT (h_order_pk, h_product_pk) DO NOTHING;

            -- l_product_restaurant
            INSERT INTO dds.l_product_restaurant (h_product_pk, h_restaurant_pk)
            VALUES (v_h_product_pk, v_h_restaurant_pk)
            ON CONFLICT (h_product_pk, h_restaurant_pk) DO NOTHING;

            -- l_product_category
            INSERT INTO dds.l_product_category (h_product_pk, h_category_pk)
            VALUES (v_h_product_pk, v_h_category_pk)
            ON CONFLICT (h_product_pk, h_category_pk) DO NOTHING;

            -- Сателлиты
			-- s_product_names
            v_hashdiff := md5(v_h_product_pk::TEXT || v_product_name) :: UUID;
            DELETE FROM dds.s_product_names WHERE h_product_pk = v_h_product_pk;
            INSERT INTO dds.s_product_names (h_product_pk, name, hk_product_names_hashdiff)
            VALUES (v_h_product_pk, v_product_name, v_hashdiff);

            -- s_restaurant_names
            v_hashdiff := md5(v_h_restaurant_pk::TEXT || v_restaurant_name) :: UUID;
            DELETE FROM dds.s_restaurant_names WHERE h_restaurant_pk = v_h_restaurant_pk;
            INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, hk_restaurant_names_hashdiff)
            VALUES (v_h_restaurant_pk, v_restaurant_name, v_hashdiff);

			-- l_order_product
	        SELECT hk_order_product_pk
	        INTO v_hk_order_product_pk
	        FROM dds.l_order_product
	        WHERE h_order_pk = v_h_order_pk AND h_product_pk = v_h_product_pk;
	
	        IF v_hk_order_product_pk IS NOT NULL THEN
	            v_hashdiff := md5(
	                v_hk_order_product_pk::TEXT ||
	                (v_product->>'price') ||
	                (v_product->>'quantity')
	            )::UUID;
	
	            DELETE FROM dds.s_order_product_details 
	            WHERE hk_order_product_pk = v_hk_order_product_pk;
	
	            INSERT INTO dds.s_order_product_details (hk_order_product_pk,price,quantity,hk_order_product_details_hashdiff) 
				VALUES (v_hk_order_product_pk,(v_product->>'price')::NUMERIC(19,5),(v_product->>'quantity')::INT,v_hashdiff);
	        END IF;
        END;
    END LOOP;

    -- Сателлиты для заказа
    -- s_order_cost
    v_hashdiff := md5(v_h_order_pk::TEXT || v_cost::TEXT || v_payment::TEXT) :: UUID;
    DELETE FROM dds.s_order_cost WHERE h_order_pk = v_h_order_pk;
    INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, hk_order_cost_hashdiff)
    VALUES (v_h_order_pk, v_cost, v_payment, v_hashdiff);

    -- s_order_status
    v_hashdiff := md5(v_h_order_pk::TEXT || v_status) :: UUID;
    DELETE FROM dds.s_order_status WHERE h_order_pk = v_h_order_pk;
    INSERT INTO dds.s_order_status (h_order_pk, status, hk_order_status_hashdiff)
    VALUES (v_h_order_pk, v_status, v_hashdiff);

    -- s_user_names
    v_hashdiff := md5(v_h_user_pk::TEXT || v_user_name || v_user_id) :: UUID;
    DELETE FROM dds.s_user_names WHERE h_user_pk = v_h_user_pk;
    INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, hk_user_names_hashdiff)
    VALUES (v_h_user_pk, v_user_name, v_user_id, v_hashdiff);
END;
$$;