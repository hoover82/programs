
# python C:\dan\alignBI\NASGW\python_scripts\execute_queries.py



import json
import boto3
import pg8000 as pg

#The following two def procedure blocks were lifted from Brian's "redshift.py" file

def _get_connection_info(secret_name, region_name='us-west-2'):
    client = boto3.client('secretsmanager', region_name=region_name)
    raw = client.get_secret_value(SecretId=secret_name)
    return json.loads(raw['SecretString'])

def connect_db(secret_name):
    """Connect to Redshift using the given secret_name in AWS Secrets Manager"""
    info = _get_connection_info(secret_name)
    db = pg.connect(
        host=info['host'],
        port=int(info['port']),
        database=info['dbname'],
        user=info['username'],
        password=info['password'])
    db.autocommit = True

    cursor = db.cursor()
    # Set to Mountain TZ for getdate() calls
    cursor.execute("SET timezone TO 'US/Mountain'")

    # We don't actually use the db object, just the cursor.
    return cursor

#This is the name of the "AWS Secret"
db_name = "redshift/nasgwdw/nasgwadmin"

cursor = connect_db(db_name)

upc_query = \
"""
WITH data_example AS
( SELECT 'CLOVIS, CA 93611' AS ship_location UNION ALL
  SELECT 'SALT LAKE CITY, UT 84118' )
SELECT ship_location, POSITION ( ',' IN ship_location, STRPOS ( ship_position,  ',' )   FROM data_example;
   """ 




qry = """
     SELECT brand, COUNT(*)
     , SUM(COUNT(*)) OVER ( ORDER BY COUNT(*) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) running_sum
     , SUM(COUNT(*)) OVER () s 
     FROM stg.f_transactions GROUP BY 1 ORDER BY 2"""

qry = """SELECT *
          FROM information_schema.columns
         WHERE column_name LIKE '%brand%'
           AND table_name like 'upc%' 
      ORDER BY ordinal_position"""           

#qry = """SELECT brand_id, brand_name, COUNT(*) FROM stg.f_brand GROUP BY 1,2"""



#223  = Sig sauer
qry = """SELECT manufacturer_id
, manufacturer
FROM stg.f_manufacturer
WHERE manufacturer LIKE '%SIG%'
"""


qry = """SELECT manufacturer_id
, first_distributor_description
, CASE WHEN LOWER( first_distributor_description ) LIKE '%bergara%' THEN 'B'
       WHEN LOWER( first_distributor_description ) LIKE '%cva%' THEN 'C'
    ELSE 'A' END AS brand_binary
FROM stg.upc
WHERE manufacturer_id = 42
ORDER BY brand_binary
"""




qry = """SELECT manufacturer_id, brand, brand_id, COUNT(*) FROM stg.upc
WHERE manufacturer_id = 223
GROUP BY 1,2,3"""


qry = """UPDATE stg.f_brand 
 SET manufacturer_id = 223
 WHERE brand_id IN ( 8,9)"""

qry = """SELECT brand_id, brand_name, manufacturer_id FROM stg.f_brand"""

qry = """UPDATE stg.upc
SET brand_id =  9
WHERE upc_id IN ( SELECT upc_id 
                   FROM stg.upc
                   WHERE manufacturer_id = 223
                     AND brand_id IS NULL
                   )
"""

qry = """SELECT manufacturer_id, brand, brand_id, COUNT(*) FROM stg.upc
WHERE brand_id IS NOT NULL
GROUP BY 1,2,3"""

#['vw_nasgw_transactions', 'SELECT t.order_date, t.ship_date, t.distributor_id, d.distributor_name, t.order_number, t.order_line_number, COALESCE(co.override_upc, upc.upc) AS upc, upc.upc_id, upc.official_from_mfr_flag, upc.mfr_internal_identifier, m.mfr_internal_identifier_name, z.state, z.zipcode, initcap(btrim((z.county)::text)) AS county, z."region", z.division, z.incomeperhousehold, initcap(btrim((m.manufacturer)::text)) AS manufacturer, upc.manufacturer_id, t.ship_to_country, initcap(btrim((upc.category)::text)) AS category, initcap(btrim((upc.subcategory)::text)) AS subcategory, initcap((upc.product_tier)::text) AS product_tier, initcap(btrim((upc."type")::text)) AS "type", upc.sizing, (mla.product_tier_access)::character varying(65535) AS manufacturer_product_tier_access, (mla.user_list)::character varying(65535) AS manufacturer_user_list, (mla.distributor_id_list)::character varying(65535) AS manufacturer_distributor_id_list, (mla.plus_access_distributor_id_list)::character varying(65535) AS manufacturer_plus_access_distributor_id_list, (mla.plus_access_1_distr_id_list)::character varying(65535) AS plus_access_1_distr_id_list, (mla.plus_access_2_distr_id_list)::character varying(65535) AS plus_access_2_distr_id_list, (mla.plus_access_3_distr_id_list)::character varying(65535) AS plus_access_3_distr_id_list, (mla.plus_access_4_distr_id_list)::character varying(65535) AS plus_access_4_distr_id_list, (mla.plus_access_5_distr_id_list)::character varying(65535) AS plus_access_5_distr_id_list, mla.distributor_list_bitwise, mla.plus_access_distributor_list_bitwise, mla.plus_access_1_distr_list_bitwise, mla.plus_access_2_distr_list_bitwise, mla.plus_access_3_distr_list_bitwise, mla.plus_access_4_distr_list_bitwise, mla.plus_access_5_distr_list_bitwise, (dla.user_list)::character varying(65535) AS distributor_user_list, df.qty_on_hand, ((CASE WHEN ((length((m.mfr_internal_identifier_name)::text) > 0) AND (length((upc.mfr_internal_identifier)::text) > 0)) THEN (((((m.mfr_internal_identifier_name)::text || (\':\'::character varying)::text) || (upc.mfr_internal_identifier)::text) || (\'; \'::character varying)::text))::character varying ELSE \'\'::character varying END)::text || (COALESCE(co.override_description, upc.first_distributor_description))::text) AS upc_description, 3 AS unique_distributors, CASE WHEN (t.ship_date >= COALESCE(min_snapshot.min_snapshot_date, (\'2050-01-01\'::date)::timestamp without time zone)) THEN \'True\'::character varying ELSE \'False\'::character varying END AS ship_date_after_first_inventory_snapshot_date, (t.shipped_quantity * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) AS shipped_quantity, t.amount_usd, (t.amount_usd / CASE WHEN ((t.shipped_quantity * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) = ((0)::numeric)::numeric(18,0)) THEN (NULL::numeric)::numeric(18,0) ELSE (t.shipped_quantity * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) END) AS unit_price, p.max_date_in_view AS highest_date_in_view, getdate() AS view_load_date FROM ((((((((((stg.f_transactions t JOIN stg.upc ON ((upc.upc_id = t.upc_id))) LEFT JOIN stg.f_distributor d ON ((d.distributor_id = t.distributor_id))) LEFT JOIN stg.f_manufacturer m ON ((m.manufacturer_id = upc.manufacturer_id))) LEFT JOIN stg.f_zipcodes z ON (((z.zipcode = (t.ship_postal_code_clean)::character(5)) AND (z.primaryrecord = \'P\'::bpchar)))) LEFT JOIN stg.vw_manufacturer_listagg mla ON ((m.manufacturer_id = mla.manufacturer_id))) LEFT JOIN stg.vw_distributor_listagg dla ON ((d.distributor_id = dla.distributor_id))) LEFT JOIN prd.t_nasgw_inventory df ON (((df.upc)::text = (upc.upc)::text))) LEFT JOIN (SELECT upc_sub.manufacturer_id, min(fi_sub.snapshot_date) AS min_snapshot_date FROM (stg.f_inventory fi_sub JOIN stg.upc upc_sub ON ((fi_sub.upc_id = upc_sub.upc_id))) GROUP BY upc_sub.manufacturer_id) min_snapshot ON ((upc.manufacturer_id = min_snapshot.manufacturer_id))) CROSS JOIN (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN TRANSACTIONS VIEW\'::character varying)::text)) p) LEFT JOIN stg.upc_combined_override co ON (((co.upc)::text = (upc.upc)::text))) WHERE (((((((((((1 = 1) AND (t.distributor_id <> 0)) AND (t.ship_date >= \'2015-01-01\'::date)) AND (t.ship_date <= getdate())) AND (t.ship_date < (p.max_date_in_view + (1)::bigint))) AND (t.shipped_quantity > (((0)::numeric)::numeric(18,0))::numeric(18,4))) AND (t.amount > (((0)::numeric)::numeric(18,0))::numeric(18,2))) AND (regexp_replace((upc.upc)::text, (\'[^0-9]\'::character varying)::text, (\'\'::character varying)::text) = (upc.upc)::text)) AND (upc.upc_id <> 22331)) AND ((CASE WHEN (((initcap((upc.product_tier)::text) = (\'Ammunition\'::character varying)::text) OR (initcap((upc.product_tier)::text) = (\'Optics\'::character varying)::text)) AND (t.distributor_id = 12)) THEN \'N\'::character varying ELSE \'Y\'::character varying END)::text = (\'Y\'::character varying)::text)) AND (1 = 1));']
#['vw_nasgw_inventory', 'SELECT i.snapshot_date, initcap(btrim((m.manufacturer)::text)) AS manufacturer, upc.manufacturer_id, COALESCE(co.override_upc, upc.upc) AS upc, upc.upc_id, upc.official_from_mfr_flag, upc.mfr_internal_identifier, m.mfr_internal_identifier_name, ((CASE WHEN ((length((m.mfr_internal_identifier_name)::text) > 0) AND (length((upc.mfr_internal_identifier)::text) > 0)) THEN (((((m.mfr_internal_identifier_name)::text || (\':\'::character varying)::text) || (upc.mfr_internal_identifier)::text) || (\'; \'::character varying)::text))::character varying ELSE \'\'::character varying END)::text || (COALESCE(co.override_description, upc.first_distributor_description))::text) AS description, (i.qty_on_hand * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) AS qty_on_hand, i.distributor_id, d.distributor_name, (mla.product_tier_access)::character varying(65535) AS manufacturer_product_tier_access, (mla.user_list)::character varying(65535) AS manufacturer_user_list, (mla.distributor_id_list)::character varying(65535) AS manufacturer_distributor_id_list, (mla.plus_access_distributor_id_list)::character varying(65535) AS manufacturer_plus_access_distributor_id_list, (mla.plus_access_1_distr_id_list)::character varying(65535) AS plus_access_1_distr_id_list, (mla.plus_access_2_distr_id_list)::character varying(65535) AS plus_access_2_distr_id_list, (mla.plus_access_3_distr_id_list)::character varying(65535) AS plus_access_3_distr_id_list, (mla.plus_access_4_distr_id_list)::character varying(65535) AS plus_access_4_distr_id_list, (mla.plus_access_5_distr_id_list)::character varying(65535) AS plus_access_5_distr_id_list, mla.distributor_list_bitwise, mla.plus_access_distributor_list_bitwise, mla.plus_access_1_distr_list_bitwise, mla.plus_access_2_distr_list_bitwise, mla.plus_access_3_distr_list_bitwise, mla.plus_access_4_distr_list_bitwise, mla.plus_access_5_distr_list_bitwise, (dla.user_list)::character varying(65535) AS distributor_user_list, initcap(btrim((upc."type")::text)) AS "type", initcap(btrim((upc.category)::text)) AS category, initcap(btrim((upc.subcategory)::text)) AS subcategory, upc.product_tier, upc.sizing, df.distributors_shipping, ((CASE WHEN ((length((m.mfr_internal_identifier_name)::text) > 0) AND (length((upc.mfr_internal_identifier)::text) > 0)) THEN (((((m.mfr_internal_identifier_name)::text || (\':\'::character varying)::text) || (upc.mfr_internal_identifier)::text) || (\'; \'::character varying)::text))::character varying ELSE \'\'::character varying END)::text || (COALESCE(co.override_description, upc.first_distributor_description))::text) AS upc_description, getdate() AS view_load_date FROM (((((((((stg.f_inventory i LEFT JOIN stg.vw_distributor_listagg dla ON ((i.distributor_id = dla.distributor_id))) LEFT JOIN stg.f_distributor d ON ((i.distributor_id = d.distributor_id))) JOIN (SELECT imax.distributor_id, "max"(imax.snapshot_date) AS maxsnap FROM stg.f_inventory imax WHERE (imax.snapshot_date <= (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text))) GROUP BY imax.distributor_id) msnap ON (((msnap.distributor_id = i.distributor_id) AND (msnap.maxsnap = i.snapshot_date)))) LEFT JOIN stg.upc ON ((i.upc_id = upc.upc_id))) LEFT JOIN stg.f_manufacturer m ON ((m.manufacturer_id = upc.manufacturer_id))) LEFT JOIN stg.vw_manufacturer_listagg mla ON ((m.manufacturer_id = mla.manufacturer_id))) LEFT JOIN (SELECT icnt.upc_id, count(DISTINCT icnt.distributor_id) AS distributors_shipping FROM (stg.f_inventory icnt JOIN (SELECT fi.distributor_id, "max"(fi.snapshot_date) AS maxsnap FROM stg.f_inventory fi WHERE (fi.snapshot_date <= (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text))) GROUP BY fi.distributor_id) "max" ON ((("max".distributor_id = icnt.distributor_id) AND ("max".maxsnap = icnt.snapshot_date)))) WHERE (icnt.qty_on_hand > (((0)::numeric)::numeric(18,0))::numeric(18,2)) GROUP BY icnt.upc_id) df ON ((df.upc_id = i.upc_id))) CROSS JOIN (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text)) p) LEFT JOIN stg.upc_combined_override co ON (((co.upc)::text = (upc.upc)::text))) WHERE (((((((1 = 1) AND (i.qty_on_hand > (((0)::numeric)::numeric(18,0))::numeric(18,2))) AND (upc.upc_id <> 22331)) AND (i.snapshot_date <= p.max_date_in_view)) AND (((i.distributor_id <> 0) AND (i.distributor_id <> 11)) AND (i.distributor_id <> 15))) AND ((CASE WHEN (((initcap((upc.product_tier)::text) = (\'Ammunition\'::character varying)::text) OR (initcap((upc.product_tier)::text) = (\'Optics\'::character varying)::text)) AND (i.distributor_id = 12)) THEN \'N\'::character varying ELSE \'Y\'::character varying END)::text = (\'Y\'::character varying)::text)) AND (1 = 1));']
#['vw_nasgw_inventory_all_snapshot_dates', 'SELECT i.snapshot_date, initcap(btrim((m.manufacturer)::text)) AS manufacturer, upc.manufacturer_id, i.description, (i.qty_on_hand * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) AS qty_on_hand, i.distributor_id, d.distributor_name, mla.product_tier_access AS manufacturer_product_tier_access, mla.user_list AS manufacturer_user_list, mla.distributor_id_list AS manufacturer_distributor_id_list, mla.plus_access_1_distr_id_list, mla.plus_access_2_distr_id_list, mla.plus_access_3_distr_id_list, mla.plus_access_4_distr_id_list, mla.plus_access_5_distr_id_list, mla.distributor_list_bitwise, mla.plus_access_distributor_list_bitwise, mla.plus_access_1_distr_list_bitwise, mla.plus_access_2_distr_list_bitwise, mla.plus_access_3_distr_list_bitwise, mla.plus_access_4_distr_list_bitwise, mla.plus_access_5_distr_list_bitwise, dla.user_list AS distributor_user_list, i.upc, i.upc_id, initcap(btrim((upc."type")::text)) AS "type", initcap(btrim((upc.category)::text)) AS category, initcap(btrim((upc.subcategory)::text)) AS subcategory, initcap(btrim((upc.product_tier)::text)) AS product_tier, upc.sizing, df.distributors_shipping, upc.first_distributor_description AS upc_description, getdate() AS view_load_date, imax.most_recent_inventory_flag FROM ((((((((stg.f_inventory i LEFT JOIN stg.vw_distributor_listagg dla ON ((i.distributor_id = dla.distributor_id))) LEFT JOIN stg.f_distributor d ON ((i.distributor_id = d.distributor_id))) LEFT JOIN stg.upc upc ON ((i.upc_id = upc.upc_id))) LEFT JOIN stg.f_manufacturer m ON ((upc.manufacturer_id = m.manufacturer_id))) LEFT JOIN stg.vw_manufacturer_listagg mla ON ((m.manufacturer_id = mla.manufacturer_id))) LEFT JOIN (SELECT i.upc_id, count(DISTINCT i.distributor_id) AS distributors_shipping FROM (stg.f_inventory i JOIN (SELECT i.distributor_id, "max"(i.snapshot_date) AS maxsnap FROM stg.f_inventory i GROUP BY i.distributor_id) "max" ON ((("max".distributor_id = i.distributor_id) AND ("max".maxsnap = i.snapshot_date)))) WHERE (i.qty_on_hand > (((0)::numeric)::numeric(18,0))::numeric(18,2)) GROUP BY i.upc_id) df ON ((df.upc_id = i.upc_id))) LEFT JOIN (SELECT f_inventory.distributor_id, "max"(f_inventory.snapshot_date) AS most_recent_snapshot_date, (\'Y\'::character varying)::character varying(1) AS most_recent_inventory_flag FROM stg.f_inventory WHERE ((((f_inventory.distributor_id <> 0) AND (f_inventory.distributor_id <> 11)) AND (f_inventory.distributor_id <> 15)) AND (f_inventory.snapshot_date <= (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text)))) GROUP BY f_inventory.distributor_id) imax ON (((imax.distributor_id = i.distributor_id) AND (imax.most_recent_snapshot_date = i.snapshot_date)))) CROSS JOIN (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text)) p) WHERE (((((1 = 1) AND (i.qty_on_hand > (((0)::numeric)::numeric(18,0))::numeric(18,2))) AND (i.snapshot_date <= p.max_date_in_view)) AND ((CASE WHEN (((initcap((upc.product_tier)::text) = (\'Ammunition\'::character varying)::text) OR (initcap((upc.product_tier)::text) = (\'Optics\'::character varying)::text)) AND (i.distributor_id = 12)) THEN \'N\'::character varying ELSE \'Y\'::character varying END)::text = (\'Y\'::character varying)::text)) AND (1 = 1));']

qry = """SELECT manufacturer_id, brand, brand_id, COUNT(*) FROM stg.upc"""

qry = """SELECT TABLE_NAME 
, VIEW_DEFINITION
FROM information_schema.views
WHERE table_schema = 'prd'
--  AND table_name LIKE 'vw_nasgw_inventory'
--  AND table_name LIKE 'vw_nasgw_transactions'
  AND table_name LIKE 'vw_nasgw_inventory_all_snapshot_dates'
"""  


qry_trans = """
--CREATE OR REPLACE VIEW dan
CREATE OR REPLACE VIEW PRD.vw_nasgw_transactions
AS 
SELECT t.order_date, t.ship_date, t.distributor_id, d.distributor_name, t.order_number, t.order_line_number
, COALESCE(co.override_upc, upc.upc) AS upc
, upc.upc_id
, upc.official_from_mfr_flag
, upc.mfr_internal_identifier, m.mfr_internal_identifier_name
, z.state, z.zipcode, initcap(btrim((z.county)::text)) AS county, z."region", z.division, z.incomeperhousehold
, initcap(btrim((m.manufacturer)::text)) AS manufacturer
, upc.manufacturer_id
--------------------------------
, b.brand_id, b.brand_name
--------------------------------
, t.ship_to_country
, initcap(btrim((upc.category)::text)) AS category, initcap(btrim((upc.subcategory)::text)) AS subcategory
, initcap((upc.product_tier)::text) AS product_tier, initcap(btrim((upc."type")::text)) AS "type", upc.sizing
, (mla.product_tier_access)::character varying(65535) AS manufacturer_product_tier_access, (mla.user_list)::character varying(65535) AS manufacturer_user_list, (mla.distributor_id_list)::character varying(65535) AS manufacturer_distributor_id_list
, (mla.plus_access_distributor_id_list)::character varying(65535) AS manufacturer_plus_access_distributor_id_list, (mla.plus_access_1_distr_id_list)::character varying(65535) AS plus_access_1_distr_id_list, (mla.plus_access_2_distr_id_list)::character varying(65535) AS plus_access_2_distr_id_list, (mla.plus_access_3_distr_id_list)::character varying(65535) AS plus_access_3_distr_id_list, (mla.plus_access_4_distr_id_list)::character varying(65535) AS plus_access_4_distr_id_list, (mla.plus_access_5_distr_id_list)::character varying(65535) AS plus_access_5_distr_id_list, mla.distributor_list_bitwise, mla.plus_access_distributor_list_bitwise, mla.plus_access_1_distr_list_bitwise, mla.plus_access_2_distr_list_bitwise, mla.plus_access_3_distr_list_bitwise, mla.plus_access_4_distr_list_bitwise, mla.plus_access_5_distr_list_bitwise, (dla.user_list)::character varying(65535) AS distributor_user_list
, df.qty_on_hand, ((CASE WHEN ((length((m.mfr_internal_identifier_name)::text) > 0) 
AND (length((upc.mfr_internal_identifier)::text) > 0)) THEN (((((m.mfr_internal_identifier_name)::text || (\':\'::character varying)::text) || (upc.mfr_internal_identifier)::text) || (\'; \'::character varying)::text))::character varying ELSE \'\'::character varying END)::text || (COALESCE(co.override_description
, upc.first_distributor_description))::text) AS upc_description, 3 AS unique_distributors
, CASE WHEN (t.ship_date >= COALESCE(min_snapshot.min_snapshot_date, (\'2050-01-01\'::date)::timestamp without time zone)) THEN \'True\'::character varying ELSE \'False\'::character varying END AS ship_date_after_first_inventory_snapshot_date
, (t.shipped_quantity * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) AS shipped_quantity, t.amount_usd, (t.amount_usd / CASE WHEN ((t.shipped_quantity * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) = ((0)::numeric)::numeric(18,0)) THEN (NULL::numeric)::numeric(18,0) ELSE (t.shipped_quantity * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) END) AS unit_price
, p.max_date_in_view AS highest_date_in_view, getdate() AS view_load_date 
FROM ((((((((((stg.f_transactions t JOIN stg.upc ON ((upc.upc_id = t.upc_id))) 
LEFT JOIN stg.f_distributor d ON ((d.distributor_id = t.distributor_id))) 
LEFT JOIN stg.f_manufacturer m ON ((m.manufacturer_id = upc.manufacturer_id))) 
LEFT JOIN stg.f_brand b ON b.brand_id  = upc.brand_id
LEFT JOIN stg.f_zipcodes z ON (((z.zipcode = (t.ship_postal_code_clean)::character(5)) AND (z.primaryrecord = \'P\'::bpchar)))) 
LEFT JOIN stg.vw_manufacturer_listagg mla ON ((m.manufacturer_id = mla.manufacturer_id))) 
LEFT JOIN stg.vw_distributor_listagg dla ON ((d.distributor_id = dla.distributor_id))) 
LEFT JOIN prd.t_nasgw_inventory df ON (((df.upc)::text = (upc.upc)::text))) 
LEFT JOIN (SELECT upc_sub.manufacturer_id, min(fi_sub.snapshot_date) AS min_snapshot_date FROM (stg.f_inventory fi_sub JOIN stg.upc upc_sub ON ((fi_sub.upc_id = upc_sub.upc_id))) GROUP BY upc_sub.manufacturer_id) min_snapshot ON ((upc.manufacturer_id = min_snapshot.manufacturer_id))) 
CROSS JOIN (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view 
FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN TRANSACTIONS VIEW\'::character varying)::text)) p) 
LEFT JOIN stg.upc_combined_override co ON (((co.upc)::text = (upc.upc)::text))) 
WHERE (((((((((((1 = 1) 
AND (t.distributor_id <> 0)) 
AND (t.ship_date >= \'2015-01-01\'::date)) 

--AND t.ship_date >= \'2019-07-01\'::date
--AND upc.manufacturer_id IN ( 32 , 119, 287, 280, 23 , 284, 42 , 232, 162, 243, 278, 223, 143, 188)

AND (t.ship_date <= getdate())) 
AND (t.ship_date < (p.max_date_in_view + (1)::bigint))) AND (t.shipped_quantity > (((0)::numeric)::numeric(18,0))::numeric(18,4))) AND (t.amount > (((0)::numeric)::numeric(18,0))::numeric(18,2))) AND (regexp_replace((upc.upc)::text, (\'[^0-9]\'::character varying)::text, (\'\'::character varying)::text) = (upc.upc)::text)) AND (upc.upc_id <> 22331)) AND ((CASE WHEN (((initcap((upc.product_tier)::text) = (\'Ammunition\'::character varying)::text) OR (initcap((upc.product_tier)::text) = (\'Optics\'::character varying)::text)) AND (t.distributor_id = 12)) THEN \'N\'::character varying ELSE \'Y\'::character varying END)::text = (\'Y\'::character varying)::text)) 
AND (1 = 1))
"""

#qry_trans = """DROP VIEW PRD.vw_nasgw_transactions"""


qry_inv = """
--CREATE OR REPLACE VIEW dan
CREATE OR REPLACE VIEW prd.vw_nasgw_inventory
AS 
SELECT i.snapshot_date, initcap(btrim((m.manufacturer)::text)) AS manufacturer
, upc.manufacturer_id
--------------------------------
, b.brand_id, b.brand_name
--------------------------------
, COALESCE(co.override_upc, upc.upc) AS upc
, upc.upc_id, upc.official_from_mfr_flag
, upc.mfr_internal_identifier
, m.mfr_internal_identifier_name
, ((CASE WHEN ((length((m.mfr_internal_identifier_name)::text) > 0) AND (length((upc.mfr_internal_identifier)::text) > 0)) THEN (((((m.mfr_internal_identifier_name)::text || (\':\'::character varying)::text) || (upc.mfr_internal_identifier)::text) || (\'; \'::character varying)::text))::character varying ELSE \'\'::character varying END)::text || (COALESCE(co.override_description, upc.first_distributor_description))::text) AS description, (i.qty_on_hand * ((CASE WHEN (((upc."type")::text = (\'AMMUNITION\'::character varying)::text) AND (upc.ammo_round_count IS NOT NULL)) THEN upc.ammo_round_count ELSE 1 END)::numeric)::numeric(18,0)) AS qty_on_hand, i.distributor_id, d.distributor_name, (mla.product_tier_access)::character varying(65535) AS manufacturer_product_tier_access, (mla.user_list)::character varying(65535) AS manufacturer_user_list, (mla.distributor_id_list)::character varying(65535) AS manufacturer_distributor_id_list, (mla.plus_access_distributor_id_list)::character varying(65535) AS manufacturer_plus_access_distributor_id_list, (mla.plus_access_1_distr_id_list)::character varying(65535) AS plus_access_1_distr_id_list, (mla.plus_access_2_distr_id_list)::character varying(65535) AS plus_access_2_distr_id_list, (mla.plus_access_3_distr_id_list)::character varying(65535) AS plus_access_3_distr_id_list, (mla.plus_access_4_distr_id_list)::character varying(65535) AS plus_access_4_distr_id_list, (mla.plus_access_5_distr_id_list)::character varying(65535) AS plus_access_5_distr_id_list, mla.distributor_list_bitwise, mla.plus_access_distributor_list_bitwise, mla.plus_access_1_distr_list_bitwise, mla.plus_access_2_distr_list_bitwise, mla.plus_access_3_distr_list_bitwise, mla.plus_access_4_distr_list_bitwise, mla.plus_access_5_distr_list_bitwise, (dla.user_list)::character varying(65535) AS distributor_user_list, initcap(btrim((upc."type")::text)) AS "type", initcap(btrim((upc.category)::text)) AS category, initcap(btrim((upc.subcategory)::text)) AS subcategory, upc.product_tier, upc.sizing, df.distributors_shipping, ((CASE WHEN ((length((m.mfr_internal_identifier_name)::text) > 0) AND (length((upc.mfr_internal_identifier)::text) > 0)) THEN (((((m.mfr_internal_identifier_name)::text || (\':\'::character varying)::text) || (upc.mfr_internal_identifier)::text) || (\'; \'::character varying)::text))::character varying ELSE \'\'::character varying END)::text || (COALESCE(co.override_description, upc.first_distributor_description))::text) AS upc_description, getdate() AS view_load_date 
FROM (((((((((stg.f_inventory i 
LEFT JOIN stg.vw_distributor_listagg dla ON ((i.distributor_id = dla.distributor_id))) 
LEFT JOIN stg.f_distributor d ON ((i.distributor_id = d.distributor_id))) 
JOIN (SELECT imax.distributor_id, "max"(imax.snapshot_date) AS maxsnap 
          FROM stg.f_inventory imax 
           WHERE (imax.snapshot_date <= (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view 
                                          FROM stg.f_transactions_parameters 
                                              WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text))) 
                                              GROUP BY imax.distributor_id) msnap 
                                              ON (((msnap.distributor_id = i.distributor_id) 
                                              AND (msnap.maxsnap = i.snapshot_date)))) LEFT JOIN stg.upc ON ((i.upc_id = upc.upc_id))) 
LEFT JOIN stg.f_manufacturer m ON ((m.manufacturer_id = upc.manufacturer_id))) 

LEFT JOIN stg.f_brand b ON b.brand_id  = upc.brand_id

LEFT JOIN stg.vw_manufacturer_listagg mla ON ((m.manufacturer_id = mla.manufacturer_id))) 
LEFT JOIN (SELECT icnt.upc_id, count(DISTINCT icnt.distributor_id) AS distributors_shipping 
           FROM (stg.f_inventory icnt JOIN (SELECT fi.distributor_id, "max"(fi.snapshot_date) AS maxsnap 
                                             FROM stg.f_inventory fi 
                                               WHERE (fi.snapshot_date <= (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view 
                                                                          FROM stg.f_transactions_parameters 
                                                                          WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text))) GROUP BY fi.distributor_id) "max" ON ((("max".distributor_id = icnt.distributor_id) AND ("max".maxsnap = icnt.snapshot_date)))) WHERE (icnt.qty_on_hand > (((0)::numeric)::numeric(18,0))::numeric(18,2)) GROUP BY icnt.upc_id) df ON ((df.upc_id = i.upc_id))) CROSS JOIN (SELECT f_transactions_parameters.parameter_date_value AS max_date_in_view 
FROM stg.f_transactions_parameters WHERE ((f_transactions_parameters.parameter_name)::text = (\'MAX DATE IN INVENTORY VIEW\'::character varying)::text)) p) LEFT JOIN stg.upc_combined_override co ON (((co.upc)::text = (upc.upc)::text))) WHERE (((((((1 = 1) AND (i.qty_on_hand > (((0)::numeric)::numeric(18,0))::numeric(18,2))) AND (upc.upc_id <> 22331)) AND (i.snapshot_date <= p.max_date_in_view)) AND (((i.distributor_id <> 0) AND (i.distributor_id <> 11)) AND (i.distributor_id <> 15))) AND ((CASE WHEN (((initcap((upc.product_tier)::text) = (\'Ammunition\'::character varying)::text) OR (initcap((upc.product_tier)::text) = (\'Optics\'::character varying)::text)) AND (i.distributor_id = 12)) THEN \'N\'::character varying ELSE \'Y\'::character varying END)::text = (\'Y\'::character varying)::text)) 
AND (1 = 1))
"""

#[76160887]
#[76160887]
qry = """SELECT COUNT(*) FROM prd.vw_nasgw_transactions"""
# [362815]
# [362815]
#qry = """SELECT COUNT(*) FROM prd.vw_nasgw_inventory"""

qry = """SELECT TRUNC( order_date ) - EXTRACT ( DOW FROM order_date )  ship_week
  , COUNT(*)
--FROM stg.f_transactions
FROM prd.vw_nasgw_transactions
WHERE order_date > DATE '2019-10-01'
  AND distributor_id = 17
GROUP BY 1
ORDER BY 1"""

#[datetime.date(2019, 9, 29), 1168]
#[datetime.date(2019, 10, 6), 2081]
#[datetime.date(2019, 10, 13), 2105]
#[datetime.date(2019, 10, 20), 2264]
#[datetime.date(2019, 10, 27), 1891]
#[datetime.date(2019, 11, 3), 2041]
#[datetime.date(2019, 11, 10), 2049]
#[datetime.date(2019, 11, 17), 1914]
#[datetime.date(2019, 11, 24), 1802]
#[datetime.date(2019, 12, 1), 2112]
#[datetime.date(2019, 12, 8), 1961]
#[datetime.date(2019, 12, 15), 1749]
#[datetime.date(2019, 12, 22), 971]
#[datetime.date(2019, 12, 29), 414]
#[datetime.date(2020, 1, 5), 2]
#[datetime.date(2020, 1, 12), 13]
#[datetime.date(2020, 1, 19), 1744]
#[datetime.date(2020, 1, 26), 1915]

try:
   cursor.execute (  qry )
except Exception as error:
    print ("\nQuery error!")
    print ("\nError message: {}".format(error))
    
else:
    print ("\n* * * The query was successful! * * *")


    colnames = [desc[0] for desc in cursor.description]

    print ( colnames ) 
    print ( '-----------------------------------')

    rslt = cursor.fetchall()

    for i, rec in enumerate (rslt):
        print ( rec ) 

    print ( '{} rows selected'.format(i+1))    
    











