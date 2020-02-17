
#### ###########################################################################
##
## Jobs to main UPC_ID ETL
## I am using this for all updates for data maintenance
##
#### ###########################################################################

#### local
#$### pytin 

import sys

running_in_aws_glue_flag = False

if (running_in_aws_glue_flag):
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
 
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
 

def print_separator():

    print ( )
    print ( "--------------------------------------------------")
    print ( )

def execute_this_query ( queryID, active ):

    #If not specified or overridden, do not execute query
    retValue = False
	
    if active:
        retValue = True
		
    if override_true.count(queryID) > 0:
        retValue = True

    if override_false.count(queryID) > 0:
        retValue = False
		
    return retValue


###+++++++++++++++++++++++++++++++++++++++++++++++

import json
import boto3
import pg8000 as pg

def _get_connection_info(secret_name, region_name='us-west-2'):
    client = boto3.client('secretsmanager', region_name=region_name)
    raw = client.get_secret_value(SecretId=secret_name)
    return json.loads(raw['SecretString'])

#AWS Secret connects to 'nasgwdw as nasgwadmin'
secret_name = 'redshift/nasgwdw/nasgwadmin'

#info = _get_connection_info(secret_name)

#db = pg.connect(
#        host=info['host'],
#        port=int(info['port']),
#        database=info['dbname'],
#        user=info['username'],
#        password=info['password'])
#db.autocommit = True

cursor = db.cursor()

##++++++++++++++++++++++++++++++++++++++++++++

queries = []

override_true = []
override_false = []  # To skip any query which is True to run,place id here [60,70, 10, 20, 90]

##++++++++++++++++++++++++++++++++++++++++++++

##### BUILD QUERY STRINGS #####

#### Queries will be executed in the order they appear here; It does NOT order by "id" value
#### Queries will not be executed unless "active" is True or if "id" appears in override_true
 
 
query = { "id":60
  , "active":True
  , "meta":"FIX Null UPC_ID in F_TRANSACTIONS" 
  , "sql" : """
UPDATE stg.f_transactions 
SET upc_id = -1
WHERE upc_id IS NULL
"""}

queries.append (query)

#################################################################################################

query = { "id":70
  , "active":True
  , "meta":"Fix Null UPC_ID in F_INVENTORY" 
  , "sql" : """
UPDATE stg.f_inventory
SET upc_id = -1
WHERE upc_id IS NULL
"""}

queries.append (query)

#################################################################################################

 
query = { "id":601
  , "active":True
  , "meta":"FIX ORPHAN UPC_ID in F_TRANSACTIONS" 
  , "sql" : """
UPDATE stg.f_transactions 
SET upc_id = -1
WHERE upc_id NOT IN ( SELECT upc_id FROM stg.upc )
"""}

queries.append (query)

#################################################################################################

query = { "id":701
  , "active":True
  , "meta":"Fix ORPHAN UPC_ID in F_INVENTORY" 
  , "sql" : """
UPDATE stg.f_inventory
SET upc_id = -1
WHERE upc_id NOT IN ( SELECT upc_id FROM stg.upc )
"""}

queries.append (query)

#################################################################################################
#################################################################################################


query = { "id":342
  , "active":True
  , "meta":"TRIM user-names in MANUFACTURERS_SECURITY" 
  , "sql" : """
UPDATE stg.f_manufacturers_security 
SET user_name = TRIM ( user_name ) 
WHERE LENGTH ( user_name ) <> LENGTH ( TRIM(user_name) ) 
"""}

queries.append (query)


#################################################################################################


#################################################################################################

 
query = { "id":10
, "active":True
, "meta":"Insert new UPCs from F_TRANSACTIONS" 
, "sql" : """
 INSERT INTO stg.upc (upc,
 first_distributor_description,
 first_distributor_description_id
--,type, type_id
 , category, category_id, subcategory, subcategory_id, first_distributor_id)
SELECT
  REGEXP_REPLACE(si.upc, '[^0-9]+', '') AS upc
  ,max(si.description)
  ,min(ud.description_id)
--  ,null as type
--  ,null as type_id
  ,max(CASE td.category WHEN '' THEN NULL ELSE td.category END)  AS category
  ,max(c.category_id)   AS category_id
  ,max(CASE td.subcategory WHEN '' THEN NULL ELSE td.subcategory END)  AS subcategory
  ,max(sc.subcategory_id)  AS subcategory_id
  ,-1 AS first_distributor_id
FROM stg.f_transactions si
LEFT JOIN stg.s_unique_descriptions ud
  ON si.description = ud.description
LEFT JOIN tmp.tagged_descriptions td
  ON REGEXP_REPLACE(td.upc, '[^0-9]+', '') = REGEXP_REPLACE(si.upc, '[^0-9]+', '')
LEFT JOIN stg.f_category c
  ON UPPER(td.category) = UPPER(c.category)
LEFT JOIN stg.f_subcategory sc
  ON UPPER(td.subcategory) = UPPER(sc.subcategory)
--- 2018-09-27
---  Use LTRIM to prevent duplicate UPCs -- UPCs which are otherwise the same except for leading zeroes
LEFT JOIN stg.upc u
  ON LTRIM(u.upc,'0') = LTRIM(REGEXP_REPLACE(si.upc, '[^0-9]+', ''),'0')
WHERE LEN(REGEXP_REPLACE(si.upc, '[^0-9]+', '')) in (12, 14, 8)
  AND u.upc is null
GROUP BY   REGEXP_REPLACE(si.upc, '[^0-9]+', '')
"""}
queries.append (query)

#################################################################################################

query = { "id":20
  ,"active":True
  , "meta":"Insert new UPCs from F_INVENTORY" 
  , "sql" : """
 INSERT INTO stg.upc (upc,
 first_distributor_description,
 first_distributor_description_id
-- ,type, type_id
, category, category_id, subcategory, subcategory_id, first_distributor_id)
SELECT
  REGEXP_REPLACE(si.upc, '[^0-9]+', '')
  ,max(si.description)
  ,min(ud.description_id)
--  ,null as type
--  ,null as type_id
  ,max(CASE td.category WHEN '' THEN NULL ELSE td.category END)  AS category
  ,max(c.category_id)   AS category_id
  ,max(CASE td.subcategory WHEN '' THEN NULL ELSE td.subcategory END)  AS subcategory
  ,max(sc.subcategory_id)  AS subcategory_id
  ,-1 AS first_distributor_id
FROM stg.f_inventory si
LEFT JOIN stg.s_unique_descriptions ud
  ON si.description = ud.description
LEFT JOIN tmp.tagged_descriptions td
  ON REGEXP_REPLACE(td.upc, '[^0-9]+', '') = REGEXP_REPLACE(si.upc, '[^0-9]+', '')
LEFT JOIN stg.f_category c
  ON UPPER(td.category) = UPPER(c.category)
LEFT JOIN stg.f_subcategory sc
  ON UPPER(td.subcategory) = UPPER(sc.subcategory)
--- 2018-09-27
---  Use LTRIM to prevent duplicate UPCs -- UPCs which are otherwise the same exceot for leading zeroes
LEFT JOIN stg.upc u
  ON LTRIM(u.upc,'0') = LTRIM(REGEXP_REPLACE(si.upc, '[^0-9]+', ''),'0')
WHERE LEN(REGEXP_REPLACE(si.upc, '[^0-9]+', '')) in (12, 14, 8)
  AND u.upc is null
GROUP BY   REGEXP_REPLACE(si.upc, '[^0-9]+', '')
"""}

queries.append (query)


#################################################################################################

query = { "id":52
  ,"active":True
  , "meta":"Pass UPC_ID back to F_TRANSACTIONS" 
  , "sql" : """
UPDATE stg.f_transactions
SET upc_id = upc.upc_id 
  , updated_at = getdate()
FROM stg.upc 
WHERE LTRIM(REGEXP_REPLACE ( upc.upc, '[^0-9]', ''),'0') = LTRIM( REGEXP_REPLACE ( f_transactions.upc, '[^0-9]', ''), '0')
  AND f_transactions.upc_id = -1
"""}

queries.append (query)

#################################################################################################

query = { "id":53
  ,"active":True
  , "meta":"Pass UPC_ID back to F_INVENTORY" 
  , "sql" : """
UPDATE stg.f_inventory
SET upc_id     = upc.upc_id 
  , updated_at = getdate()
FROM stg.upc 
WHERE LTRIM(REGEXP_REPLACE ( upc.upc, '[^0-9]', ''),'0') = LTRIM( REGEXP_REPLACE ( f_inventory.upc, '[^0-9]', ''), '0')
  AND f_inventory.upc_id = -1
"""}

queries.append (query)


#################################################################################################

query = { "id":90
  ,"active":True
  , "meta":"Pass UPC tagged info to STG.UPC" 
  , "sql" : """
UPDATE stg.upc
SET type        = ut.type
  , category    = ut.category 
  , subcategory = ut.subcategory
  , sizing      = ut.sizing
  , type_id     = f_type.type_id
  , category_id = f_category.category_id
--, subcategory_id *** NOT ADDRESSED YET ***
FROM stg.upc_tagged ut 
  LEFT JOIN stg.f_type      ON ( ut.type     = f_type.type         )
  LEFT JOIN stg.f_category  ON ( ut.category = f_category.category )
  ---- Nothing for subcategory yet
WHERE LTRIM ( ut.upc, '0' ) = LTRIM ( upc.upc, '0' )
AND 1=0  --- DO NOT RUN THIS ANY MORE 2019-03-05
"""}

queries.append (query)

#################################################################################################
 
query = { "id":411
  , "active":True
  , "meta":"UPPER case on tagging" 
  , "sql" : """
UPDATE stg.upc_tagged 
SET "type"      = UPPER ( "type")
  , category    = UPPER ( category )
  , subcategory = UPPER ( subcategory )
"""}

queries.append (query)

#################################################################################################

query = { "id":412
  , "active":True
  , "meta":"UPPER case on tag values in UPC table" 
  , "sql" : """
UPDATE stg.upc
SET "type"      = UPPER ( "type")
  , category    = UPPER ( category )
  , subcategory = UPPER ( subcategory )
"""}

queries.append (query)

#################################################################################################

#Changed OPTICS from CATEGORY to TYPE on 2019-03-05

query = { "id":413
  , "active":True
  , "meta":"Populate the derived column PRODUCT TIER" 
  , "sql" : """
UPDATE stg.upc 
SET product_tier = CASE WHEN "type" IN ( 'FIREARMS','OPTICS') THEN "type" 
                        WHEN "type" ='OTHER' THEN CASE WHEN category IN ( 'AMMUNITION','OPTICS' ) 
                                                       THEN category 
                                                       ELSE 'ACCESSORIES' END END
"""}

queries.append (query)


####### TEMP TEMP TEMP TEMP TEMP ############## TEMP TEMP TEMP TEMP TEMP ########################


query = { "id":430
  , "active":True
  , "meta":"Maintain dual UPC logic for new hierarchy" 
  , "sql" : """
INSERT INTO stg.upc_new_hierarchy_dan
SELECT * FROM stg.upc
WHERE upc IN  (  SELECT upc FROM stg.upc
                 MINUS
                 SELECT upc FROM stg.upc_new_hierarchy_dan  )
"""}

queries.append (query)

####### TEMP TEMP TEMP TEMP TEMP ############## TEMP TEMP TEMP TEMP TEMP ########################
##### Turned off 2019-01-02...........

query = { "id":431
  , "active":False
  , "meta":"Maintain dual UPC logic for new hierarchy step-up" 
  , "sql" : """
  UPDATE stg.upc_new_hierarchy_dan
SET type        = CASE WHEN ut.category IN ( 'AMMUNITION','OPTICS' ) THEN ut.category    ELSE  ut.type     END
  , category    = CASE WHEN ut.category IN ( 'AMMUNITION','OPTICS' ) THEN ut.subcategory ELSE  ut.category END
  , subcategory = ut.subcategory
  , sizing      = ut.sizing
  , type_id     = f_type.type_id
  , category_id = f_category.category_id
--, subcategory_id *** NOT ADDRESSED YET ***
FROM stg.upc_tagged ut 
  LEFT JOIN stg.f_type      ON ( f_type.type         = CASE WHEN ut.category IN ( 'AMMUNITION','OPTICS' ) THEN ut.category    ELSE ut.type     END )
  LEFT JOIN stg.f_category  ON ( f_category.category = CASE WHEN ut.category IN ( 'AMMUNITION','OPTICS' ) THEN ut.subcategory ELSE ut.category END )
  ---- Nothing for subcategory yet
WHERE LTRIM ( ut.upc, '0' ) = LTRIM ( upc_new_hierarchy_dan.upc, '0' )
"""}

queries.append (query)

####### TEMP TEMP TEMP TEMP TEMP ############## TEMP TEMP TEMP TEMP TEMP ########################
##### Turned off 2019-01-02...........

query = { "id":433
  , "active":False
  , "meta":"Maintain dual UPC logic for new hierarchy step-up" 
  , "sql" : """
UPDATE stg.upc_new_hierarchy_dan
SET product_tier = CASE WHEN "type" IN ( 'FIREARMS', 'AMMUNITION','OPTICS' ) THEN "type" 
                        WHEN "type" ='OTHER' THEN 'ACCESSORIES' END 
    """}

queries.append (query)


####### TEMP TEMP TEMP TEMP TEMP ############## TEMP TEMP TEMP TEMP TEMP ########################

#################################################################################################

#### There is a possiblility that the subquery could return more than one record and result in exception
####  Be ready to re-write to test for only one record returned ####

query = { "id":721
  ,"active":True
  , "meta":"Derived Manufacturer IDs from RAW in F_TRANSACTIONS" 
  , "sql" : """
UPDATE stg.upc
SET manufacturer_id = sub.manufacturer_id
FROM (  SELECT t.upc_id, um.manufacturer_id
          FROM stg.f_transactions t
         JOIN stg.s_unique_manufacturers um
           ON t.manufacturer_raw_id = um.manufacturer_raw_id
        WHERE 1=1 
          AND t.upc_id <> -1
         GROUP BY t.upc_id, um.manufacturer_id ) sub
WHERE upc.upc_id = sub.upc_id 
  AND sub.manufacturer_id IS NOT NULL
  AND upc.official_from_mfr_flag IS NULL 
  AND (  upc.manufacturer_id < 0 
      OR upc.manufacturer_id IS NOT NULL )  
"""}

queries.append (query)


#################################################################################################

#### There is a possiblility that the subquery could return more than one record and result in exception
####  Be ready to re-write to test for only one record returned ####

query = { "id":722
  ,"active":False
  , "meta":"Derived Manufacturer IDs from RAW in F_INVENTORY" 
  , "sql" : """
UPDATE stg.upc
SET manufacturer_id = sub.manufacturer_id
FROM (  SELECT upc_id, manufacturer_id
          FROM stg.f_inventory t
         JOIN stg.s_unique_manufacturers um
           ON t.manufacturer_raw_id = um.manufacturer_raw_id
         GROUP BY upc_id, manufacturer_id ) sub
WHERE upc.upc_id = sub.upc_id 
  AND sub.manufacturer_id IS NOT NULL
  AND upc.official_from_mfr_flag IS NULL 
"""}

queries.append (query)


#################################################################################################

#### The RAW ID logic identifies more UPCs as belonging to Sig Sauer than the official list they 
###    provided until we decide how to handle this, mhy temporary solutuin is to assign them to -negative ID values

#### Changed to False -- Do not run on 2019-02-26 -------------------------

query = { "id":729
  ,"active":False
  , "meta":'Sig Sauer records which are not in the "official" list' 
  , "sql" : """
UPDATE stg.upc
SET manufacturer_id = -223
WHERE 1=1
  AND manufacturer_id = 223
  AND official_from_mfr_flag IS NULL
"""}

queries.append (query)

#################################################################################################


query = { "id":110
  ,"active":True
  , "meta":"Ensure that all records have the same value in AMOUNT and AMOUNT_USD" 
  , "sql" : """
UPDATE stg.f_transactions
SET amount_usd = amount
WHERE amount_usd <> amount
"""}


#################################################################################################
## Added 2019-02-19

query = { "id":44
  ,"active":True
  , "meta":"consistency of distr name and id in Distributor Security" 
  , "sql" : """
UPDATE STG.F_DISTRIBUTOR_security
-- Distributor name is not used  in any logic, just for covenience of user
  SET distributor_name  = d.distributor_name
  FROM stg.f_distributor d
WHERE F_DISTRIBUTOR_security.distributor_id  = d.distributor_id
"""}

#################################################################################################
## Added 2019-01-17

query = { "id":45
  ,"active":True
  , "meta":"consistency of mfr name and id in Manufacturer Security" 
  , "sql" : """
UPDATE STG.F_MANUFACTURERS_security
-- Manufacturer name is not used  in any logic, just for covenience of user
  SET manufacturer_name  = m.manufacturer
  FROM stg.f_manufacturer m
WHERE F_MANUFACTURERS_security.manufacturer_id  = m.manufacturer_id
"""}

#################################################################################################
## Added 2019-01-17

query = { "id":46
  ,"active":True
  , "meta":"consistency of mfr name and id in Manufacturer Product Tier" 
  , "sql" : """
UPDATE STG.F_MANUFACTURERS_PRODUCT_TIER
-- Manufacturer name is not used  in any logic, just for convenience of user
  SET manufacturer_name  = m.manufacturer
  FROM stg.f_manufacturer m
WHERE F_MANUFACTURERS_PRODUCT_TIER.manufacturer_id  = m.manufacturer_id
"""}

queries.append (query)

#################################################################################################
## Added 2019-02-07

query = { "id":246
  ,"active":True
  , "meta":"Official mfr records into UPC table" 
  , "sql" : """
UPDATE  stg.upc  
  SET manufacturer_id = mo.manufacturer_id
   , official_from_mfr_flag = 'Y'
FROM stg.upc_manufacturer_official mo
WHERE LTRIM ( mo.upc, '0')  = LTRIM ( upc.upc, '0')
  AND (  mo.manufacturer_id <> upc.manufacturer_id
      OR official_from_mfr_flag IS NULL )
"""}

queries.append (query)


#################################################################################################

print_separator()
        
for query in queries:

    print ( "Query: " + query ["meta"])
	
    if execute_this_query ( query["id"], query["active"] ):
	
     ####   cursor.execute( query["sql"] ) 
        print ( "EXECUTE Query ID = {}".format (	query["id"] ))	
    else:
        print ( "SKIP Query ID = {}".format (	query["id"] ))	
    ####    print ( "Query not executed.")
	
    print_separator()

# You must specify a region....
#boto3.setup_default_session(region_name='us-west-2')

#sns = boto3.client('sns')


#######NOT WORKING -- Maybe fixed 2018/09/29
#sns.publish(
#        TopicArn='arn:aws:sns:us-west-2:680912259910:nasgw-data-quality-etl'
#		, Message="The UPC Update ETL ran"
#        , Subject="NASGW Data Quality ETL")
#	
#arn:aws:sns:us-west-2:680912259910:nasgw-data-quality-etl

if (running_in_aws_glue_flag):
    job.commit()
