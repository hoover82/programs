
# python C:\dan\alignBI\NASGW\python_scripts\create_json.py



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


def dbl_quotes ( str ):
    return '"{}"'.format(str)

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


#This one is OK
qry = """
SELECT manufacturer_name,manufacturer_id,user_name
FROM STG.f_manufacturers_security
WHERE LOWER ( user_name ) NOT LIKE '%_dev'
  AND LOWER ( user_name ) NOT LIKE '%_demo'
  AND LOWER ( user_name ) NOT IN ( 'felixm','dans','eastonk','jayh','cobyg')
"""

qry = """
SELECT * FROM information_schema.schemata
"""

try:
   cursor.execute (  qry )
except Exception as error:
    print ("\nQuery error!")
    print ("\nError message: {}".format(error))
else:
    print ("\n* * * The query was successful! * * *")


colnames = [desc[0] for desc in cursor.description]
commas = []
for i,c in enumerate(colnames):
    commas.append(',')
commas[len(colnames)-1] = ''    

rslt = cursor.fetchall()

records = []
for rec in rslt:

    record = "{"

    for i,f in enumerate(rec):
        record += '"{}":"{}"{}'.format(colnames[i].decode('UTF-8'),f,commas[i])

    record += "}"
    records.append(record) 

commas = []
for i,r in enumerate(records):
    commas.append(',')
commas[len(records)-1] = ''    


file = """{"manufacturers":
[
"""
for i,r in enumerate(records):
    file += '  {}{}\n'.format ( r, commas[i] )

file +="""]
}"""

print (file)
    










