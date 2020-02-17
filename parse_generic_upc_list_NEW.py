
#############################################################################################
# Use this to parse a generic CSV File.  It will parse the file, creating a table called 
#   tmp.import, with fields which match the headers in the file 
#
#    2018-11-06, Dan Stober
#    Revised 2019-10-01
#
#        python C:\dan\alignBI\NASGW\python_scripts\parse_generic_upc_list_NEW.py
#
############################################################################################

#from smart_open import smart_open

import csv
import re
import json

import boto3
import pg8000 as pg

from itertools import chain, islice

import time

## Constants defined for file operations
READ = "r"
IMPORT_TABLE = 'tmp.import'
			
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
	
    return ( cursor )

def column_name_cleanup ( colname ):
	#Need to add regex to clean up illegal characters...
	#retval = colname.translate( "abcdefghijklmonpqrstuvwxyz", "_")
    #And.. at least once, a column name was a keyword....

	
    tmp1 = colname.replace( " ", "_")
    tmp2 = tmp1.replace( "/", "_")
    tmp1 = tmp2.replace( "-", "_")
    tmp2 = tmp1.replace( ".", "_")

    tmp1 = tmp2.replace( "=", "_")
    tmp2 = tmp1.replace( ":", "_")

    tmp1 = tmp2.replace( ",", "_")
    tmp2 = tmp1.replace( "\n", "_")

    tmp1 = tmp2.replace( "[", "_")
    tmp2 = tmp1.replace( "]", "_")

    tmp1 = tmp2.replace( "(", "_")
    tmp2 = tmp1.replace( ")", "_")
    tmp1 = tmp2.replace( "#", "_NUM")
    tmp2 = tmp1.replace ( "[^a-z0-9]_", "" )

#    line = line.replace("\0", "")
#  #  line = line.replace(u"\81", "")
#\ufeffmaterial__NUM
	
	#2019-05-21: it turns out that "delta" is not a valid column name -- I think it's a reserved word
    if tmp2 == "delta":
        tmp1 = "_delta"
    else:
        tmp1 = tmp2 
	
    retval = tmp1
#   print (retval)
    return retval
	
def create_import_table (cursor, cols):

	#Need to test for cols which exist more than once
    cursor.execute( 'DROP TABLE IF EXISTS {}'.format(IMPORT_TABLE) )	

    cursor.execute( 'CREATE TABLE {} ( lineno int )'.format(IMPORT_TABLE) )	
    for col in cols:
        alter_stmnt = 'ALTER TABLE {} ADD COLUMN {} varchar(255)'.format( IMPORT_TABLE , column_name_cleanup (col) )
        print ( alter_stmnt )
        cursor.execute( alter_stmnt )

#NOT IN USE SO FAR, JUST CODE STOLEN FROM BRIAN 	
def groups(it, size):
    """Utility function to iterate in groups of a given size."""
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())
	
def insert_rows (cursor,rows, errors):

    then = time.time() #Time before the inserts start

    print ( "Rows in file {}: ".format(len(rows)))		

    for i,row in enumerate (rows):
		
        if i%100 == 0:
		
            now = time.time() #Time after it finished
            print ( "Inserting row {} ... timer {} seconds".format(i, (now-then)))		

#            print("It took: ", now-then, " seconds")

#        firstone = True
        firstone = False
		
        stmnt = r'INSERT INTO {} VALUES ( {}'.format( IMPORT_TABLE , i)
	
        stmnt += ')'

#       print ( stmnt ) #######################################

        try:
#            cursor.execute ( stmnt )
            x=1
        except Exception:
            print("While processing row {}: {}\n{}".format(idx+1, row))
            errors.append ( row )
            raise

#   python C:\dan\alignBI\NASGW\python_scripts\parse_generic_upc_list_NEW.py

#pathAndFilename = r"C:\dan\alignBI\NASGW\wendy\export_untagged_201911 COMPLETED.csv"
pathAndFilename = r"C:\dan\alignBI\NASGW\wendy\export_untagged_201912COMPLETED.csv"

#### Change delimiter here
delimiter = ","
#delimiter = "|"

##############################
##############################
if 1==1:
### with open( pathAndFilename, READ, encoding='utf-8' ) as csvfile:
    with open( pathAndFilename, READ, encoding='utf-8-sig' ) as csvfile:
### ENCODING NOTE:  Most files I get seem to a byte \ufeff, which apparently is an embedded encoding marker.
###  I had never been able to get rid of it using replace or any other hack I tried.  I finally found this solution on
###    this page (which offers a good explanation and a couple more ways around it):
###  https://stackoverflow.com/questions/17912307/u-ufeff-in-python-string
###  I may also want to try Brian's sniffer method from csv_wrapper here sometime.  2019-12-03
#cp1252

#    with open( pathAndFilename, READ ) as csvfile:

#    reader = csv.reader(csvfile, delimiter=',')
        reader = csv.reader(csvfile, delimiter=delimiter)
		
        col_name_mode = input ("Do you want to use the column names from the first linein the file? (Y=Use from file, N=Keep generic) ")

        if col_name_mode.upper() == 'Y':
            file_cols = [ column_name_cleanup( key.strip().lower()) for key in next(reader)]
        else:	
            file_cols = []
            for idx, key in enumerate ( next(reader)):
                file_cols.append ( 'column_{}'.format(idx) )

        renamed_cols = []
        rows = []

        col_list = '( lineno '

        print ( "Length of column list is {}".format( len(file_cols)) )

        for idx, col in enumerate(file_cols):

            if idx == 0 and col_name_mode == 'names':
	
	        #There are consistent problems with the first column. I don't understand why, but the first two characters
			# of the column name get chopped off
	
                ALPHA_ONLY = re.compile(r'^[A-Za-z]', re.IGNORECASE)
                match = ALPHA_ONLY.search(col)
                print ("Match on first character is {}".format(match))
                print ("length of Match  {}".format(match))

                col_list += ','
                tmpcol = col[3:]
			
#####			column_name_cleanup 
            else:	
                col_list += ','
                tmpcol = col
		
# Do this only if garbled stuff shows up in front of the first column name			
#       tmpcol = col[3:]

            if tmpcol == "":
                tmpcol = "col_{}".format(idx)
		
#            print ( "{}: {}".format( idx,tmpcol ))
            col_list += tmpcol
            renamed_cols.append(tmpcol)
		
        col_list += ')'
	
        print (col_list)
        print (renamed_cols)
	
#        dan=0
#        priorrow = ""

        J= 0	
        for row in reader:	
			
            this_row = [key.strip() for key in row]
#           rows.append ( this_row )

            #Converting to DICT and then bringing it back to LIST ensures that the number of fields is correct
            record = { key:val for key,val in zip ( renamed_cols, this_row ) }			
            listify = [ record[key] for key in renamed_cols ]

            rows.append ( listify )

 			

            J +=1

            if J == 5002:
                print ( record )
                print ( listify )

					
        print ( 'AFTER chunk call')
		
        cursor = connect_db ( "redshift/nasgwdw/nasgwadmin" )
		
        create_import_table ( cursor, renamed_cols )

        errors = []

        x = chain ( renamed_cols, rows )
        print ( ' * * * LENGTH OF chain is {} * * * '.format ( len ( list (x) )))		
		
        stmt_row = '({})'.format(", ".join( '%s' for c in renamed_cols ))

        first_time = False		
        group_size = 1000
        group_counter = 0




        then = time.time() #Time before the inserts start
		
        for chunk in groups ( rows, group_size ):
			
            stmt = "INSERT INTO {} ( {} ) VALUES \n{}".format( IMPORT_TABLE, ', '.join ( renamed_cols ), ',\n'.join([stmt_row]*len(chunk))) 
            params = list(chain.from_iterable(chunk) )				
			
            if first_time:
                first_time = False		
                print (stmt)
                print ( params )

            cursor.execute ( stmt, params )
            group_counter += 1

            now = time.time() #Time after it finished

            print ( 'Chunk {} inserted. Rows Group = {}: {} seconds'.format ( group_counter, ( group_counter * group_size ), (now-then)))
			
        print ( 'AFTER chunk inserts')

##################################################		
#        insert_rows (cursor, rows, errors)

    #ADD COLUMN for source_filename...
        cursor.execute( 'ALTER TABLE {} ADD COLUMN source_filename VARCHAR(255)'.format ( IMPORT_TABLE ))	
        cursor.execute( "UPDATE {} SET source_filename = '{}'".format ( IMPORT_TABLE, pathAndFilename ))	

#    print ( errors)

print ( 'Count of rows is {}'.format ( len ( rows ) ))
print ( 'Load completed into table "{}"'.format ( IMPORT_TABLE ) )
