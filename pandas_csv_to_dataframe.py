
import pandas as pd

filename = "data\\apportionment2010.csv"

filename = "data\\us_presidents_birth_death.csv"
## President, Birth date, Birth Place, Death Date, Death Location
df1=pd.read_csv(filename)

filename = "data\\us_presidents_years.csv"
## President, BirthYr, TermBeginYr, TermEndYr
df2=pd.read_csv(filename)

filename = "data\\us_presidents_terms.csv"
## No., Name, Term of Office (with dashes)
df3=pd.read_csv(filename)

# change column header for president name to match the other two DFs
df3.rename ( columns = { "Name":"President"},inplace = True)

##name_length1 = pd.DataFrame({'nameLength':
##                             df1.President.length()})

name_length1 = pd.DataFrame({'nameLength': df1.President.str.len()})
name_length2 = pd.DataFrame({'nameLength': df2.President.str.len()})
name_length3 = pd.DataFrame({'nameLength': df3.President.str.len()})

#new_df1 = pd.concat ( [df1,name_length1], axis=1)
#new_df2 = pd.concat ( [df2,name_length2], axis=1)
#new_df3 = pd.concat ( [df3,name_length3], axis=1)

new_df1 = pd.concat ( [ name_length1, df1 ], axis=1)
new_df2 = pd.concat ( [ name_length2, df2 ], axis=1)
new_df3 = pd.concat ( [ name_length3, df3 ], axis=1)

#df3 ['My_test'] = len(df3['President'])

## ##print (name_length1)

if 1 == 0:
    print ( "AVENAL")

    print ( " * * * * * * * * * * * * * * * *")
    print ( " *  * * * * new_df1 * * * * * * ")
    print ( "Has president name, DOB, death, places of birth and death")
    print ( "0-43")
    print ( " * * * * * * * * * * * * * * * *")
    print (new_df1)

    print ( "BIOLA")

if 1==0:
    print ( "CALWA")

    print ( " * * * * * * * * * * * * * * * *")
    print ( " *  * * * * new_df2 * * * * * * ")
    print ( "President, birthyear, termyears" )
    print ( "0-44")
    print ( " * * * * * * * * * * * * * * * *")
    print (new_df2)

    print ( "DINUBA")

if 1==0:
    print ( "EASTON")

    print ( " * * * * * * * * * * * * * * * *")
    print ( " *  * * * * new_df3 * * * * * * ")
    print ( " No., President, term ")
    print ( "0-44")
    print ( " * * * * * * * * * * * * * * * *")
    print (new_df3)

    print ( "FRIANT")

#This concatenates by appending rows below one another (axis=0 is default)
#df = pd.concat ( [df2,df3] )
#df = pd.concat ( [df2,df3],axis=0 )

#This concatenates by columns
#df = pd.concat ( [df2,df3],axis=1 )
#See the last few values, with columns with similar data
#print (df.ix[38:,[0,5,2,3]])

#print (df.columns)

print ( "GOSHEN")

# # All columns, first three rows
#df = pd.concat ( [new_df3.loc[:2],new_df2.loc[:2]],axis=1)

if 1 == 0:
    df = pd.concat ( [new_df3.loc[40:44],new_df2.loc[40:44]],axis=1)
    print (df)

    print ( "HURON")


## Cannot use "axis" with "merge"
#df = pd.merge ( df2, df3, on="President",axis=0) ## This is an Error

df = pd.merge ( df2, df3, on="President")
#### SUCCESS!!! This joined on the 26 President names which are the same in the two datasets !!!



print ( " * * * * * * * * * * * * * * * *")
print ( " *  Merging on President -- works * * * ")
print ( " * * * * * * * * * * * * * * * *")
print (df)


## Fill in NaN values with  the value from a different column
## df["TermEndYr"].fillna(value=df["TermBeginYr"])
#(This is not what  I want to do, but it works)

## Fill in the NaN value with the current year
from datetime import datetime
df["TermEndYr"].fillna(value = datetime.today().year )


#print (df.columns)

## ##print ( df)
#print ( df.ix [38:])

#Use loc to address by index values

#print(df.loc [38:,"Birth Date"])

#print(df.loc [38:,["President","Birth Date"]])

#print(df.ix [20:30,["President","Birth Date","Death Date"]])
##
##df.loc["COUNT"] = df.ix [20:30,"President"].count()
##
##print(df.ix [:,["President","Birth Date","Death Date"]])




