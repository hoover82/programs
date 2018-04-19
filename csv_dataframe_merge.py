

### This can parse the CSV
### SHows how to plot it

## Constants defined for file operations
READ = "r"
WRITE = "w"
APPEND = "a"
READWRITE = "w+"
BINARY = "b"

#csvdata = myfile.read ()
#myfile.close()

from datetime import date, time, datetime
import datetime as D_T

# probably will not need this
#import csv

import pandas as pd

filenames = [ "us_presidents_birth_death", "us_presidents_terms"]

filenames = [ "data\\" + f + ".csv" for f in filenames ]

df = []
i = 0

print ( filenames )

for f in filenames:
    print ( f )

    df.append ( pd.read_csv( f ))

    print ( df[i].head( 30) )
    i += 1

quit()

#creating a Pandas DataFrame
#df = pd.DataFrame (data.data, columns=data.feature_names)

#Instead of what's above wherein we import the data and then assign
#  column names, the technique below creates dictionary objects and
#  then the dictionary labels become the column headers

#df = pd.DataFrame ( { "snowpack"  : snowpack
#                    , "open_days" : opening_daynos } )

df_X = pd.DataFrame ( { "snowpack"  : snowpack } )
    





import numpy as np
from sklearn import linear_model
#from sklearn.metrics import mean_squared_error, r2_score

print ( "Chart rendered...")
