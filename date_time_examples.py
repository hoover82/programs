#from datetime import datetime, date, time
import datetime

#date and time -- today's date -- no time component
dan = datetime.date.today()
print (dan);

#dan = datetime.now()
#print (dan);  

dan = datetime.time()
print (dan);  #00:00:00


#hard-code a date
dan = datetime.datetime(2013, 6, 9, 11, 13, 3, 57000)
print (dan);


# Combine a date and a time
d = date(2017,11,18)
t = time(13,45,23)
dt = datetime.datetime.combine ( d,t)

print ("\nCombining date and time")
print (d)
print (t)
print (dt)


print ("\nAdding and substracting with dates")

#Adding to a date

date1 = date (2017,11,18)
date2 = date (1964, 2, 12)

dan = date2 + datetime.timedelta (days=20000) #Adding 20,000 days to my DOB
print ( dan)

#Difference between dates
print (date1 - date2)


print ("\nFormatting date output")

print ( dan.strftime( "%d %b, %y"))            
print ( dan.strftime( "%b %d %y"))            



dates = (1000, 2000, 3000, 4000, 5000, 10000, 20000, 30000)

# Examples of both styles of formatting, right-aligned
for i in range (len(dates)):
    print ("%5d" % (dates[i]))
    
for i in range (len(dates)):
    print ("{0:5d}".format(dates[i]))

#DOB entered as String (input always accepts String)
DOB_str = input ("\nEnter DOB in format MM/DD/YYYY: ")

# # Firstone results in 00:00:00 on end, second one, not
#DOB = datetime.datetime.strptime(DOB_str, "%m/%d/%Y")
DOB = datetime.datetime.strptime(DOB_str, "%m/%d/%Y").date()

print (DOB)
                                 
for i in range (len(dates)):

#   print ( DOB + datetime.timedelta (days=dates[i]))
    print ("{0:5d} days on {1:%Y-%m-%d}".format(dates[i], DOB + datetime.timedelta (days=dates[i])))


