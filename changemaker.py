

#creating a list (array) of the values in my monetary system

#Doesn't matter whether we use tuple or list (array) here.
#Tuple is probably preferred because these values do not change at runtime

#Tuple format
bills_and_coins_values = ( 100, 50, 20, 10, 5, 1, 0.25, 0.10, 0.05, 0.01 )   #USA
#bills_and_coins_values = ( 100, 50, 20, 10, 5, 2, 1, 0.50, 0.20, 0.10, 0.05, 0.01 )   #EURO
#bills_and_coins_values = ( 100, 50, 20, 10, 5, 2, 1, 0.25, 0.10, 0.05, 0.01 )   #CANADA
#bills_and_coins_values = ( 1000, 500, 100, 50, 20, 10, 5, 2, 1, 0.50, 0.20, 0.10 )   #HONG KONG

#bills_and_coins_names = ("Twenty", "Ten", "Five", "One", "Quarter", "Dime", "Nickel", "Penny")

sale_amt = float(input ( "Enter amount of sale: " ))
tender_amt = float(input ( "Enter amount tendered: " ))

change_amt = tender_amt - sale_amt

print ("\n")
#print ("\nSale --> tendered --> change")
print ("Sale: %4.2f" % (sale_amt))
print ("Tendered: %4.2f" % (tender_amt))
print ("Change: %4.2f" % (change_amt))
print ("\n")

#Calculating the denominations and coins

remaining_amt = change_amt
change = [ ]  #Creates empty list.
#As each bill/coin value is encountered, the array will grow by one

for x in range ( len(bills_and_coins_values)):

    #Appends a new item into the array.
    #The value appended is the number of the particular denomination of bill or coin to be included in the change 

    change.append( int(remaining_amt / bills_and_coins_values [x]) )

    #Reduce the remaining amount by the amount just included in this demonimation0
    #(Two ways: modulo or -= ; both accomplish the same thing)

    remaining_amt = round ( remaining_amt % (bills_and_coins_values [x]), 2)

for x in range ( len(bills_and_coins_values)):
     print ("{:6.2f}".format (bills_and_coins_values [x]) 
	       + " × " + "{:1d}".format (  int(change [x])) 
		   + ' = ' + "{:6.2f}".format ( bills_and_coins_values [x] * change [x] )
		   )
