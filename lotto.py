
# Very basic code that checks a powerball ticket.  
# Enter arrays of numbers into the ticket -- this is an array of arrays. Each outer array entry is one line from the ticket
#  The nested array is the numbers from that line
# Because the powerball number is from a different universe/domain, there is a separate array for those -- one entry per line
# Ticket checking is accomplished by using "sets" -- arrays are converted to sets
# 
# Written 2017/12/24, Dan Stober

ticket = []
powerTicket = []

ticket.append ( [  2,  4,  7, 21, 60 ] )
ticket.append ( [ 19, 36, 43, 56, 58 ] )
ticket.append ( [  7, 16, 22, 39, 54 ] )
ticket.append ( [ 20, 24, 30, 42, 47 ] )
ticket.append ( [ 26, 38, 51, 62, 68 ] )


powerTicket.append ( 22 )
powerTicket.append (  7 )
powerTicket.append ( 23 )
powerTicket.append ( 18 )
powerTicket.append (  1 )


#winners = [  1, 20, 61, 64, 69]
#powerWinner = 20

winners = [  1, 3, 13, 15, 44 ]
powerWinner = 25

#print ( set (ticket01) & set (winners ) )

for i in range ( len (ticket)):
    matches = set(ticket[i]) & set (winners)

    print ( "Line " + str(i + 1) + ":  " + str(len ( matches )) + " matches")
    print ( matches )

    if powerTicket [i] == powerWinner:
        print ("Power match!")
    else:
        print ("Power NOT matched")

    print ()


