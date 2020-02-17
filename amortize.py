

# python C:\dan\learn\python\amortize.py


# Credit: Taken from here: https://blog.easyaspy.org/post/8/2019-01-17-calculating-amortization-with-python
#

def calculate_amortization_amount(principal, annual_interest_rate, period):
    periodic_interest_rate  = annual_interest_rate/12
    x = (1 + periodic_interest_rate) ** period
    return round( principal * (periodic_interest_rate * x) / (x - 1),2)

def build_amortization_schedule(beginning_principal, annual_interest_rate, payment_amt):
    number = 1
    bal = beginning_principal
    ret_val = []
    while bal>0:
        i= round(bal *  ( annual_interest_rate /12),2)
        p = round(payment_amt-i,2)
        bal -= p
        bal = round(bal,2)
        ret_val.append ( (i,p,bal)  )	
    return ret_val

def build_months_list(first_payment_tuple,number_of_pmts):
# first_payment_date should be tuple in format (YYYY,MM), ie: (2019,5)
    (yr,mo)= first_payment_tuple 
    ret_val = []	
    number = 1
    while number <= number_of_pmts: 
        ret_val.append(( yr,mo))
        mo += 1
        if mo == 13:
            mo =1
            yr+=1			
        number +=1	
    return ret_val


amt = 365500
pct = 3.75
pmts = 360
#mopmt = round(calculate_amortization_amount(amt, pct/100, pmts),2)
mopmt = calculate_amortization_amount(amt, pct/100, pmts)

current_balance = 325218.36


print ( "Amortizing mortgage amt: {}".format(amt))
print ( "Interest rate: {}%\n".format(pct))

print ( "Payment is ${}".format ( mopmt ))		


amortization = build_amortization_schedule(amt, pct/100, mopmt) 
months_list  = build_months_list ( (2015,5) , pmts)

closest_diff = amt
closet_pmt = pmts
marker = 'X'

for i, ( ( yr, mo ),( int, p, bal ) ) in enumerate( zip ( months_list, amortization ) ):
    if abs ( current_balance - bal ) < closest_diff:
        closest_diff = abs ( current_balance - bal )
        closest_pmt = i	
        closest_bal = bal
#    print ( '{}\t{}\t -->\t {}\t{}\t{}\t{}\t{}\t{}'.format( closest_pmt,closest_bal, i,yr,mo,int,p,bal ))

print ( '{} -- {}'.format ( closest_pmt, closest_bal))
print ( 'Remaining payments: {}'.format( 360-closest_pmt))


#    print ( '{}\t{}\t{}\t{}\t{}\t{}'.format(i,yr,mo,int,p,bal ))


"""
===========================
FiveThirtyEight style sheet
===========================

This shows an example of the "fivethirtyeight" styling, which
tries to replicate the styles from FiveThirtyEight.com.
"""

from matplotlib import pyplot as plt
import numpy as np


plt.style.use('fivethirtyeight')

x = np.linspace(0, 10)

# Fixing random state for reproducibility
np.random.seed(19680801)

fig, ax = plt.subplots()

ax.plot(x, np.sin(x) + x + np.random.randn(50))
ax.plot(x, np.sin(x) + 0.5 * x + np.random.randn(50))
ax.plot(x, np.sin(x) + 2 * x + np.random.randn(50))
ax.plot(x, np.sin(x) - 0.5 * x + np.random.randn(50))
ax.plot(x, np.sin(x) - 2 * x + np.random.randn(50))
ax.plot(x, np.sin(x) + np.random.randn(50))
ax.set_title("'fivethirtyeight' style sheet")

if 1==0:
    plt.show()




