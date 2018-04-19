
#Tax rates from
# http://www.businessinsider.com/senate-tax-plan-brackets-trump-tax-reform-two-charts-2017-11

income = float(input ( "Enter income: " ))

#single filers - 2017
ceilings = (0,9325,37950,91900,191650,416700,418400)
rates = (0,.10,.15,.25,.28,.33,.35,.396)
#64635.96 = 11897.74

#single filers - senate bill
ceilings = (0,9525,38700,70000,160000,200000,500000)
rates = (0,.10,.12,.22,.24,.32,.35,.385)
#68685.96 (no personal exemption) = 11050.41

#MFJ 2017
ceilings = (0,18650,75900,153100,233350,416700,470700)
rates = (0,.10,.15,.25,.28,.33,.35,.396)
#67737.54 = 9228.13

#MFJ Senate bill
ceilings = (0,19050,77400,140000,320000,400000,1000000)
rates = (0,.10,.12,.22,.24,.32,.35,.385)
#79887.54 = 9454.26

#single filers - 2017
ceilings = (0,9325,37950,91900,191650,416700,418400)
rates = (0,.10,.15,.25,.28,.33,.35,.396)
#64635.96 = 11897.74

#NEW 2018 n- Not verified with IRS
#https://files.taxfoundation.org/20171220113959/TaxFoundation-SR241-TCJA-3.pdf
#Singles
ceilings = (0,9525,38700,82500,157500,200000,500000)
rates = (0,.10,.12,.22,.24,.32,.35,.37)


incomeIsLess = True
tax = 0

i=0

while incomeIsLess:
    if  income >= ceilings[i]:    
        tax += (rates[i] * (ceilings[i]-ceilings[i-1]))    

        i += 1

        if i == len(ceilings):
            incomeIsLess = False
        
    else:
        incomeIsLess = False

tax += (rates[i] * (income-ceilings[i-1]))    

effRate = (tax/income)*100
    
print ("Tax: %4.2f" % (tax))
print ("Effective rate: %1.4f" % (effRate))
