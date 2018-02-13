# programs

This repository is just for some example code I've written while learning

changemaker.py - Accepts values for sale amount and amount tendered and calculates the change, printing out the number of each coin and bill. Denomications are loaded into a tuple -- uncomment the one for the currency you want to use. 
One of my earliest programs; code written 2017/11/18, refined 2018/02/13 for placement into github. There still are some known flaws: 
   Does not reject entry when amount tendered is less than sale amount.
   Does not test user entries to ensure numeric values
   Assumes all currencies are accruate to two decimal places. (This is not true for HKD (Hong Kong dollar), MXP (Mexico peso), THB (Thai
       baht) for example
