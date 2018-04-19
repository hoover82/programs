
### Not used for any of these charts
#from sklearn import linear_model


def chart_1():
    """
    ====================
    A Simple Linear Plot
    ====================

    Values a graphed on the Y-Axis.
    The X-Axis is just the indecx values of the array

    """

    import matplotlib.pyplot as plt

    #Passing an array, and it ends up on the the Y-Axis
    # I thought it would end up on the X-Axis is null

    plt.plot([10,20,30,40],label='Hello')

    #Plotting a second line
    # This time specifically passing the X-Axis values
    plt.plot([0,1,2,3],[15,20,25,30], label="Dan is cool")

    #This does nothing if I have not given it a line to graph
    plt.plot(label="overridden value")

    #I could add "xlabel", too
    plt.ylabel('some numbers')

    #The label specified above will not show up without this statement
    plt.legend()

    plt.show()

    return()


#import matplotlib.pyplot as plt
#import numpy as np

## Prepare the data
#x = np.linspace(0, 10, 100)

## Plot the data
#plt.plot(x, x, label='linear')

## Add a legend
#plt.legend()

## Show the plot
#plt.show()

def chart_2():

    import matplotlib.pyplot as plt
    import numpy as np

    #Plot of year of birth versus first year of term for US presidents
    #Using arrays here but I think you can use numpy.lin, too
    #birth_years = [1732,1735,1743,1751,1758,1767,1767,1782,1773,1790,1795,1784,1800,1804,1791,1809,1808,1822,1822,1831,1829,1837,1829,1833,1843,1858,1857,1856,1865,1872,1874,1882,1884,1890,1917,1908,1913,1913,1924,1911,1924,1946,1946,1961,1946]
    #terms_start = [1789,1797,1801,1809,1817,1825,1829,1837,1841,1841,1845,1849,1850,1853,1857,1861,1865,1869,1877,1881,1881,1885,1889,1893,1897,1901,1909,1913,1921,1923,1929,1933,1945,1953,1961,1963,1969,1974,1977,1981,1989,1993,2001,2009,2017]

    # Scatter the data
    #ax.scatter(np.linspace(0, 1, 5), np.linspace(0, 5, 5))

    birth_years = np.linspace(1732,1735,1743,1751,1758,1767,1767,1782,1773,1790,1795,1784,1800,1804,1791,1809,1808,1822,1822,1831,1829,1837,1829,1833,1843,1858,1857,1856,1865,1872,1874,1882,1884,1890,1917,1908,1913,1913,1924,1911,1924,1946,1946,1961,1946)
    terms_start = np.linspace(1789,1797,1801,1809,1817,1825,1829,1837,1841,1841,1845,1849,1850,1853,1857,1861,1865,1869,1877,1881,1881,1885,1889,1893,1897,1901,1909,1913,1921,1923,1929,1933,1945,1953,1961,1963,1969,1974,1977,1981,1989,1993,2001,2009,2017)

    ###Figure out how to plot loser years too

    # plot type "ro" = "red" "circles"
    # Other choices for color
    #colors = {
    # b : blue.
    # g : green.
    # r : red.
    # c : cyan.
    # m : magenta.
    # y : yellow.
    # k : black.
    # w : white.
    # Shapes: "o" Circles, "^" Triangles, "x" X, "*" Stars
    # #############################

    plt.plot(terms_start, birth_years, "g*" )
    plt.xlabel('Year term began')
    plt.ylabel('Year of birth')
    plt.show()

    return()

def chart_3():

    """
    ====================
    Horizontal bar chart
    ====================

    This example showcases a simple horizontal bar chart.
    """
    import matplotlib.pyplot as plt
    plt.rcdefaults()
    import numpy as np
    import matplotlib.pyplot as plt

    plt.rcdefaults()
    fig, ax = plt.subplots()

    # Example data
    #people = ('Tom', 'Dick', 'Harry', 'Slim', 'Jim')
    people = ('Trump', 'Trudeau', 'May', 'Merkel', 'Macron')
    #y_pos = np.arange(len(people))

    performance = ( 46, 30, 26, 40, 35)
    #performance = 3 + 10 * np.random.rand(len(people))
    error = np.random.rand(len(people))

    ax.barh(people, performance, xerr=error, align='center',
        color='green', ecolor='black')
    #ax.set_yticks(y_pos)
    ax.set_yticks(people)

    ax.set_yticklabels(people)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel('Approval ratings - Nov 2017')
    ax.set_title('Is Trump the most popular Western leader?')
    
    plt.show()
    
    return()

def chart_4():

    #https://www.datacamp.com/community/tutorials/matplotlib-tutorial-python

    # Import `pyplot`
    import matplotlib.pyplot as plt

    # Initialize a Figure 
    fig = plt.figure()

    # Add Axes to the Figure
    fig.add_axes([0,0,1,1])

    plt.show()

    return()

#This is a switch to run just one of the charts

print ("Chart selection...")
print ("  1) Simple linear plot")
print ("  2) Scatter plot")
print ("  3) Horizontal Bar Chart")
print ("  4) XXX")
PlotChoice = int( input ("Enter an integer: "))

       
if (PlotChoice == 1):

    chart_1()

elif (PlotChoice == 2):

    chart_2()

elif (PlotChoice == 3):

    chart_3()
    
elif (PlotChoice == 4):

    chart_4()

print ( "ALL DONE")




