#%% youtube tutorial. Lesson 1
from matplotlib import pyplot as plt
import numpy as np
import math


# to include "STYLE" into our plots
# to check what styles are available plt.style.available in command line
plt.style.use("fivethirtyeight")


# styles for fun
# plt.xkcd()


# Median Java Developer Salaries by Age
ages_x = [25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35]
plt.xlabel("Ages")
c_dev_y = [37810, 43515, 46823, 49293, 53437, 56373, 62375, 66674, 68745, 68746, 74583]
plt.ylabel("Median Salary (USD)")
plt.title("Median Salary (USD) by Age")
# the arguments within the plot need not be in order actually.
# MUST individually invoke plot() command to include the set of data into graph. "k--" to make graph
# black and dotted. OR make it more readable. marker == dots throughout the line graph. Hex colour values
# linewidth change thiccness of plot.
plt.plot(ages_x, c_dev_y, color = "#adad3b", linestyle = "-", marker = "o", linewidth = 1, label = "c++")


# Median C++ Developer Salaries by Age. x-axis remains the same, change the rest
# This graph command code is written last, it gets the top most layer priority. Shift this segment of code
# to display the graph underneath
dev_y = [38496, 42000, 46752, 49320, 53200,
         56000, 62316, 64928, 67317, 68748, 73752]
plt.plot(ages_x, dev_y, color = '#444444', linestyle = "--", marker = '.', linewidth = 8, label = "All Devs")


# Median Python Developer Salaries by Age
# we can just include another set-of y-values to include in the graph, dont need another set of
# x-axis
py_dev_y = [45372, 48876, 53850, 57287, 63016, 65998, 70003, 70000, 71496, 75370, 83640]
# Automatically include this set of data into the above existing axis' tgt with the title and axis'
# titles. "r" to make line graph red. Bigger dots thorughout the line graph. 
plt.plot(ages_x, py_dev_y, color = "#5a7d9a", linestyle = "-", marker = "o", linewidth = 3, label = "Python")


# include grid for the x & y axis. Must include true to force the lines out. If have a "style" applied,
# the grid might not show
# plt.grid(True)


# MUST invoke this legend command to "actualize" those legends arguments in your .plot() commands
plt.legend()


# to adjust white padding space around plot
plt.tight_layout()

# usually must invoke show() if you not coding on jupyter notebook
# plt.show()

# to save all plots to a desired location & name, pass in full path
# useful for saving images in the background when you are mass producing plots
# plt.savefig("full_path_here")


#%% Lesson 2
from matplotlib import pyplot as plt
import numpy as np

plt.style.use("fivethirtyeight")

ages_x = [25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35]

# arange creates a list from 0.... to the integer you key into the argument
# we need to use this to replace the x-axis in order to fit all 3 bar graphs side by side as we +, - 
# off and on this list of values using the 'width' value
x_index = np.arange(len(ages_x))

# to shift the entire graph to the left or right of the x-axis/y - axis depending on how much you
# add or substract from the respective axis in the argument. It affects EVERY element in the list
width = 0.25


dev_y = [38496, 42000, 46752, 49320, 53200,
         56000, 62316, 64928, 67317, 68748, 73752]
plt.bar(x_index, dev_y, color="#444444", label="All Devs", width = width) # the label argument is for the legend box only

py_dev_y = [45372, 48876, 53850, 57287, 63016,
            65998, 70003, 70000, 71496, 75370, 83640]
plt.bar(x_index - width, py_dev_y, color="#008fd5", label="Python", width = width)

js_dev_y = [37810, 43515, 46823, 49293, 53437,
            56373, 62375, 66674, 68745, 68746, 74583]
plt.bar(x_index + width, js_dev_y, color="#e5ae38", label="JavaScript", width = width)

plt.legend()

# to OVERWRITE the x-axis to STILL put the bars side by side WHILE displaying the correct x-axis that we want
# to show our data labels. ticks = can be thought of as the raw numbers on the x-axis. labels = can be thought
# of as the cover over these numbers
plt.xticks(ticks = x_index, labels = ages_x)

plt.title("Median Salary (USD) by Age")
plt.xlabel("Ages")
plt.ylabel("Median Salary (USD)")

plt.tight_layout()

plt.show()


#%% Lesson 2 part 2
from matplotlib import pyplot as plt
import numpy as np
import csv
from collections import Counter
import pandas as pd

plt.style.use("fivethirtyeight")  

PATH = r"C:\Users\E707562\OneDrive - EDP\Desktop"
fn = r"\eda_tut_2 practise_data.csv"

with open(PATH + fn) as f:
    csv_reader = csv.DictReader(f)

    # create counter object
    counter = Counter()

    # looping over ea row in the entire csv file, ea row have 2 key value sets. Responder_id: ... & LanguagesWorkedWith: ...
    for row in csv_reader:

        # considering only values in LanguagesWorkedWith key. Returns
        counter.update(row["LanguagesWorkedWith"].split(";")) # output: 'JavaScript': 59219, 'HTML': ...

        # create empty lists
        language = []
        num_of_lan = []

        # parsing through the top 15 most counted VALUE
        for item in counter.most_common(15):
            language.append(item[0])
            num_of_lan.append(item[1])

# reverseing the order of the langauages because coincidently it shows a linear trend upward and we want the
# longest bar on top. Inplace command code
language.reverse() # x-axis
# dont forget to reverse x-axis' respective values as well
num_of_lan.reverse()


# since the x-axis names cluttered af, we use the y axis as x-axis
plt.barh(language, num_of_lan, color = 'r', label = "LanguagesWorkedWith Column Data Only")
plt.xlabel("Count")
plt.ylabel("Language")


#%% Lesson 6 Histograms
import pandas as pd
from matplotlib import pyplot as plt

PATH = r"C:\Users\E707562\OneDrive - EDP\Desktop"
fn = r"\eda_tut_6_practise_data.csv"

plt.style.use('fivethirtyeight')

# tutorial test file
rida_f = pd.read_csv(PATH+fn)
res_id = rida_f["Responder_id"]
res_age = rida_f["Age"] # [5,60,4,66,60,5...]

# bins argument in hist() can take in list data. number of elements in list == number of bins
# raw data will be categorized roughly into this "ranges". "ranges" are bins. If you delete a bin. The ranges of data
# within that bin will not be plotted at ALL. its like it was deleted from raw data
bins = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

# for histograms, it can aggregate, collate data into a "bin". How they group tgt is in the source code
# edge colour to distinguish bins from ea other
# histogram and bar graph is different! bar graph == at this x-axis data, what is its corresponding y-axis valu
# for histogram, from this RANGE of x-axis data, these range has this corresponding y-axis value 
# for hist, we only take in x-axis data, the y-axis data is the NUMBER fo times this x-axis value is REPEATED
# in the data set.
# for histogram, there exist a logarithmithic argument for the y-axis values
plt.hist(res_age, bins = bins, edgecolor = "black", log = True)

# to plot a median line. axis_vertical_line() function also has label arguement for a legend box
# decrease linewidth thiccness if too thick
median_age = 29
plt.axvline(median_age, color = 'r', label = "median_line", linewidth = 2)

plt.legend()

plt.title('Ages of Respondents')
plt.xlabel('Ages')
plt.ylabel('Total Respondents')

plt.tight_layout()

plt.show()

# creating a counter object. The arguments are the VALUES we GIVE the counter to count. THE data itself
# # counter reads off whatever is given to its argument
# c = Counter(["Python", "JavaScript"])
# c.update(["C++", "Python"])

#%% Lesson 3 Scatter Plots
import pandas as pd
from matplotlib import pyplot as plt

plt.style.use("seaborn")

x = [5, 7, 8, 5, 6, 7, 9, 2, 3, 4, 4, 4, 2, 6, 3, 6, 8, 6, 4, 1]
y = [7, 4, 3, 9, 1, 3, 2, 5, 2, 4, 8, 7, 1, 6, 4, 9, 7, 7, 5, 1]

color_for_each_coordinate = [7, 5, 9, 7, 5, 7, 2, 5, 3, 7, 1, 2, 8, 1, 9, 2, 5, 6, 7, 5]

size_for_ea_coordinate = [209, 486, 381, 255, 191, 315, 185, 228, 174,
         538, 239, 394, 399, 153, 273, 293, 436, 501, 397, 539]

# to set size s = 100, colour = c "green", can use a list of colours also for diff intensity of cols, 
# 'c' & 'cmap' arguments goes hand in hand. They come as a pair. 'cmap' (takes over the role of 'c')
# and gives 'c' colours instead because now 'c' holds a list[] instead of a color variable already
# change dots into cross = 'marker' = None means default circle
# 'alpha' means fade.'s' can take in a list to adjust size for ea data point.

# we only use cmap if we have a stupid list of numbers corresponding to each coordinate value
# which is likely the case when using other columns (dependant) to color code our 
# coordinates (2 other columns) of interest

plt.scatter(x, y, s = size_for_ea_coordinate, c = color_for_each_coordinate, cmap = "Greens", marker = None, edgecolor = "black", linewidth = 1, 
            alpha = 0.75)

# giving "color_for_each_coordinate"[] a legend to give readers a sense of what the intensity of the hue
# means
colorbar_legend = plt.colorbar()
# giving this legend a title
colorbar_legend.set_label("lululemon")


# %% Lesson 3 scatter plot part 2
import pandas as pd
from matplotlib import pyplot as plt

plt.style.use("seaborn")

PATH = r"C:\Users\E707562\OneDrive - EDP\Desktop"
fn = r"\eda_tut_7_practise_data.csv"

df = pd.read_csv(PATH+fn)
view_count = df["view_count"]
likes = df["likes"]
ratio = df["ratio"] # the like and dislike ratio

# very impt! by using c = ratio, we get to see the colour intensity. Lower ratios
# we colour it fader ('alpha' parameter very important), higher ratios = darker 
plt.scatter(view_count, likes, linewidth = 1, c = ratio, cmap = "summer", edgecolor = "black", alpha = 0.75)

# unlike histogram, scatter() function does not have a log argument. thus we use:
plt.xscale('log')
plt.yscale('log')

plt.xlabel("viewcount")
plt.ylabel("likes")

# if scatter() plot doesnt have any 'c' arguement paired with 'alpha' argument 
# nor colorbar() will display 0-1 default range
cbar = plt.colorbar()

# in order to label your colorbar you have to assign it to a variable
cbar.set_label("Like_Dislike_Ratio")

#%% Lesson 10 (Lesson 8 and 9 i skip ba)
import pandas as pd
from matplotlib import pyplot as plt

# if we use: from matplotlib import pyplot as plt, we are using stateful
# method, where the figure (the white square) is assumed to consist only of 1 axes. 1 x-axis and 
# 1 y-axis. A figure can consist of multiple axes (plots). So the power of subplots()
# allows me to repalce all .plt code with ax. but you have to include 'set_' behind these
# few functions E.g style(), tight_layout(), xlabel(), ylabel(), title().
# Except: .show() & .legend()

fig, ax = plt.subplots()

plt.style.use("seaborn") # the style cannot use 'ax'

PATH = r"C:\Users\E707562\OneDrive - EDP\Desktop"
fn = r"\eda_tut_10_practise_data.csv"

df = pd.read_csv(PATH+fn)
age_salaries = df["Age"]
all_dev_salaries = df["All_Devs"]
python_salaries = df["Python"]
jv_script_salaries = df["JavaScript"]

ax.plot(age_salaries, all_dev_salaries, color = "r", linestyle = '--', label = 'All Devs')
ax.plot(age_salaries, python_salaries, color = "m", linestyle = None, label = 'Python')
ax.plot(age_salaries, jv_script_salaries, color = "c", linestyle = None, label = 'JavaScript')

ax.legend()

ax.set_title("Median Salary (USD) by Age")
ax.set_xlabel("Age")
ax.set_ylabel("Median Salary (USD)")

plt.legend()

plt.show()


# %% Lesson 10 part 2
import pandas as pd
from matplotlib import pyplot as plt

# number of mini plots in 1 set ox xy axis
# sharex, removes the x-axis tick marsk on the top most graph to look neater
# fig, (ax1, ax2) = plt.subplots(nrows = 2, ncols = 1, sharex = True)

# to plot 2 different figures separately
fig1, ax1 = plt.subplots()
fig2, ax2 = plt.subplots()

plt.style.use("seaborn") # .style() cannot use 'ax'

PATH = r"C:\Users\E707562\OneDrive - EDP\Desktop"
fn = r"\eda_tut_10_practise_data.csv"

df = pd.read_csv(PATH+fn)
age_salaries = df["Age"]
all_dev_salaries = df["All_Devs"]
python_salaries = df["Python"]
jv_script_salaries = df["JavaScript"]

ax1.plot(age_salaries, all_dev_salaries, color = "r", linestyle = '--', label = 'All Devs')

ax2.plot(age_salaries, python_salaries, color = "m", linestyle = None, label = 'Python')
ax2.plot(age_salaries, jv_script_salaries, color = "c", linestyle = None, label = 'JavaScript')

# ancilliary details for the plot
ax1.legend()
ax1.set_title("Median Salary (USD) by Age")
# ax1.set_xlabel("Age")
ax1.set_ylabel("Median Salary (USD)")

# ancilliary details for the plot
ax2.legend()
# ax2.set_title("Median Salary (USD) by Age")
ax2.set_xlabel("Age")
ax2.set_ylabel("Median Salary (USD)")

# %%
import pandas as pd
import numpy as np
import matplotlib as plt
from sklearn import linear_model
from mpl_toolkits.mplot3d import Axes3D

df = pd.read_csv(r"C:\Users\E707562\Downloads\unconv_MV_v5.csv")
cols = df[['Por', 'Brittle']].values # make into 2D array instead of dataframe
X = cols[:, 0] # 'Por' data
Y = cols[:, 1] # 'Brittle' data
Z = df['Prod'] # 'Prod' data

X_min = round(X.min())
X_max = round(X.max())
# prepping x data values?
x_pred = np.linspace(X_min, X_max, 30) # start, end , number of points (inclusive)

Y_min = round(Y.min())
Y_max = round(Y.max())
# prepping y data values?
y_pred = np.linspace(Y_min, Y_max, 30)

# combining the 30 values in xx's 1D array tgt with 30 values in yy's 1D array to form coordinates in the meshgrid
# xx has (30,30) shape and yy has (30, 30) shape
xx, yy = np.meshgrid(x_pred, y_pred)

# flatten all (30 x 30) matrices. Resulting in 1D array, making a 2D array again using xx's 1D values and yy's 1D values
# have to transpose because there are more columns than there are rows
model_viz = np.array([xx.flatten(), yy.flatten()]).T

# Creating the model AKA: Training the model. We want a plane. That's why we use linear. ols stands for ordinary linear regression
# creating an instance of the regression tool thingy
ols = linear_model.LinearRegression() 
# obtaining the model itself. Note: we use the raw data columns itself here. idk why
model = ols.fit(X, Y)
# 
predicted = model.predict(model_viz)



print("")