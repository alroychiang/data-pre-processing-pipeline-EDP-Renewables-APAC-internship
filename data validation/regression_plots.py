#%%
import pandas as pd
import seaborn as sns
import missingno as msno
import numpy as np
import csv
import math
import itertools

from sklearn.linear_model import LinearRegression
from sklearn import linear_model

from matplotlib.font_manager import FontProperties
from matplotlib import gridspec
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mpl_toolkits.mplot3d import Axes3D
# plt.switch_backend('QtAgg4')

from skspatial.objects import Plane, Points
from skspatial.plotting import plot_3d



#%%
DATA_PATH = f"C:/Users/E707562/WorkSpace/project/eda/EDA todo/"
TIMESTAMP = "Date"

# ensures all float values in the dataframe have a maximum of 5 decimal points
# 'x' is the dummy variable that represents each value in the dataframe
# lambda function applies the function to every 'x' value. the function being 
# setting a float value to 5 d.p
pd.set_option('display.float_format', lambda x: '%.5f' % x)


fontP = FontProperties()
fontP.set_size('xx-small')


#%% read data
def read_data(filename):
    print(f"reading {DATA_PATH+filename}...")
    df = pd.read_excel(DATA_PATH+filename, nrows = 1000) # NUMBER of rows here              ----------------------------------
    
    # drop the row in df["Date"] with the value 'Statistics"
    df = df[df['Date'] != 'Statistics']
    df[TIMESTAMP] = pd.to_datetime(df[TIMESTAMP], format='%Y-%m-%d')
    # df.set_index(TIMESTAMP, inplace=True)
    
    return df


#%%
# global dataset
filename = "Site Report-2023_04_03_09_09_22_053.xlsx"

df = read_data(filename)
df.dropna(thresh=1, axis=1, inplace = True) # ONLY removes COLUMNS that consist of ALL null values.

df['Commissioning Date'] = df['Commissioning Date'].apply(pd.to_datetime)
df['Age (days)'] = (df['Date'] - df['Commissioning Date']).dt.days # extra column 'Age' appended to the dataframe

#%% 3D scatter (conditions BS implemented)
df1 = df[df['Capacity (MWp)'] < 2.5] # we remove all outliers (slide 30)
# df1 = df1.drop(df1[(df1['Capacity (MWp)'] < 0.75) & (df1['Site Production (kWh)'] > 5000)].index)
# df1 = df1.drop(df1[(df1['Capacity (MWp)'] < 0.4) & (df1['Site Production (kWh)'] > 2400)].index)


# groups = df1.groupby('Site')



#%% Failed. 3d scatter (slide 30). Plane located flat on the x-y plane.
#region
# plt.ion()
# fig6 = plt.figure()
# ax6 = fig6.add_subplot(111, projection = "3d")

# for site_name, df in groups:
#     ax6.scatter(df['POA Irradiation (Wh/m²)'], df['Capacity (MWp)'], df['Site Production (kWh)'], label = site_name)

# # have no choice but to remove those rows that consist of NaN values (even if only 1 cell consist of NaN)
# df2 = df1[['POA Irradiation (Wh/m²)','Capacity (MWp)']].dropna(axis = 0)
# # regression plane using site production data. '1' == degree of polynomial fit. '1' means linear. a plane instead of a saddle curve. we need this model to obtain predicted Z values that's NOT the column of z-values that we have at hand.
# plane_fit = np.polyfit(df2['POA Irradiation (Wh/m²)'], df2['Capacity (MWp)'], 1)
# # arange( start, end, step), just creating some frame? like a BBQ mesh? Can't run the entire global dataset. Too huge 39 Gigabytes
# xx, yy = np.meshgrid(np.arange(df1['POA Irradiation (Wh/m²)'].min(), df1['POA Irradiation (Wh/m²)'].max(), 0.1), 
#                      np.arange(df1['Capacity (MWp)'].min(), df1['Capacity (MWp)'].max(), 0.1)) # to perform operation on 2 arrays quickly in C code
# # getting z_predict values?
# zz = plane_fit[0]*xx + plane_fit[1]*yy # model[0] & model[1] are the gradient coefficient. Get the Z-predicted values.

# ax6.set_xlabel('POA Irradiation (Wh/m²)')
# ax6.set_ylabel('Capacity (MWp)')
# ax6.set_zlabel('Site Production (kWh)')
# ax6.plot_surface(xx, yy, zz, alpha = 0.5) # alpha makes the plane slightly transparent

# plt.show

# print("")
# # theoretical plane using theoretical production data
# df2 = df1[['POA Irradiation (Wh/m²)','Capacity (MWp)']].dropna(axis = 0)
# plane_fit = np.polyfit(df2['POA Irradiation (Wh/m²)'], df2['Capacity (MWp)'], 1)
# xx, yy = np.meshgrid(np.arange(df1['POA Irradiation (Wh/m²)'].min(), df1['POA Irradiation (Wh/m²)'].max(), 0.1), 
#                      np.arange(df1['Capacity (MWp)'].min(), df1['Capacity (MWp)'].max(), 0.1))
# zz = plane_fit[0]*xx + plane_fit[1]
# ax6.plot_surface(xx, yy, zz, alpha = 0.5)
# ax6.plot_surface(df['POA Irradiation (Wh/m²)'], df['Capacity (MWp)'], df['Theoretical Production (kWh)'])
# endregion


#%% Failed. Tried plotting the 3D regression plane. The plane is located top right
#region
# from skspatial.objects import Plane, Points
# from skspatial.plotting import plot_3d

# df_3D = df1[['POA Irradiation (Wh/m²)', 'Capacity (MWp)', 'Site Production (kWh)']].dropna(axis = 0)


# x_min = df_3D['POA Irradiation (Wh/m²)'].min() # 1499.0
# x_max = df_3D['POA Irradiation (Wh/m²)'].max() # 6873.0

# y_min = df_3D['Capacity (MWp)'].min() # 3.22371
# y_max = df_3D['Capacity (MWp)'].max() # 4.60701

# # z_min = df_3D['Site Production (kWh)'].min() # 1369.43
# # z_max = df_3D['Site Production (kWh)'].max() # 23907.26

# data_array = np.array(df_3D)

# # to put it into a list()
# p_list = data_array.tolist() # take all 3 columns and make into data points. [1,4,5]
# # to input into the Points() function
# points = Points(p_list)


# plane = Plane.best_fit(points)
# _, ax = plot_3d(points.plotter(c = 'b', s = 5, depthshade = True),
#         plane.plotter(alpha = 0.2, lims_y = (y_min, y_max), lims_x = (x_min, x_max))
#         )

# # ax.set_zlim([z_min, z_max])
# print("")

#endregion


#%% 3D regression plane. chatGPT using skspatial.objects.Plane.best_fit(points). Create a 3D figure and axes
#region
# plt.ion()
# fig = plt.figure()
# ax = fig.add_subplot(111, projection='3d')

# for site_name, frame in groups:
#     ax.scatter(frame['POA Irradiation (Wh/m²)'], frame['Capacity (MWp)'], frame['Site Production (kWh)'])

# ax.set_xlabel('POA Irradiation (Wh/m²)')
# ax.set_ylabel('Capacity (MWp)')
# ax.set_zlabel('Site Production (kWh)')

# # need to drop empty rows of df1[['POA Irradiation (Wh/m²)', 'Capacity (MWp)', 'Site Production (kWh)']] columns ONLY
# # why do we need to drop all empty rows? For training the model
# dropped_df1 = df1.dropna(subset=['POA Irradiation (Wh/m²)', 'Capacity (MWp)','Site Production (kWh)'])
# X = dropped_df1[['POA Irradiation (Wh/m²)', 'Capacity (MWp)']]
# Y = dropped_df1[['Site Production (kWh)']]

# # make ea column dataframe into an array. X.values returns a tuple. X.values.reshape(-1,2) returns a 2D array.
# # [[]] returns a dataframe. [] returns a series



# plt.show()

# print("")

# # Fit a plane to your 3D data
# # points = data[:, :3]  # select the x, y, z columns from your data
# plane = Plane.best_fit(ax)
# # points = Points(p_list)

# # Create a meshgrid for the plane surface
# # x_min, x_max = np.min(data[:,0]), np.max(data[:,0])
# # y_min, y_max = np.min(data[:,1]), np.max(data[:,1])

# x_min = df_3D['POA Irradiation (Wh/m²)'].min() # 1499.0
# x_max = df_3D['POA Irradiation (Wh/m²)'].max() # 6873.0

# y_min = df_3D['Capacity (MWp)'].min() # 3.22371
# y_max = df_3D['Capacity (MWp)'].max() # 4.60701

# xx, yy = np.meshgrid(np.linspace(x_min, x_max, 10),
#                      np.linspace(y_min, y_max, 10))

# # Evaluate the plane equation to get the z values for the surface
# zz = plane.evaluate(np.vstack([xx.ravel(), yy.ravel()]).T).reshape(xx.shape)

# # Plot the plane as a surface
# ax.plot_surface(xx, yy, zz, alpha=0.5)

# # Set the labels and title
# ax.set_xlabel('POA Irradiation (Wh/m²)')
# ax.set_ylabel('Capacity (MWp)')
# ax.set_zlabel('Site Production (kWh)')
# ax.set_title('To plot Regression and Theoretical Plane')
# fig.tight_layout()

# # Show the plot
# plt.show()

# print("")
# endregion


#%% for part 3, Plot the scatter points only. ORIGINAL. 
#region
# for site_name, df in groups:
#     ax6.scatter(df['POA Irradiation (Wh/m²)'], df['Capacity (MWp)'], df['Site Production (kWh)'])


# ax6.set_xlabel('POA Irradiation (Wh/m²)')
# ax6.set_ylabel('Capacity (MWp)')
# ax6.set_zlabel('Site Production (kWh)')
# ax6.set_title('Predicted Regression Plane')
# fig6.tight_layout()


# plt.show() # only use this line of code once at the end of ALL plots. Anything plots after this code wont show.

# print("")
# endregion


#%% slide 39. Scatter points that look like bar graphs.
#region
# plt.ion() # turning on interactive mode, to allow debugger to continue running and not freeze.

# # Part 1 identifying excluded sites
# fig2, ax2 = plt.subplots()
# fig3, ax3 = plt.subplots()

# # # using the minimum coordinate and the maximum coordinate i plot a linear line
# x_val = [0, 2.08548]
# y_val = [0, 11268.45]
# # # the line equation/ model. since we have (0,0) as one of our coordinates, c = 0.
# m = (y_val[1]-y_val[0])/ (x_val[1] - x_val[0])

# def model(x, m, buffer): # buffer is % value we are willing to accept E.g 10% buffer I make y-predict 90% of its original value
#     y_buffered = ( (m*x)/100 ) * (100 - buffer)
#     y_predict = m*x
#     return y_buffered, y_predict

# # Part 1, first graph, ax2 inclusive of outliers
# # for each site, find the largest y-value coordinate and compare with its corresponding y-predicted value
# # df["Capacity (MWp)"], is the x-coordinate(S)
# # df["Site Production (kWh)"], is the y-coordinate(S)
# # each iteration of the for loop consist of all data rows of a particular site E.g Tuas
# csv_info = []
# for site_name, df in groups: 

#     # remove all 2.5% top and bottom of ea df. Replot the graph
#     # sort the dataframe
#     tot_rows = len(df['Site Production (kWh)'].sort_values())
#     rem_row = round((tot_rows/100)*2.5)

#     # removing first and last few rows of the dataframe. Slicing the dataframe. df.head() represents which row index we want to START keeping the dataframe. df.tail() is the row index which we want to STOP keeping the dataframe.
#     # my_slice = slice(1, 4) # maybe can use these
#     # sliced_df = df.iloc[my_slice]
#     if rem_row != 0:
#         df = df.head(-rem_row).tail(-rem_row)

#     ax2.scatter(df['Capacity (MWp)'], df['Site Production (kWh)'], label = site_name)
#     # generate a list of predicted y-values from my Capacity (MWp) x- axis coordinates (all the same)
#     # y_buffered, y_predict = model(df['Capacity (MWp)'].iloc[0], m, 0)
#     # maximum y-value in the current site group, true y-value (site_production)
#     true_y = df['Site Production (kWh)'].max()
#     # taking this row index of the maximum site production, extract the row's columns below.
#     # mpd = df['Site Production (kWh)'].idxmax() # mpd IS the date, acts as an index ID as well


#     # any site with its maximum point that is even remotely BELOW the y_predicted line gets written to a csv file with its Date, Site_name & percentage distance from threshold line
#     if true_y < y_predict:
#         # ratio = math.log(true_y / df.loc[mpd, ['Capacity (MWp)']][0]) # i took site_prod divide by Capacity value
#         # ratio = true_y/y_predict # 0.99 ratio value means the true_y is extremely close to the predicted_y value
#         ratio = (true_y/y_predict)*100 # percentage of how close the true_y is from the predicted_y value. Excel sheet can filter according to percentage and identify sites interested.
#         csv_info.append([site_name, round(y_predict-true_y, 5), round(ratio, 5)])

#     # Plotting the comparison graph excluding ALL sites that do not meet the threshold line. As long as a single point is ABOVE the threshold line, we plot.
#     if true_y > y_predict:

#         ax3.scatter(df['Capacity (MWp)'], df['Site Production (kWh)'], label = site_name)

# # Plotting reduced scatter graph, excluding sites that did not meet the threshold. Threshold adjusted with percentage TOGGLE
# # REMOVE_PERCEN = 100 # 100% means ALL rows in the site MUST consist of 23:55:00 timestamp rows to be considered 'removed' from the plot & added to flagged_sites[] list
# # flagged_sites = []
# # for site_name, df in groups:

# #     try:
# #         t_f = df['Peak Power Moment'].str.contains('23:55:00').value_counts()
# #         count = t_f[True]
# #     except KeyError as e: # if all rows are 23:55:00 or all rows are NOT 23:55:00. All boolena mask True/False. Produces keyError
# #         count = 0

# #     percen = round((count/df['Peak Power Moment'].shape[0])*100)

# #     if percen <= REMOVE_PERCEN:
# #         ax3.scatter(df['Capacity (MWp)'], df['Site Production (kWh)'], label = site_name)
    
# #     # any site that contains too many rows of 23:55:00 ( >= REMOVE_PERCEN constant) gets added to a flagged_sites list
# #     if percen >= REMOVE_PERCEN:
# #         flagged_sites.append(site_name)


# # Displaying original scatter plot (bar graph like) full plots. No missing plots.
# ax2.plot(x_val, y_val, color = "r") # plotting the straight line (man made regression line) in the figure tgt with scatter plot
# ax2.grid()
# ax2.set_xlabel('Capacity (MWp)')
# ax2.set_ylabel('Site Production (kWh)')
# ax2.set_title("Full Scatter Plot")
# plt.tight_layout()
        
# # Displaying reduced scatter graph, excluding sites that did not meet the threshold.
# ax3.plot(x_val, y_val, color = "r") # adding in the regression line
# ax3.grid()
# ax3.set_xlabel('Capacity (MWp)')
# ax3.set_ylabel('Site Production (kWh)')
# ax3.set_title("Plot Excluding Under Performing Sites")
# plt.tight_layout()


# # this csv file has cols: site_name, y_true/y_predicted %ratio.
# file_location = r"C:\Users\E707562\WorkSpace\project\eda\EDA todo\31_03_2023 tasks\2\under_performing_sitenames_all_sites_trimmed_of_2.5%_Y_value_difference_%difference.csv"
# header = ["Site Names", "Y Value Difference", "% Y Value Difference"]
# with open(file_location, 'w', newline ='') as csvfile:
#     # Create a CSV writer object
#     writer = csv.writer(csvfile)
#     writer.writerow(header)
#     # Write each string to a new row in the CSV file
#     for obj in csv_info:
#         writer.writerow(obj)

# # fn = r"C:\Users\E707562\WorkSpace\project\eda\EDA todo\31_03_2023 tasks\235500_rows_thingy_rename_this_pls.csv"
# # with open(fn, 'w', newline ='') as csvfile:
# #     # Create a CSV writer object
# #     writer = csv.writer(csvfile)
# #     writer.writerow(["Sites that have 60% of its rows as 23:55:00"])
# #     # Write each string to a new row in the CSV file
# #     for s in flagged_sites:
# #         writer.writerow([s])

# plt.show()

# print("")
#endregion


#%% part 2, Plotting 2D plot of Age-Site_Yield & Age-Performance Ratio.
#region
# fig4, ax4 = plt.subplots()
# fig5, ax5 = plt.subplots()

# for site_name, df in groups:
#     ax4.scatter(df['Age (days)'], df["Site Yield (h)"], label = site_name, s = 20, edgecolors = "black", alpha = 0.75)
#     ax5.scatter(df['Age (days)'], df["Site PR"], label = site_name, s = 20, edgecolors = "black", alpha = 0.75)

# # Displaying ancilliary stuff, dont need plt.plot() because we not adding regression line to this graph(s)
# ax4.grid()
# ax4.set_xlabel('Age (days)')
# ax4.set_title('Site Yield (h)[generation(Wh)/system capacity(MWp)]')

# ax5.grid()
# ax5.set_xlabel('Age (days)')
# ax5.set_title('Site Performance Ratio [generation(kWh)/system capacity(MWp)/irradiation(Wh/m²)]', fontsize = 10)

# plt.show() # only use this line of code once at the end of ALL plots. Anything plots after this code wont show.

#endregion


#%% Average for all sites (site_PR y-axis) for a single day (x-axis)
#region
# fig6, ax6 = plt.subplots()

# group_site = df1.groupby('Site')

# r_df = []

# # to remove all 2.5% of ea site. Trying to elimate some outliers.
# for site_name, df_gs in group_site: 

#     # reset the index. We need the date column
#     df_gs.reset_index(inplace = True)

#     # remove all 2.5% top and bottom of ea df. Replot the graph
#     # sort the dataframe
#     tot_rows = len(df_gs["Site PR"].sort_values())
#     rem_row = round((tot_rows/100)*2.5)

#     # removing first and last few rows of the dataframe. Slicing the dataframe. df.head() represents which row index we want to START keeping the dataframe. df.tail() is the row index which we want to STOP keeping the dataframe.
#     # my_slice = slice(1, 4) # maybe can use these
#     # sliced_df = df.iloc[my_slice]
#     if rem_row != 0:
#         df_gs = df_gs.head(-rem_row).tail(-rem_row)

#     r_df.append(df_gs)

# reduced_df = pd.concat(r_df, ignore_index=True)

# group_date = reduced_df.groupby('Date')

# for Date, df_gd in group_date:

#     # taking the average of ALL site production values for ALL sites
#     mean = df_gd["Site PR"].mean()

#     # the x-axis per per point is a singale date.
#     ax6.scatter(Date, mean, label = Date, s = 20, alpha = 0.75)

# # Displaying ancilliary stuff, dont need plt.plot() because we not adding regression line to this graph(s)
# ax6.grid()
# ax6.set_xlabel('Date')
# ax6.set_title('Site Performance Ratio [generation(kWh)/system capacity(MWp)/irradiation(Wh/m²)]', fontsize = 10)
# plt.xticks(rotation=45)

# plt.show() # only use this line of code once at the end of ALL plots. Anything plots after this code wont show.

# print("")

#endregion


# #%% Imputation of Global Dataset's Accumulation Values
#region
# df1 = df.sort_values(['Site', 'Date'])

# df1['Site Production (kWh)'] = df1['Site Production (kWh)'].fillna(method='bfill')


# print("")

# # for the case where the ENTIRE site experience data loss ENTIRELY (no communication reconnect)
# # i take note of the site. No interpolation for this site
# no_ntwrk_reconnect = []



# df1[spr]

# for site_name, df in groups:

#     print(df[['Site', 'Date', spr]])
#     break

#endregion


#%% Plot Yield, Site Production both against date. After filtering 'Total Column Interruption Time(h)' == 0.00hrs.
# we dont focus on site_production plot because extremely hard to identify any outliers
#region
fig1, ax1 = plt.subplots()
# fig2, ax2 = plt.subplots()


# df_before = df1.copy()


# dropping all rows are not df['Total Communication Interruption Time\xa0(h)'] == 0
# df1 = df1[df1['Total Communication Interruption Time\xa0(h)'] == 0]
# using site_yield vs date we identified even more outliers and removed them to get iur clean dataset to plot all our other graphs
df1 = df1[df1['Site Yield (h)'] < 10] # using site_yield vs date we identified even more outliers and removed them to get iur clean dataset to plot all our other graphs
df1 = df1.drop(df1[df1['Site Yield (h)'] == 0].index)

# removing all zero plots (the build up plots) that are associated with the outlier plots we identified in the list, to plot yield vs date graph
# how do i get a list of all their build up data rows?? all site production values == 0 until the next value site prod value is encountered
# use the list i got (yield > 10) and their index value. (KIVVVVVV - bs)
# outlier_df = df1[df1['Site Yield (h)'] > 10] 


# proportion of dataset remaining after 'cleaning'
# p = (df1.shape[0]/df_before.shape[0])*100
# print(f"\nThe percentage of the dataset left is: {round(p, 1)}")

groups = df1.groupby('Site')

# regress line
x_val = [0, 2.085]
y_val = [0, 11269]
m = (y_val[1]-y_val[0])/ (x_val[1] - x_val[0])
def model(x, m):
    y_predict = m*x
    return y_predict

csv_info = []
for site_name, df in groups:
    y_predict = model(df['Capacity (MWp)'].iloc[0], m)
    true_y = df['Site Production (kWh)'].max()
    
    # extracting all sites below the regression line
    if true_y < y_predict:
        ratio = (true_y/y_predict)*100
        csv_info.append([site_name, round(y_predict-true_y, 5), round(ratio, 5)])

    ax1.scatter(df['Capacity (MWp)'], df['Site Production (kWh)'], label = site_name)
    # ax2.scatter(df['Date'], df['Site Yield (h)'], label = site_name, s = 20, edgecolors = "black", alpha = 0.75)


# sorting the csv_info lowest to highest. x[2] is the index position within the sublist, the element selected to sort
csv_info = sorted(csv_info, key=lambda x: int(x[2]))


# writing to csv file
file_location = r"C:\Users\E707562\WorkSpace\project\eda\EDA todo\total comms interruption time zero\05_04_2023 deliverables\under_performing_sites_regress_line_scatter_plot.csv"
header = ["Site Names", "Raw Y Value Difference", "% Y Value Difference (E.g How much % the raw Y value is OF the Y_predicted value)"]
with open(file_location, 'w', newline ='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)
    for obj in csv_info:
        writer.writerow(obj)


ax1.plot(x_val, y_val, color = "r") #regress line
ax1.set_xlabel('Capacity (MWp)')
ax1.tick_params(labelrotation=45)
ax1.set_ylabel('Site Production (kWh)')
# ax1.set_title('Total Comms Interruption Time(h) == 0')
ax1.grid(True)

fig1.tight_layout()


# ax2.set_xlabel('Date')
# ax2.tick_params(labelrotation=45)
# ax2.set_ylabel('Site Yield (h)')
# ax2.set_title('Total Comms Interruption Time(h) == 0')
# fig2.tight_layout()

plt.show()

print("")

#endregion


# OLD CODE
#region

# IQR for an entire dataframe. Doesnt work because number of rows (amount of data remains the same. I coulfnt find out why)
# i continued to try other methods.
# removing outliers from all data using IQR
# q1 = df2.quantile(0.25)
# q3 = df2.quantile(0.75)
# iqr = q3 - q1
# df2_cleaned = df2[(df2 >= q1 - 1.5*iqr) & (df2 <= q3 + 1.5*iqr)] # 48400 rows

# you dont fit ALL y-values, you fit only the top few coordinates. Doesnt work because theres outliers.
# model.fit(x, max_y) # the model

# flattening a nested list into a 1D list
# flagged_sites = list(itertools.chain(*csv_info))

# I have a dataframe with many cols. The certain columns there are NaN values. I want to selected all the remainging rows apart from these
# rows that have NaN columns.
# df1 = df1.dropna(subset = ['Peak Power Moment']) # remove all NaN to prevent '~' from being applied to a Str Error
# df1 = df1[~df1['Peak Power Moment'].str.contains('23:55:00')]

# to check for duplicates in a flat list
# len(flagged_sites) != len(set(flagged_sites))

# -------------------------------------------------------------------------------------------------------------
# ALL the code below is me trying to mdel a LinearRegression line. Realized the quickest way is to plot
# the equation myself.
# literally creating x-axis data, num == number of x-axis points, start.stop is the x-axis limit values
# we use this set of x-values in our model to produce our y-values
# x_new = np.linspace(start = 0, stop = 2.35, num = 100, endpoint = True)
# y_new = model.predict(x[:, np.newaxis])

# plotting the regression line in the same fig2
# ax2.plot(x_new, y_new)

# '-1' parameter means auto detect size of 1st dimension. declare 2nd dimension as 1
# just so i can fit in the model
# y = df2['Site Production (kWh)'].values.reshape(-1, 1)
# x = df2['Capacity (MWp)'].values.reshape(-1, 1)

# finding the maxium y-values for ea distinct x-unique category (the site name) in this case
# max_y = df2.groupby('Capacity (MWp)').max()['Site Production (kWh)'] # but this code takes in the outliers as well
# need to find corresponding x-values for these y-values


# old strategy that i thought might help me identify the under performing sites
# for each site_name, get its greatest coordinate value amongst all the rows!
# each for loop iteration, it processes an ENTIRE site. It plots ALL the site's coordinate.
# E.g Alpha_Site, Beta_site....Zulu_Site (no site is repeated due to the groupby function)

# creating the model
# model = LinearRegression()
# since scatter() alrd do not plot coordinates with NaN values in them, its automatically "removed" from the graph
# we remove coordinates (all rows) with NaN in them in order to obtain a regression line (model).
# df2 = df[['Site Production (kWh)', 'Capacity (MWp)']]
# df2.dropna(inplace = True) # 65660 rows before, 48400 rows after
# --------------------------------------------------------------------------------------------------------------

#endregion
