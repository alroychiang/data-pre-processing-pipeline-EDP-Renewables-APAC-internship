#%%
import pandas as pd
import matplotlib.pyplot as plt
import math
import numpy as np
# import seaborn as sns
# import missingno as msno
import os
import glob
from matplotlib.colors import Normalize
# plt.ion()

SAVEFIG = False
SET_INCHES = False

# PROJECT = ["Tuas Checkpoint - A4", "Tuas Checkpoint - A5", "Tuas Checkpoint - A6", 
#            "Tuas Checkpoint - A11", "Tuas Checkpoint - A12", "Tuas Checkpoint - A14", 
#            "Tuas Checkpoint - A22", "Tuas Checkpoint - A23", "Tuas Checkpoint - A24", 
#            "Tuas Checkpoint - A31","Tuas Checkpoint - A32", "Tuas Checkpoint - A33", 
#            "Tuas Checkpoint - D4", "Tuas Checkpoint - D12", "Tuas Checkpoint - D13", 
#            "Tuas Checkpoint - D21", "Tuas Checkpoint - D23", "Tuas Checkpoint - D24"] 

# PROJECT = ["Tuas Checkpoint - D13", 
#            "Tuas Checkpoint - D21", "Tuas Checkpoint - D23", "Tuas Checkpoint - D24"]

PROJECT = ["Tuas Checkpoint - A5"] # this line of code use to plot Live Plots for analysis

DATA_PATH = "C:/Users/E707562/WorkSpace/project/eda/EDA todo/explore_site_2/"
TIMESTAMP = "Time"


def read_data(filename):
    print(f"reading {DATA_PATH+filename}...")
    df = pd.read_csv(DATA_PATH+filename)
    
    df[TIMESTAMP] = pd.to_datetime(df[TIMESTAMP])
    # df.set_index(TIMESTAMP, inplace=True)
    
    return df

# truncate a float value without rounding. 
def truncate(f, n): # f == float number you key in, n == number of decimal places (strictly does not consider the whole number)
    if math.isnan(f):
        return np.NaN
    return math.floor(f*10**n) / 10**n

#%% plot
def plot_subplots(df, keys, f_f, title):
    print("subplots...")

    # if [keys] only consist of "inverter" & 'time', means the rest of the columns empty. break out of plot_subplots(). return nothing.
    if len(keys) == 2:
        return

    df1 = df[keys] # consist of ALL plottable columns
    groups = df1.groupby('Inverter') # varying number of inverters

    if title == 'String Current (A)' or title == 'MPPT Voltage (V)' or title == 'Phase Voltage (V)' or title == "Phase Current (A)" or title == "Line Voltage (V)":

        num_of_plots = len(keys) - 2 # 2 for inverter column and time column

        # we create this subplot OUTSIDE of the for-loop because we need ALL inverters (all names/groups) to be plotted through
        # on the same axes BEFORE plt.show().
        # string current 1
        # many axes on 1 figure. Ea axe == one string current columns (E.g string current 8). This axe also consist of other inverter's [string current 8 data column]
        # many inverters in a single axe.
        fig1, ax1 = plt.subplots(num_of_plots, sharex=True, sharey=True) # sharey == true
        for name, group, in groups:

            for i in range(0, num_of_plots):
                ax1[i].plot(group[TIMESTAMP], group[keys[i+2]], label=f"{name} {keys[i+2]}")
                ax1[i].grid(visible=True)        
                # if i > num_of_plots - 2: # include last 2 string current columns into legend box ONLY
                ax1[i].legend(bbox_to_anchor=(0.08, 0.2), fontsize = 6)
                ax1[i].tick_params(axis='y', labelsize=7)
        
        # for the plot: "All inverter in the same Graph's plot". Need to wait till the for-loop finishes executing 
        # then save the final plot
        fig1.suptitle(title)
        if SET_INCHES: fig1.set_size_inches(25, 10)

        p = DATA_PATH + f"plots/{f_f}/{title}/"
        os.makedirs(p, exist_ok = True)

        if SAVEFIG: fig1.savefig(p + f'{title} 1.png', bbox_inches='tight') # {title} 1.png okay. because this plot will always be plotted no matter what. only 1.
        

        # single inverter plotted in the entire figure. Many axes in a figure (depending on number of columns). Ea axe consist of a column's data ONLY. Not multiple columns in a single axe.
        # plots ALL inverters on different figures (many figures produced)
        plotted_inverters = set() # for each inverter i plot, it gets added into this set()
        count = 2 # the first string current1.png has alrd been plotted. we always start from 2
        for name, group, in groups:
            if name not in plotted_inverters:
                # each time a new inverter encountered, we create a fresh subplot.
                fig1_1, ax1_1 = plt.subplots(num_of_plots, sharex = True, sharey = True)
                for i in range(0, num_of_plots):
                    ax1_1[i].plot(group[TIMESTAMP], group[keys[i+2]], label=f"{name} {keys[i+2]}", color = 'orange')
                    ax1_1[i].grid(visible=True)        
                    ax1_1[i].legend(bbox_to_anchor=(0.08, 0.2), fontsize = 6)
                    ax1_1[i].tick_params(axis='y', labelsize=7)

                # the figure plots/ plt.show() is OUTSIDE the for-loop. To allow all axes' in the single figure to finish plotting first before saving png
                fig1_1.suptitle(title)
                if SET_INCHES: fig1_1.set_size_inches(25, 10)

                p = DATA_PATH + f"plots/{f_f}/{title}/"
                os.makedirs(p, exist_ok = True)

                if SAVEFIG: fig1_1.savefig(p + f'{title} {count}.png', bbox_inches='tight')
                count += 1
                

                # string current slide 4 and 5 ++++++
                # plot all columns in a single figure, single axe. SINGLE inverter. Ea inverter has its own figure plotted.
                fig2, ax2 = plt.subplots()
                # we iterate thorugh all string current columns of the current inverter/dataframe
                for i in range(0, num_of_plots):
                    ax2.plot(group[TIMESTAMP], group[keys[i+2]], label=f"{name} {keys[i+2]}")
                    ax2.grid(visible=True)
                    ax2.legend()
                
                # place outside for-loop to show() figure AFTER all string currents have been plotted per inverter
                fig2.suptitle(title)
                if SET_INCHES: fig2.set_size_inches(25, 10)

                p = DATA_PATH + f"plots/{f_f}/{title}/"
                os.makedirs(p, exist_ok = True)
                if SAVEFIG: fig2.savefig(p + f'{title} {count}.png', bbox_inches='tight')
                count +=1

                plotted_inverters.add(name)


    # activates only for string deviation 8, 9, 10. DC input power 11, 12 and 13. and more
    # only produces 3 graphs
    if title == 'String Current Deviation (%)' or title == 'DC Input Power(kW)' or title == 'Active Power (kW)' or title == "Total Production (kWh)" or title == "Internal Temperature (°C)" or title == "Inverter Efficiency":
        count = 1
        fig3, ax3 = plt.subplots()

        for name, group, in groups:
            
            # string deviation plot of all inverters on a single axe.
            # string deviation 8, DC input power 11
            ax3.plot(group[TIMESTAMP], group[keys[2]], label=f"{name} {keys[2]}")
            ax3.grid(visible=True)
            ax3.legend(loc = 'lower right', fontsize = 5)

        fig3.suptitle(title)
        if SAVEFIG: fig3.set_size_inches(25, 10)

        p = DATA_PATH + f"plots/{f_f}/{title}/"
        os.makedirs(p, exist_ok = True)
        if SAVEFIG: fig3.savefig(p + f'{title} {count}.png', bbox_inches='tight')
        count +=1

        # plotting all individual inverters on single figures
        plotted_inverters = set()
        for name, group, in groups:
            if name not in plotted_inverters:
                
                fig3_1, ax3_1 = plt.subplots()

                # string deviation 9, DC input power 12. # string deviation 10, DC input power 13
                ax3_1.plot(group[TIMESTAMP], group[keys[2]], label=f"{name} {keys[2]}")
                ax3_1.grid(visible=True)
                ax3_1.legend(loc = 'lower right', fontsize = 5)

                fig3_1.suptitle(title)
                if SET_INCHES: fig3_1.set_size_inches(25, 10)

                p = DATA_PATH + f"plots/{f_f}/{title}/"
                os.makedirs(p, exist_ok = True)

                if SAVEFIG: fig3_1.savefig(p + f'{title} {count}.png', bbox_inches='tight')
                count += 1

            plotted_inverters.add(name)
        
        # plt.show()

# string current 6 and 7. Phase Voltage 21 and 24.
def corr(df, keys, f_f, title):
    print("corr...")
    df1 = df[keys]
    groups = df1.groupby('Inverter') # I1 & I2 +++++
    count = 1 # to save and name the figures so they do not overwrite one another
    for name, group in groups:

        # to prevent plotting of Inverter & Time collumns in correlation plot
        rem = ["Inverter", TIMESTAMP]
        group.drop(rem, axis = 1, inplace = True)
        group.dropna(axis = 0, inplace = True) # remove ENTIRE ROW that has NaN values. Resulting df is has all cells filled.

        fig, ax = plt.subplots()

        # Create a heatmap plot with annotations. Set the colorbar range from -1 to 1
        im = ax.matshow(group.corr(), vmin = -1, vmax = 1)

        # iterate thorugh the "matrix"/ dataframe's cell to annotate one by one
        for i in range(group.corr().shape[0]): # x-axis
            for j in range(group.corr().shape[1]): # y-axis

                # limit to 6 decimal places without rounding
                ax.annotate(truncate(group.corr().iloc[i,j], 6) , xy=(j, i), ha='center', va='center', fontsize = 'xx-small')


        ax.set_xticks(range(0, len(group.columns)))
        ax.set_xticklabels(group.columns, fontsize = 7)
        plt.gca().xaxis.tick_bottom()
        plt.xticks(rotation=45)
        ax.set_yticks(range(0, len(group.columns)))
        ax.set_yticklabels(group.columns, fontsize = 7)
        
        
        fig.colorbar(im) # setting the correlation plot itself as its colourbar
        fig.suptitle(f"{name}")

        p = DATA_PATH + f"plots/{f_f}/{title}/"
        os.makedirs(p, exist_ok = True)

        # each inverter is saved separ`ately. Ea correlation plot has all data columns but only focuses on 1 inverter at a time
        # saves many figures with ea figure consisting of a unique inverter only
        if SAVEFIG: fig.savefig(p + f'Corr {title} {count}.png', bbox_inches='tight')
        count += 1


# main ()
# iterate through each site in the list: PROJECT
for f_f in PROJECT:
    df = pd.DataFrame()
    files = glob.glob(DATA_PATH + f"{f_f}*.csv")

    for f in files:
        filename = f.split('\\')[-1]
        print(filename + " IS BEING REEEEEEEEEEEEEEEEEEEEEEEEEEEEAD")
        d = read_data(filename)
        df = pd.concat([df, d]) # keep appending dataframes into the emprt 'df' until entire Site is a single df

    df = df.reset_index() # cannot touch idk why will give different plot
    df.drop(columns=["index"], inplace = True)

    # iterates through the df columns. if an entire column is encountered to be null, it adds the column title to col. set [] around the entire length of the code to set it as a list
    n_col = [col for col in df.columns if df[col].isnull().all()]

    # get a list of plottable columns
    plot_list = [x for x in df.columns.values if x not in n_col]

    # main()
    keys = ["Inverter", TIMESTAMP] + [col for col in plot_list if 'String Current' in col] # 9 cols + inv + time
    keys.remove(' String Current Deviation(%)')
    plot_subplots(df, keys, f_f, title = "String Current (A)")
    # corr(df, keys, f_f, title = 'String Current (A)')

    # keys = ["Inverter", TIMESTAMP] + [col for col in plot_list if 'Deviation' in col] # 3 cols, 1 axes, 1 plot.
    # plot_subplots(df, keys, title = 'String Current Deviation (%)')

    # keys = ["Inverter", TIMESTAMP, " DC Input Power(kW)"]
    # plot_subplots(df, keys, title = "DC Input Power(kW)")

    # # (all empty in Tuas dataset)
    # keys = ["Inverter", TIMESTAMP] + [col for col in plot_list if 'MPPT Voltage' in col]
    # plot_subplots(df, keys, title = 'MPPT Voltage (V)')

    keys = ["Inverter", TIMESTAMP] + [col for col in plot_list if 'Phase Voltage' in col]
    # plot_subplots(df, keys, f_f, 'Phase Voltage (V)')
    # corr(df, keys, f_f, title = "Phase Voltage (V)")

    keys = ["Inverter", TIMESTAMP, " R Phase Current(A)", " S Phase Current(A)", " T Phase Current(A)"]
    # plot_subplots(df, keys, f_f, 'Phase Current (A)')
    # corr(df, keys, f_f, title = "Phase Current (A)")

    keys = ["Inverter", TIMESTAMP, " Line Voltage L1-L2(V)", " Line Voltage L2-L3(V)", " Line Voltage L3-L1(V)"] 
    # plot_subplots(df, keys, f_f, 'Line Voltage (V)')
    # corr(df, keys, f_f, title = "Line Voltage (V)")

    # keys = ["Inverter", TIMESTAMP, " Active Power(kW)"]
    # plot_subplots(df, keys, title = "Active Power (kW)")

    # keys = ["Inverter", TIMESTAMP, " Total Production Reading(NoUnit-kWh)"]
    # plot_subplots(df, keys, title = 'Total Production (kWh)')

    # keys = ["Inverter", TIMESTAMP, " Internal Temperature(°C)"]
    # plot_subplots(df, keys, title  = "Internal Temperature (°C)")

    # keys = ["Inverter", TIMESTAMP, " Inverter Efficiency"]
    # plot_subplots(df, keys, title = "Inverter Efficiency")

    plt.show()

# #%%
# keys = ["Inverter", TIMESTAMP] + [col for col in plot_list if 'MPPT Current' in col]
# plot_subplots(df, keys, 'MPPT Current (A)')

