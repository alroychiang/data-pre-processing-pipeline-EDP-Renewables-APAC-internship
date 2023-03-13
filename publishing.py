import json
import pandas as pd
import os
import re
import datetime
import glob
import csv
import numpy as np

from transformation import transformation
from metageneration import create_meta

PRINT = False

# opening json file
js = open(r"C:\Users\E707562\WorkSpace\project\config.json", "r")
configdata = json.loads(js.read())
js.close()



def dd_mm_yyyy(df):
    if PRINT: print("changing to dd_mm_YYYY format ")
    # standardize entire datetime format column
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], yearfirst = True, errors = 'raise')
    # df["TIMESTAMP"] is a pandas.core.series.Series datatype. But df["TIMESTAMP"][0] individual elements in the column is timestamp datatype
    df['TIMESTAMP'] = df['TIMESTAMP'].dt.strftime('%d-%m-%Y %H:%M:%S')

    return df



# takes into account the postal code and the load when appending files together
def monthly_concat(cleaned_filename, df, postal_code, year, fn):  
    # appending common monthly data into  single month's worth of data
    try:
        # searching the respective postal code folder -> year
        # if file name the exact same as any existing file name, append df
        # take postal code and the year and create the directory to search for the matching file name
        search_dir = configdata["CLEAN"] + "\\" + postal_code + "\\" + year
        check = False
        for match in glob.iglob(search_dir + "\\*.csv"): 
            if cleaned_filename == match:
                exist_df = pd.read_csv(match, sep = ",", header = 0, index_col = 0) # getting the existing dataframe
                # exist_df = dd_mm_yyyy(exist_df)
                cols_keep = list(configdata["COL_NAMES"].values())
                # to prevent the TIMESTAMP column from being dropped
                cols_keep.append("TIMESTAMP")
                # removing any redundant columns (E.g TIMESTAMP, A_diff() columns) leaving the original df's columns
                exist_df = exist_df.drop(exist_df.columns.difference(cols_keep), axis = 1)
                
                # have to convert the exist_df to standard dt format first.
                exist_df['TIMESTAMP'] = pd.to_datetime(exist_df['TIMESTAMP'], format='%d-%m-%Y %H:%M:%S')

                df = pd.concat([df, exist_df]) # appending the new dataframe to existing dataframe
                df = df.drop_duplicates(subset = "TIMESTAMP", keep = 'last') # if theres any EXACT date-time 
                df = df.sort_values(by='TIMESTAMP')
                df.reset_index(inplace=True, drop=True)
                check = True
                return df, check
            else:
                continue
        return df, check
    except (FileNotFoundError, PermissionError, OSError, IOError, ValueError) as e:
        if PRINT: print("The file: %s cannot be merged to an existing file due to %e" %fn %e)
    
    

def rearrange_metadf(metadf, metadf_dir):
    temp_df = metadf.copy()
    filename = "Filename                " + temp_df.loc["Filename:"][0]
    address = "Postal,Address         " + temp_df.loc["Address:"][0]
    start_time = "Start Time              " + str(temp_df.loc["Start Time", "TIMESTAMP"])
    end_time = "End Time                " + str(temp_df.loc["End Time", "TIMESTAMP"])
    time_period = "Time Period             " + str(temp_df.loc["Time Period", "TIMESTAMP"])
    time_resolution = "Time Resolution         " + str(temp_df.loc["Time Interval", "TIMESTAMP"])
    num_of_row = "Num of row              " + str(int(temp_df.loc["Num Of Rows(count)"][-1]))
    min_time_gap = "Minimum Time Gap        " + str(temp_df.loc["Minimum Time Gap", "TIMESTAMP"])
    median_time_gap = "Median Time Gap         " + str(temp_df.loc["Median Time Gap", "TIMESTAMP"])
    mean_time_gap = "Mean Time Gap           " + str(temp_df.loc["Average Time Gap", "TIMESTAMP"])
    max_time_gap = "Maximum Time Gap        " + str(temp_df.loc["Maximum Time Gap", "TIMESTAMP"])

    # making a list for these data
    list1 = [filename, address, start_time, end_time, time_period, time_resolution, num_of_row,
             min_time_gap, median_time_gap, mean_time_gap, max_time_gap]

    # writing all these data into csv file
    with open(metadf_dir, 'a', newline = "") as csvfile:
        writer = csv.writer(csvfile)
        for i in list1:
            writer.writerow([i])
        # metadf.to_csv(metadf_dir, mode = 'a', header=True)

    # selecting rows off the dataframe
    miss_val_df = metadf.loc["Missing Values(count)"]
    miss_val_df = pd.DataFrame(miss_val_df).transpose().astype(int)
    
    # dropping the 'timestamp' column off the row to round all remaining numerals in other columns to the nearest whole number
    # getting the 
    count_df = metadf.loc["Outliers(count)"]
    count_df = pd.DataFrame(count_df).transpose()

    # getting the remaining df rows
    desc_df = metadf.loc[["mean", "std", "min", "25%", "50%", "75%", "max"]]

    
    # concatenating ALL of the different rows in our desired order before writing it to csv file
    f_df = pd.concat([miss_val_df, count_df, desc_df], ignore_index=False)
    f_df = f_df.to_string(index=True)
    
    # writing f_df to the output .txt file
    with open(metadf_dir, 'a', newline = "") as csvfile:
        csvfile.write('\n')
        csvfile.write(f_df)



def publish(df, metadf, postal_code, load, f): # if any of the arguments here has any error

    # try:
        # use DataFrame's TIMESTAMP to extract year and month instead of the file name
        # Obtaining the Month
        date_obj = df["TIMESTAMP"].iloc[0]
        month = "{:02d}".format(date_obj.month) #returns an integer that is always 2 characters wide

        # Obtaining the year
        year = str(date_obj.year)

        # creating 'postal' folder E.g 825566 if the postal folder does not exist initially making each file's respective postal code's folder 
        pos_path = os.path.join(configdata["CLEAN"], postal_code)
        # won't overwrite folder if repeated folder created.
        os.makedirs(pos_path, exist_ok = True)
        # won't created a duplicate folder
        if PRINT: print("\n%s postal code folder has been created in 'Cleaned'\n" %postal_code)

        
        # making each file's respective year's folder
        yr_path = os.path.join(pos_path, year)
        os.makedirs(yr_path, exist_ok = True) # exist_ok = True means if directory exists, leaves it unaltered
        if PRINT: print("%s year folder has been created in %s" %(year, postal_code))

        
        # getting the final address filename directory
        cpy = configdata["CLEAN"] + '\\' + postal_code + '\\' + year + "\\"
        fn = postal_code + "_" + year + "_" + month + "_" + load + ".csv"
        cleaned_filename = cpy + fn


        # if the current file merges with an existing file, we have to regenerate the metadata.txt for that file
        df, check = monthly_concat(cleaned_filename, df, postal_code, year, fn)

        if check == True:
            s = f.split('.')[0].split('\\')[-1]
            print(f"\nThe current raw data file: {s}, has merged with an existing data file due to their identical address, load and month." 
                  "Please re-confirm the datatypes with their respective columns again as the program needs to regenerate the metadata.txt file.")
            _, metadf, _, _ = create_meta(df, f)
       
        # Export the meta data to a text file
        metadf_dir = cpy + postal_code + "_" + year + "_" + month + "_" + load + ".txt"
       
       # create the metadata.txt file and include the df.info() in it. 
        with open(metadf_dir, 'w') as f:
            df.info(buf = f, show_counts = True)
            f.write('\n')
        
        # adding log() and time_diff() columns into the df right before publishingggggggggg
        df = transformation(df)

        # changing datetime column into dd-mm-yyyy 24:00:00 format
        # see transformation of transf.py for explanation for why this func is here
        df = dd_mm_yyyy(df)

        # writing df to cleaned directory
        # won't create any intermittent non-existent directories in the path to the file. The year/postal code above will make the dir()s
        df.to_csv(cleaned_filename, sep = ',')

        # to include postalcode_YYYY_MM_load into the metadata itself
        incl = postal_code + "_" + year + "_" + month + "_" + load
        
        incl_df = pd.DataFrame({
            df.columns[0] : [incl]
            }, index = ["Filename:"])
        metadf = pd.concat([metadf, incl_df])

        # replace all NaN with empty string
        metadf = metadf.fillna('')

        rearrange_metadf(metadf, metadf_dir)
        
        return True
    
    # except Exception as e:
    #     print(f"\nThe error occurred is: {e}\n")
    #     return False



# try to concatenate the files via address and load name ONLY. 
# concatenate sequence using TIMESTAMP continuation flow

# file_names = []
# for i in glob.iglob(r"C:\Users\E707562\WorkSpace\project\staging\*"):
#     if re.search(r"\(.*\).csv$", i):
#         # print(type(i)) # string
#         file_names.append(i)

# # print(file_names) # A list consisting of ALL segemented files

# # the keys & values in this dictionary is as follows
# # Agg: ....Agg1, ...Agg2, ...Agg3 etc
# grouped_files = {}
# for f in file_names:
#     pattern = " \([0-9]+\)" # removes " (1)" from the string
#     key = re.sub(pattern, "", f.split('.')[0])
    
#     # if key match with file name
#     if key in grouped_files:
        
#         # add file name to key in dictionary
#         grouped_files[key].append(f)

#     else:
        
#         # if the key doesn't have any values create a new list
#         # and add the 1st file name to it
#         grouped_files[key] = [f] # a list is created for each key

# print(grouped_files) # A dictionary consisting of { values : filename(S) }


# someFile = {
#     "bobby": "alpha",
#     "age": 28,
#     "doodle": 8999
# }
# key = "bobby"
# f = "long string"

# print(someFile.append(f)) # i know whyyyyyyyy. BECAUSE the datatype
# # of the key "bobby" is a string. You only can ADD strings into the value. One huge string
# # only if you change the datatype into a list then can you add another 'string' to the key 
# # value. {'bobby': ['alpha', 'long string']}

# def separateNumbersAlphabets(str):
#     if SKELE: print("separating numbers from alphabets")
#     numbers = re.findall(r'[0-9]+', str)
#     alphabets = re.findall(r'[a-zA-Z]+', str)
#     return numbers[0], aphabets[0]

# getting the year month off a STRING timestamp
# date_obj = datetime.datetime.strptime(date_str, '%d-%m-%Y %H:%M:%S')