import pandas as pd
import json
import numpy as np
import re
import csv
import sys
import os

TOGGLE_USER_PROMPT = True


# opening json file
js = open(r"C:\Users\E707562\WorkSpace\project\config.json", "r")
configdata = json.loads(js.read())
js.close()



def inputPostal(prompt):
    while True:
        uPos = input(prompt)
        if not re.match(r"^[0-9]{6}$", uPos):
            print("Invalid input! Please re-enter the 6 digit postal code.")
            continue
        break
    return uPos



def outliers(df_for_stats, metadf):
    
    # calculates the upper quantile (a single float value) for ea column
    # calculates the lower quantile (a single float value) for ea column
    # IQR is a series object that consist of a single float value for ea respective col
    IQR = df_for_stats.quantile(0.75) - df_for_stats.quantile(0.25)

    # create an empty dataframe (with our col headers) to store the count of values exceeding the IQR
    exceed_counts = pd.DataFrame(columns=df_for_stats.columns)
    exceed_counts.loc[0,:] = 0 # initialize all to 0

    # takes each row under each column in the df_for_stats and compare each value to the lower threshold OR upper threshold
    # masks this value as Boolean. then by using df_for_stats[] to encapsulate everything, it filters out False rows that do not 
    # meet either criteria. Lastly using len() function to count the total number of rows in this new df_for_stats[]
    for col in df_for_stats.columns:

        exceed_counts.loc[0,col] = len(df_for_stats[(df_for_stats[col] < df_for_stats.quantile(0.25)[col] - 1.5 * IQR[col]) |
                                      (df_for_stats[col] > df_for_stats.quantile(0.75)[col] + 1.5 * IQR[col])])

    exceed_counts.rename(index={0: "Outliers(count)"}, inplace=True)
    metadf = pd.concat([metadf, exceed_counts])

    return metadf



def time_gap_stats(df, metadf):
    # converting df['TIMESTAMP'] series format into datetime format
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])

    # finding time period of the TIMESTAMP column
    start_time = df['TIMESTAMP'].iloc[0]
    end_time = df['TIMESTAMP'].iloc[-1]

   # adding a row to metadf labelled: "Start Time"
    strt_thyme = pd.DataFrame(
        {
        "TIMESTAMP" : [start_time]
        }, 
        index = ["Start Time"])
    
    # adding a row to metadf labelled: "End Time"
    end_thyme = pd.DataFrame(
        {
        "TIMESTAMP" : [end_time]
        }, 
        index = ["End Time"])

    # concatenating time_df to metadf dataframe
    metadf = pd.concat([metadf, strt_thyme])
    metadf = pd.concat([metadf, end_thyme])
  
    timeDiff = end_time-start_time

    # creating timeDiff dataframe 1st before concatetenating to
    # main dataframe
    time_df = pd.DataFrame(
        {
        "TIMESTAMP" : [timeDiff]
        }, 
        index = ["Time Period"])

    # concatenating time_df to metadf dataframe
    metadf = pd.concat([metadf, time_df])

    # create a new column that is shifted down 1 row. 0th index row is NaT
    df['timestamp_shifted'] = df['TIMESTAMP'].shift(1) # left col - right
    
    # The column that isnt shifted minus column that is shifted
    # Producing a column called df['time_gap']
    df['time_gap'] = df['TIMESTAMP'].subtract(df['timestamp_shifted'])
    
    # finding time gap statistics
    # print("\nThis is the average time-gap value in the time-gap column:\n %s" %timeInt)
    smallest_gap = df['time_gap'].min()
    
    # to be located under "TIMESTAMP" column within 'metadf'
    # to append into 'metadf' dataframe
    time_Interval = pd.DataFrame(
        {
        "TIMESTAMP" : [smallest_gap]
        }, 
        index = ["Time Interval"])

    # concatenating time_Interval dataframe into metadf dataframe
    metadf = pd.concat([metadf, time_Interval])

    # finding the MinimumTimeGapValue of the df
    min_time_gap_val = df['time_gap'].min()

    # finding the MaximumTimeGapValue of the df
    max_time_gap_val = df['time_gap'].max()

    # Finding the MedianTimeGapValue of the df
    median_time_gap_val = df['time_gap'].iloc[np.argsort(df['time_gap']).iloc[[len(df['time_gap'])//2]]]
    median_time_gap_val = median_time_gap_val.iloc[0] # accessing the series' 0th index object

    # Finding the AvgTimeGapValue of the df
    avg_val = df['time_gap'].mean()
    # r_sec = round(avg_val.total_seconds(), 4)
    # avg_time_gap_val = pd.Timedelta(seconds = r_sec)
    # print("")

    # trying to add missing values & min, max, median time gap data
    # into 'metadf' dataframe
    # creating new dataframes to append into 'metadf'
    minTimedf = pd.DataFrame({
        "TIMESTAMP" : [min_time_gap_val]
    }, index = ["Minimum Time Gap"])

    medTimedf = pd.DataFrame({
        "TIMESTAMP" : [median_time_gap_val]
    }, index = ["Median Time Gap"])

    avgTimedf = pd.DataFrame({
        "TIMESTAMP" : [avg_val]
    }, index = ["Average Time Gap"])

    maxTimedf = pd.DataFrame({
        "TIMESTAMP" : [max_time_gap_val]
    }, index = ["Maximum Time Gap"])

    # appending these dataframes/ writing these avg, median, mean, max information into the metadf
    metadf = pd.concat([metadf, minTimedf])
    metadf = pd.concat([metadf, medTimedf])
    metadf = pd.concat([metadf, maxTimedf])
    metadf = pd.concat([metadf, avgTimedf])

    # removing timestamp_shifted, time_gap columns from the main dataframe 'df'
    droplist = ["timestamp_shifted", "time_gap"]
    df = df.drop(droplist, axis = 1)    

    return df, metadf



def new_address(add):

    existing_post = None
    
    # opening json file, closes automatically
    with open(r"C:\Users\E707562\WorkSpace\project\address.json", "r") as js:
        address_json = json.loads(js.read())

    # check if address exist in address.json file
    for key, values in address_json.items():
        # if the address already exist in addrress.json, we take note of the corresponding postal code
        if add in values:
            existing_post = key
            break
    
    # check if address does not exist in address.json file
    if existing_post == None:

        uPos = inputPostal(f"\nThe address {add} in your raw file is new, please provide it's 6 digit postal code: ")
        
        # if address_legend.csv file don't exist, create it (here is the first instance where address_legend.csv is invoked)
        if os.path.isfile(configdata["LEGEND"]) == False:
            with open(configdata["LEGEND"], "w") as empty_csv:
                pass
        
        # opening address_legend file
        with open(configdata["LEGEND"], 'r', encoding='UTF-8') as alf:
            
            # 
            if os.path.getsize(configdata["LEGEND"]) == 0: 
                str1 = uPos + ", " + "\"new entity, add postal address\""

            # 
            else:
                str1 = "\n" + uPos + ", " + "\"new entity, add postal address\""
            
            # check whether this postal code alrd exist in address_legend.csv file. If so, dont add in a duplicate line
            reader = csv.reader(alf)
            uPos_found = False
            for line in reader:
                if uPos in line:
                    print("\n%s is already stored in add_legend.csv" %uPos)
                    uPos_found = True
                    break
                else: 
                    continue
            
            # Only after the entire txt file is parsed, we key in the new postal code as a new line leaving the "postal address portion empty"
            if uPos_found == False:
                with open(configdata["LEGEND"], 'a', encoding='UTF-8') as alf2:
                    alf2.write(str1)

        # if user enters a postal code that alrd exist
        uPos_exist = None
        # Because this new address is just a variation of an existing address/postal code
        for key, values in address_json.items():
            if uPos in key:
                uPos_exist = True
        
        # append this new address (add) to it's respective list in address.json
        if uPos_exist:
            address_json[uPos].append(add)
        
        # if user enters a postal code that does not alrd exist AND the 'address' read from filename does not exist as well
        # we need to add a new key value-pair to json file. address_json[_new_postal_]
        else:
            address_json[uPos] = [add]
                
        # Write the updated dictionary back to the JSON file
        with open(configdata["ADDRESS.JSON"], 'w') as js:
            json.dump(address_json, js, indent = 2)
    
        # we export the user input postal to use outside this function
        return uPos
    
    # the address in the file name alrd exist in address.json file. We just export the postal for usage outside this function
    else:
        return existing_post
    


def missing_val(df, f):
    
    # Handling strings in dataframe. This is User's fault. To prompt user for action and decision
    s = f.split('.')[0].split('\\')[-1]
    print(f"\nThe current data file {s} has the following non-values and datatypes for each respective column:\n")
    df.info(verbose = True, show_counts = True)

    if TOGGLE_USER_PROMPT:
        while True:    
            user_int = input(f"\nFor each column, there should be {len(df.index)} non-null values. "
                                "Do you want to proceed? "
                                "If No, the program will end and the file will remain in the \"staging\" folder. "
                                "If Yes, the program will proceeed to generate the missing values metadata from this information above (Y/N): ")
            if user_int.lower() == "y":
                break
            if user_int.lower() == "n":
                print(f"\nThe file {s} is not processed and remains in \"Staging\" folder")
                sys.exit()
            else:
                print("\nInvalid input. Please key in \"Y\" or \"N\"")
                continue
    
    # if user press 'y' the below code will execute
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    NaN_vals = df.isna().sum() #accounts for entire df for NaN values

    # this is the beginning of the creation of the metadf variable
    metadf = pd.DataFrame(NaN_vals, columns=["Missing Values(count)"])
    metadf = metadf.transpose()

    no_of_rows = df.shape[0]
    metadf.loc['Num Of Rows(count)', metadf.columns[-1]] = no_of_rows     

    return metadf



# main()
def create_meta(df, f): 
    

    metadf = missing_val(df, f)


    # drop the TIMESTAMP column in order to convert everything to numeric value, assign to a new 
    # dataframe called df_for_stats
    df_for_stats = df.drop(columns = "TIMESTAMP")
    
    # replacing all strings into np.nan
    df_for_stats = df_for_stats.apply(pd.to_numeric, errors='coerce')

    # adding the "TIMESTAMP" column back and overiting the original df. (now all column)
    # we'll be using this 'df' as the main df from now on
    df = pd.concat([df_for_stats, df["TIMESTAMP"]], axis = 1)


    # obtain number of outliers and append into metadf. Using columns only. No "TIMESTAMP" column
    metadf = outliers(df_for_stats, metadf)


    # this function needs a df with "TIMESTAMP" column of course
    df, metadf = time_gap_stats(df, metadf)


    # the df still has string in it!! we only alter the df_for_stats and changed its strings only
    # Use the describe() function to get summary statistics
    desc = df.describe(datetime_is_numeric=False).round(4)
    

    # append desc statistics to metadf
    metadf = pd.concat([metadf, desc])


    # Extracting address off the filename
    lis = f.split('\\')
    add = lis[-1].split('_')[0].lower()        


    # i put new_address() function here because this is the place where the address is alrd extracted off the filename
    postal_code = new_address(add)
    

    # extracts the load from file name. Anything after the final '_' in filename and converts all alphabets to lower case
    load = lis[-1].split('_')[-1].split('.')[0].lower()


    # reading address_legend.txt file. What if its empty?
    sr = open(configdata["LEGEND"], 'r', encoding = 'utf-8')
    lines = sr.readlines() # returns a list datatype


    # Using the postal code retrieved from new_address(). We then use
    # this postal code to extract the formal address in "address_legend.csv" to get its format
    # to append in metadata.txt. Matching the postal code in the address_legend.txt
    not_found = True
    for line in lines:
        al_pos = line.split(',')[0]

        if postal_code == al_pos: # if address_legend.txt is not updated with the new postal, its corresponding metadata.txt will not have address as well
            add_df = pd.DataFrame({
                # removing postal code from string E.g 467010 in "467010, "10 Chai Chee Rd, Singapore 467010"
                # selects everything after the 1st instance of a comma
                # assigns the entire address E.g "10 Chai Chee Rd, Singapore 467010" under 'I1' df.columns[0]
                df.columns[0] : line.split(",", 1)[1].replace("\n",'').strip()
                }, index = ["Address:"])


            # Adding postal code-address information in metadf dataframe
            metadf = pd.concat([metadf, add_df])
            metadf.loc['Address:'] = metadf.loc['Address:'].str.strip('"')
            not_found = False
            break

    if not_found == True:      
        # if parse through address_legend.txt and no matching postal code found.
        # if address.json HAS the postal code BUT address_legend.txt doesn't have the postal
        empty_add = pd.DataFrame({
            df.columns[0] : "new entity, add postal address"
        }, index = ["Address:"])

        # Adding empty postal code-address information in metadf dataframe
        metadf = pd.concat([metadf, empty_add])
        metadf.loc['Address:'] = metadf.loc['Address:'].str.strip('"')

    sr.close() #closing address_legend.txt when not in use
    

    return df, metadf, postal_code, load



# # matching address in filename to address in postal_address.txt file
#     for line in lines:
#         ea_line = line.replace(" ", "").replace("\n", "").strip() # E.g '467010,"10ChaiCheeRd,Singapore467010"'
#         if add in ea_line:
#             # making the string address into a dataframe datatype
#             post_addr = pd.DataFrame({
#                 # removing the front postal code from the data within address_legend
#                 # selects everything after the 1st instance of a comma and strips('') & '\n' from ends.
#                 # assigns the entire address E.g "10 Chai Chee Rd, Singapore 467010" under 'I1' df.columns[0]
#                 df.columns[0] : line[line.index(",")+1:].replace("\n",'').strip()
#                 }, index = ["Address: "])


# # testing my final metadf
# if DEBUG: 
#     with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#         print("\nThis is my final metadata dataframe within metageneration.py:\n%s" %metadf)


# # removing 'blk' word in string
# rem_blk = re.compile("^Blk", re.IGNORECASE)
# add = rem_blk.sub("", blk_add) # removes all possible combi of 'blk'


# getting the "load" string name off the file name. E.g Agg (1).csv, Agg1.csv, Agg(100).csv will only extract "Agg" term
# load = re.match(r"[a-zA-Z]+", lis[-1].split('_')[-1].split('.')[0]).group()


# export the postal code from string: '467010,"10ChaiCheeRd,Singapore467010"'
# file_postal_code = ea_line.split(',')[0]


# # taking the log_scale and time_differencing columns from the (transformed) df.........and adding to the old_df......
# unique_cols = df.columns.difference(df.columns)
# df = pd.concat([df, df[unique_cols]], axis = 1)


# removing any redundant columns that is not used for metageneration (E.g TIMESTAMP, A_diff()... columns)
# cols_keep = list(configdata["COL_NAMES"].values())

# a very good line of code to remove all columns from my dataframe that are within the list [cols_keep]
# df_for_stats = df.drop(df.columns.difference(cols_keep), axis = 1)



# # another way of replacing all strings into np.nan
# replace_str = lambda x: np.nan if isinstance(x, str) else x
# df_for_stats = df_for_stats.applymap(replace_str)

