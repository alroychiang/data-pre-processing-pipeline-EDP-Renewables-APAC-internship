import pandas as pd
import json
import csv
from difflib import SequenceMatcher

PRINT = False

# opening json file
js = open(r"C:\Users\E707562\WorkSpace\project\config.json", "r")
configdata = json.loads(js.read())
js.close()



def read_data(f):
    
    # taking only the header row from the file
    with open(f, 'r') as file:
        reader = csv.reader(file, delimiter = "\t")
        headers = next(reader)
        del headers[0] # theres a weird empty space at 0th index idk why

    # identifying the index positions of those columns that duplicated
    # E.g if IRMS_A...(five columns later)..IRMS_A
    # E.g maybe in the dup cols = {'IRMS_A': [0,5,6]} where the integers are the index positions
    # of IRMS_A duplicated columns in the dataframe
    dup_dict = {}

    # loop through list, append index of any duplicated cols to its respective key value(column name itself)
    # else add a new key-value pair to dict
    # to group all indexes of duplicated columns together with their key values
    for i, col_key in enumerate(headers):
        if col_key in dup_dict:
            dup_dict[col_key].append(i)
        else:
            dup_dict[col_key] = [i]
            
    # reading in the entirety of the df as per normal (read_csv() will still change the column name)
    # but doesnt matter, we use index in dictionary to compare column duplicates
    df = pd.read_csv(f, sep = "\t", nrows = 100, header = 0, index_col = 0)

    # we throw away columns in this copied_df instead of the real df in order to
    # prevent indexing issues
    copied_df = df.copy()

    # using the dictionary, use the index and extract out the columns in the main df
    # compare w/ ea other (3 or more) and delete all but 1 left

    # how to access the 1st key of the dictionary and all its values ONLY
    # i MUST know the key's full name. I cannot access a dictionary via key's index as
    # they do not exist
    
    # get all the keys of the dictionary in a list.
    keys = list(dup_dict.keys())


    # accessing ea key value in the dictionary
    for k in keys:    
        
        anchor = dup_dict[k][0] # take the first column and use it as an anchor to compare with the rest of the duplicated columns

        values = dup_dict[k] # returns a list of values for that particular key.

        for i, v in enumerate(values):
        
            try:    
                next_col = values[i+1]
            except IndexError as ie:
                break # go to the next key in the dictionary. This sieves out those with only 1 key-value pair

            # the actual comparison of the columns themselves. Checking if
            # both column values match exactly
            if df.iloc[:, anchor].equals(df.iloc[:, next_col]):
                
                # delete the the subsequent column
                copied_df.drop(columns = df.columns[next_col], axis = 1, inplace = True)


    # before df = 20 cols. After df should have 17 cols remaining.
    # note k = "IRMS_A.1" does not .equals() even thoguh dict has 2 values in list[]
    # 2nd IRMS_A.1 has all values as NaN lmao added extra col lmao.
    # STILL have to test out. IF there is more than 2 column duplicate. 
    # create test file that has 3 columns duplicate or more. test if values[i+1] really works
    return copied_df



def time_gap_thresh(df, f):
    # convert string obj to timestamp obj
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    start_time = df.iloc[0]["TIMESTAMP"]
    end_time = df.iloc[-1]["TIMESTAMP"]
    timedelt = end_time - start_time
    theoretical_rows = timedelt.total_seconds()
    actual_rows = len(df["TIMESTAMP"])
    miss_rows = abs(actual_rows - theoretical_rows)
    if miss_rows >= configdata["TIMEGAP_THRESH"]:
        return True
    return False



# main()
def clean_data(df):

    # Standardizing column names
    df = df.rename(columns = configdata["COL_NAMES"])

    return df



# def duplicate_columns(df):

#     print("")
#     # identifying any identical/similar column headers
#     # getting a list of column names
#     cols = df.columns.tolist()

#     # defnining a similarity threshold. I pick 0.74 because of ratio()'s formula definition 
#     # for similarity. Calculated comparing IRMS_A & IRMS_A.1.1
#     threshold = 0.74
#     break_nested = 0

#     for i, col1 in enumerate(cols):
#         for j, col2 in enumerate(cols[i+1:]):
            
#             print("")
#             # will execute only if user deletes the current column (the comparator) the program
#             # the program is using to match subsequent columns for similarity
#             if break_nested == 1:
#                 break_nested = 0
#                 break
            
#             s = SequenceMatcher(None, col1, col2)
            
#             # if column headings are above similarity threshold and ALL values match between these two columns
#             while s.ratio() >= threshold and df[col1].equals(df[col2]):
#                 choice = input(f"\nThese two columns are similar:\n\n{df[col1]}\n\n{df[col2]}\n\n"
#                         "Do you want to delete a column? (Y/N) If \"N\" is selected, no columns will be deleted "
#                         "and the program will continue to check for any similar columns: ")
                
#                 if choice.lower() == 'y':
#                     del_choice = int(input(f"\nWhich column, 1.{col1} or 2.{col2}, do you want to delete? "
#                                     "Enter 3 to return to menu. (1/2/3): "))
                    
#                     if del_choice == 1:
#                         df.drop(columns =[col1], inplace = True)
#                         break_nested = 1
#                         break

#                     if del_choice == 2:
#                         df.drop(columns =[col2], inplace = True)
#                         # update cols[] list of dropped column as well
#                         cols.remove(col2)
#                         break

#                     if del_choice == 3:
#                         continue

#                     elif del_choice != 1 or 2 or 3:
#                         print("\nInvalid input. Returning to menu.")
#                         continue

#                 if choice.lower() =='n':
#                     break
    
#     return df


# def getDuplicateColumns(df):
#     dupSet = set() # create empty set
#     for x in range(df.shape[1]): # shape[0] = show num of rows shape[1] = show num of colms
#         xColName = df.columns[x]
#         xValues = df.iloc[:, x] # select all rows for each column iterated
#         for y in range(x+1, df.shape[1]):
#             yColName = df.columns[y]
#             yValues = df.iloc[:, y] # compares columns header + values with the column directly next to it
#             if xValues.equals(yValues):
#                 dupSet.add(yColName)
#                 df.drop(labels = yColName, axis = 1)

# use to create a file for testing. Consist of duplicate columns
    # df['testing1'] = df.loc[:, 'IRMS_A'] # copy all rows in the "IRMS_A" column only
    # df['testing2'] = df.loc[:, 'IRMS_B']
    # df['testing3'] = df.loc[:, 'IRMS_C']
    # df.rename(columns={'testing1':'IRMS_A'}, inplace = True)
    # df.rename(columns={'testing2':'IRMS_B'}, inplace = True)
    # df.rename(columns={'testing3':'IRMS_C'}, inplace = True)
    # df.to_csv("C:\\Users\\E707562\\WorkSpace\\Project\\Staging\\Blk166APunggolCentral_Oct2021_combined_outer_Solar1.csv", sep = '\t')

# used to find min, max, median values of the time-gaps
# the printed rows are used to see how the substract() function
# worked with timeseries objects
# finding the MinimumTimeGapValue in the column
    # min_time_gap_val = df['time_gap'].min()
    # print("The minimum is:")
    # print(min_time_gap_val)
    
    # print("")

    # # finding the index of the MinimumTimeGapValue in the column
    # # Printing the 3 values surroudning this value for analysis
    # mtgv = df['time_gap'].idxmin()
    # print("These are the minimum columns:")
    # # since all time gaps difference is 1 sec. The min and
    # # maximum time gaps are the same. The indexed columns selected
    # # are random actually
    # print(df['TIMESTAMP'][mtgv-1])
    # print(df['TIMESTAMP'][mtgv])
    # print(df['TIMESTAMP'][mtgv+1])
    
    # print("")

    # # finding the MaximumTimeGapValue in the column
    # max_time_gap_val = df['time_gap'].max()
    # print("The max is:")
    # print(max_time_gap_val)

    # print("")

    # # finding the index of the MaximumTimeGapValue in the column
    # # Printing the 3 values
    # mtgv2 = df['time_gap'].idxmax()
    # print("These are the maximum columns:")
    # print(df['TIMESTAMP'][mtgv2-1])
    # print(df['TIMESTAMP'][mtgv2])
    # print(df['TIMESTAMP'][mtgv2+1])
    
    # print("")
    
    # # Finding the MedianTimeGapValue in the column
    # median_time_gap_val = df['time_gap'][np.argsort(df['time_gap'])[len(df['time_gap'])//2]]
    # print("The median is:")
    # print(median_time_gap_val)

    # print("")

    # # finding the index of the MedianTimeGapValue in the column
    # # Printing the 3 values
    # mtgv3 = np.argsort(df['time_gap'])[len(df['time_gap'])//2]
    # print("These are the median columns:")
    # print(df['TIMESTAMP'][mtgv3-1])
    # print(df['TIMESTAMP'][mtgv3])
    # print(df['TIMESTAMP'][mtgv3+1])

# # run python datapipeline.py in anaconda cmd
#     with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#         print(df)


# OLD duplicated columns function
# df = df.loc[:,~df.columns.duplicated()].copy()