import pandas as pd
import json
import shutil
import os
import re

from cleaning import clean_data, read_data, time_gap_thresh
from metageneration import create_meta
from publishing import publish



# opening json file
js = open(r"C:\Users\E707562\WorkSpace\project\config.json", "r")
configdata = json.loads(js.read())
js.close()


DEL_STAGING = True

# Global variable counts the number of df's that were rej due to large missing time-gaps. Used to print an error message for the user only.
REJ_DFS = 0

# Global variable to count the number of FILES that has been processed
PROCESSED_F_COUNT = 0

# Global variable to count the number of rejected FILES encountered
REJECTED_FN_COUNT = 0



# separating the df into 2 or more if there are multiple months in it
def split_df_month(df):  

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    grouped = df.groupby(df["TIMESTAMP"].dt.month)

    return grouped



def process(f):
    
    df = read_data(f)

    # check if the raw file has already been copied to rejected folder (due to exceed timegap threshold)
    copied_alrd = False
    
    grouped = split_df_month(df) # grouped is groupBy object
    
    # counts the number of df's that do not have large missing timme-gaps
    processed_dfs = 0

    
    # df is the dataframe, "_" is the month iterator (e.g process 9th month first then 10th... etc)
    for _, df in grouped: 
       
        if time_gap_thresh(df, f):
            
            # when the next dataframe comes in, this prevens the file being copied again to the rej folder
            if copied_alrd == False: shutil.copy2(f, configdata["TIMEGAP_REJ"])
            copied_alrd = True

            # delete the raw data file from "staging". We alrd have it's entire dataframe read into the program   
            if DEL_STAGING: os.remove(f)
            
            # the current DATAFRAME has been rejected. Add to the count.
            global REJ_DFS 
            REJ_DFS += 1

            # do not let the current dataframe pass through the entire pipeline and waste time. Straight process the next month's worth of dataframe
            continue

        # if the df has an acceptable range of the number of missing rows. Process the df through the pipeline
        df = clean_data(df)

        df, metadf, postal_code, load = create_meta(df, f)
       
        publish(df, metadf, postal_code, load, f) # transformation() inside publish()
        
        # the current DATAFRAME has passed all checks (filename & time-gaps). Considered processed. Add to the count.
        processed_dfs += 1
    
    
    # if processed_dfs is 0, means all months in the data file has been rejected
    # if ALL of the months in the current file have time-gaps > threshold, we print a message to inform user
    if processed_dfs == 0:
        s = f.split('.')[0].split('\\')[-1]
        print(f"\nDue to large amounts of time-gaps in ALL of the months in this file, {s} has been moved to the \"Rejected\" folder.")
        
    return processed_dfs
  



# main()
if __name__ == "__main__":
    # reject all other file formats. Only files with location and load names in the format location_*_load are allowed
    for f in os.listdir(configdata["STAGE"]):

        f = configdata["STAGE"] + '\\' + f

        # skip directories/ folders
        if os.path.isdir(f):
            continue

        # if the file is csv
        format = f.split('.')[-1] # csv
        s = f.split('.')[0].split('\\')[-1] # E.g Blk10ChaiCheeRd_Sep2020_LiftB
        # check if there is even an address at all
        a = s.split('_')[0]
        # to check if there is even a load at all
        z = s.split('_')[-1]

        # a switch to decide if files (that has rejected file name/ extensions) should be deleted
        delete_file = False

        # checks file extension & if there exist an adress and load
        if format == "csv":

            if re.search(r".*_.*_.*", s) and len(a) != 0 and len(z) != 0: # matches (anything)_(anything)_(anything)
                
                print(f"\n{s} has started processing")

                processed_dfs = process(f)

                # if at least a dataframe "month" has been processed through the pipeline, we consider the file to be processed & "process_count" ++
                # just to inform the user that the file has been processed
                if processed_dfs != 0: 
                    print(f"\n{s} has finished processing")
                    PROCESSED_F_COUNT += 1
                    if DEL_STAGING: os.remove(f)
            
            # if file does not have the standard format address_*_load     
            else:
                print("\nInvalid filename: %s was not processed and moved to rejected folder." %s)
                REJECTED_FN_COUNT += 1
                delete_file = True
        
        # if file format is NOT .csv
        else:
            # move f to rejected folder in staging
            print("\nInvalid file extension: %s was not processed and moved to rejected folder. Please use csv files only." %s)
            REJECTED_FN_COUNT += 1
            delete_file = True
            
        # for files that has been rejected due to their file extensions or their filenames. Copy the file to the rejected folder and delete raw file
        if delete_file:
            shutil.copy2(f, configdata["FORMAT_REJ"])
            if DEL_STAGING: os.remove(f)

    print(f"\nThe number of cleaned files is: {PROCESSED_F_COUNT}")
    print(f"\nThe number of files that have invalid filenames is: {REJECTED_FN_COUNT}")
    print(f"\nThe number of monthly dataframes that has been rejected due to large amounts time gaps is: {REJ_DFS}")
            

        
     
    









# ep2020_combined_outer_BPump2.csv", sep = "\t")
# def split_files(df):
#     df_copy = df.copy()
#     mini_df1 = df_copy.iloc[ : 500, :]
    

#     # edit TIMESTAMP
#     mini_df1['TIMESTAMP'] = pd.to_datetime(mini_df1['TIMESTAMP'])  
#     mini_df1['TIMESTAMP'] = pd.date_range(start = '2020-10-01 00:00:01', periods=len(mini_df1), freq='S')
#     df = pd.concat([df, mini_df1])
#     df.reset_index(inplace=True, drop=True)

#     # df2 = df.iloc[1294620 :, :]
#     # print(df1)
#     # print(df2)
#     df.to_csv(r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_Sep2020_combined_outer_Agg (2).csv", sep = "\t")
#     # df2.to_csv(r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_S

 # this method not good. Need to automatically identify ALL months and 
    # segment them into dataframes
    # all rows that are in the same month are assigned to a dataframe
    # df[[]] is to select THE dataframe. The argument is the condition only.
    # df1 = df[df['month'] == first_month]
    # df2 = df[df['month'] != first_month]