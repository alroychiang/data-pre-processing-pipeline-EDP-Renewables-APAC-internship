import pandas as pd
import numpy as np

def create_duplicate_cols_():
    # need to slice and rename because we need the EXACT column values.
    dir = r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_Sep2020_combined_outer_BPump.csv"

    df = pd.read_csv(dir, sep = "\t", nrows = 100, header = 0, index_col = 0)

    # slicing and renaming columns off the dataframe: IRMS_A, IRMS_A, IRMS_A
    # (, IRMS_A.1, IRMS_A.1, IRMS_A.1.1 (usual columns ->) IRMS_A.... continue...........)
    sliced1 = df.iloc[:, :1]
    sliced2 = df.iloc[:, :1]

    df = pd.concat([sliced1, df], axis = 1)
    df = pd.concat([sliced2, df], axis = 1)

    # --------------------------------------------
    # takine IRMS_B columns. To demo diff column values but same column names
    diffcol1 = df.iloc[:, -4:-1]

    diffcol1 = diffcol1.rename(columns = {"RPOW_A": "IRMS_B", "RPOW_B": "IRMS_B", "RPOW_C": "IRMS_B"})

    df = pd.concat([diffcol1, df], axis = 1)

    # --------------------------------------------
    # creating IRMS_C.1, IRMS_C.1.1, IRMS_C.2 columns. To demo diff column values but same column names

    print("")

    sliced_diff1 = df.iloc[:, -5:-4]
    sliced_diff2 = df.iloc[:, -5:-4]
    sliced_diff3 = df.iloc[:, -5:-4]

    sliced_diff1 = sliced_diff1.rename(columns = {"POWER_C": "IRMS_C.1"})
    sliced_diff2 = sliced_diff2.rename(columns = {"POWER_C": "IRMS_C.1.1"})
    sliced_diff3 = sliced_diff3.rename(columns = {"POWER_C": "IRMS_C.2"})

    df = pd.concat([sliced_diff1, df], axis = 1)
    df = pd.concat([sliced_diff2, df], axis = 1)
    df = pd.concat([sliced_diff3, df], axis = 1)

    print("")
    # writing to csv file
    df.to_csv(dir, sep = '\t')

create_duplicate_cols_()

def monthly_concat_test_files(df):
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    df['TIMESTAMP'] = df['TIMESTAMP'] + pd.Timedelta(seconds=3)

    df.to_csv(r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_Only10RowsSept_Agg.csv", sep = '\t')
    print("")
    # getting the mid index
    mid_index = df.index[len(df)//2] -1

    # # get all rows of the first half of the dates
    first_half_dates = df.loc[:mid_index, "TIMESTAMP"]

    # # changing the second half of the dates in the dataframe
    start_date = pd.Timestamp("2022-10-01")

    # # # creating a "date-range", abit like just "information"
    new_dates = pd.date_range(start = start_date, periods = len(df) - len(first_half_dates), freq = "S")

    # # changing the dates itself in the dataframe
    df.loc[mid_index+1:, "TIMESTAMP"] = new_dates

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    df1 = df[df["TIMESTAMP"].dt.month == 9][:50].append(df[df["TIMESTAMP"].dt.month == 10][:50])
    df2 = df[df["TIMESTAMP"].dt.month == 9][50:].append(df[df["TIMESTAMP"].dt.month == 10][50:])
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)

    print("this is df1")
    print(df1)

    print("this is df2")
    print(df2)


def old_old_insert_random(df):
    # to randomly insert NaN, NaT, None, NONE string values into my dataframe
    import random
    ix = [(row, col) for row in range(df.shape[0]) for col in range(df.shape[1])]
    for row, col in random.sample(ix, int(round(.1*len(ix)))):
        df.iat[row, col] = np.datetime64('NaT')
    print("")
    df.to_csv(r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_EmptyStr_NULLStr_NAStr_Strings_NaT_Refuse1.csv", sep = '\t')


def old_insert_random_nan_into_df(df):
    # the same random code but different
    rows = df.sample(frac = 0.1, random_state = 1)
    cols = rows.sample(n=4, axis = 1, random_state = 2).columns
    rows = rows.index
    df.loc[rows, cols] = np.nan

    indices = df['TIMESTAMP'].sample(n= 20, replace=False, random_state=1).index
    df.loc[indices, 'TIMESTAMP'] = np.nan  # set the selected indices to NaN


def insert_random_NaN_Strings_intoDF():
    f = r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_Sep2020_combined_outer_Light.csv"
    df = pd.read_csv(f, sep = "\t", nrows = 2000, header = 0, index_col = 0)

    # to randomly inserts NaN and "string" into my dataframe
    # ix == [(0, 0), (0, 1), (0, 2), (0, 3),...] a list of coordinates
    import random
    ix = [(row, col) for row in range(df.shape[0]) for col in range(df.shape[1]-1)]

    # from the list ix[] we randomly choose 10% of the total coordinates and convert them to np.nan
    for row, col in random.sample(ix, int(round(.1*len(ix)))): # .1*len(ix) divides by 10
        df.iat[row, col] = np.nan
    # [] makes a list
    ix = [(row, col) for row in range(df.shape[0]) for col in range(df.shape[1]-1)]
    for row, col in random.sample(ix, int(round(.1*len(ix)))):
        df.iat[row, col] = "string"
    # print("")

    df.to_csv(r"C:\Users\E707562\WorkSpace\project\staging\Blk10ChaiCheeRd_testing.csv", sep = '\t')