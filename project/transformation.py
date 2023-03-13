import pandas as pd
import numpy as np
import json

PRINT = False

# transformation cannot consist of the function:
# change datetime into dd-mm-yyyy format because
# transform comes before metagen
# in metagen we need the datetime format to be in YYYY-mm-dd

# opening json file
js = open(r"C:\Users\E707562\WorkSpace\project\config.json", "r")
configdata = json.loads(js.read())
js.close()

def linear_log(df):
    if PRINT: print("\nCreate a new column of log scale values from linear scale. No column created if config[] empty\n")
    for col in configdata["LOG_SCALE"]:
        # E.g LOG(I1) column will be created
        df[f"LOG({col})"] = np.log(df[col])

def time_differencing(df):
    if PRINT: print("\nNew column of time differencing created\n")
    for colnm, period in configdata["TIME_DIFFERENCING"]:
        # a new column E.g I1_diff(1) will be created. I1 is the column name and the numeral within diff() is the period between rows
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        df[f"{colnm}_diff({period})"] = df['TIMESTAMP'].shift(periods = -period, axis = 0) - df['TIMESTAMP']


# main()
def transformation(df):

    linear_log(df)

    time_differencing(df)

    return df

