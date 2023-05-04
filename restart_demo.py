import os
import shutil
import json
import glob

# enter your test case number and run the program
# leave as empty string "" if you just want to reset everything and not import any test files
TEST_CASE = "present"


staging = r"C:\Users\E707562\WorkSpace\project\staging"
cleaned = r"C:\Users\E707562\WorkSpace\project\processed\cleaned"
process_rejected = r"C:\Users\E707562\WorkSpace\project\processed\rejected"
staging_rejected = r"C:\Users\E707562\WorkSpace\project\staging\rejected"
address_json = r"C:\Users\E707562\WorkSpace\project\address.json"
config_json = r"C:\Users\E707562\WorkSpace\project\config.json"
testP_dir = r"C:\Users\E707562\WorkSpace\test procedures" + "\\" + f"point {TEST_CASE.split('.')[0]}" # folder must be named exactly as "point __digit__"
al_csv = r"C:\Users\E707562\WorkSpace\project\processed\cleaned\address_legend.csv"
add_leg_file = r"C:\Users\E707562\OneDrive - EDP\Desktop\address_legend.csv"


# clean "staging" folder
for l in os.listdir(staging):
     f_file = staging + '\\' + l
     if l == "rejected":
          continue
     else:
          os.remove(f_file)


# clean "cleaned" folder
for l in os.listdir(cleaned):
    f_file = cleaned + '\\' + l

    if l.isnumeric():
        shutil.rmtree(f_file)
        continue

    if l == "address_legend.csv":
        continue
    
    else:
        os.remove(f_file)


# clear "rejected" folder under 'processed' parent directory
for l in os.listdir(process_rejected):
    f_file = process_rejected + '\\' + l
    os.remove(f_file)


# clear "rejected" folder under 'staging' parent directory
for l in os.listdir(staging_rejected):
    f_file = staging_rejected + '\\' + l
    os.remove(f_file)


# this function is to empty address.json file for clean slate purposes
with open(address_json, 'w') as j:
    json.dump({}, j)
    

# region deleting some address.json values only
# # delete entire 776655 Harbourfront key-value pair for 1.7b and 1.7c demonstration
# with open(address_json, 'r') as j:
#     data = json.load(j)
    
#     try:
#         del data['776655']
#     except KeyError as e:
#         print("\n776655 already deleted (for 1.7a 1.7c demo)")

# with open(address_json, 'w') as j:
#     json.dump(data, j, indent = 2)


# # removing the value: "rd10chaicheeblk" (not entire key) from address.json file for 5.4 demonstration
# with open(address_json, 'r') as j:
#     data = json.load(j)
    
#     try:
#         data["467010"].remove('rd10chaicheeblk')
#     except (KeyError, ValueError) as e:
#         print("\nrd10chaicheeblk already deleted (for 5.4 demo)")

# with open(address_json, 'w') as j:
#     json.dump(data, j, indent = 2)
# endregion


# removing all LOG_SCALE{} & TIME_DIFFERENCING{} entries in config.json file for clean_slate purposes
with open(config_json, 'r') as j:
    data = json.load(j)
    data["LOG_SCALE"] = []
    data["TIME_DIFFERENCING"] = []
    data["TIMEGAP_THRESH"] = 100
with open(config_json, 'w') as j:
    json.dump(data, j, indent = 2)


# clearing the contents of address_legend.csv file only
def clear_add_leg():
    al_file = open(al_csv, "w+")
    al_file.close()


clear_add_leg()


# delete the entire address_legend.csv file
os.remove(al_csv)


# log and time_diff{} test cases
if TEST_CASE == "3.3" or TEST_CASE == "3.2":
    with open(config_json, 'r') as j:
        data = json.load(j)
        data["LOG_SCALE"] = ["I1", "I2", "I3"]
        data["TIME_DIFFERENCING"] = [["I1", 2], ["I2", 2], ["I3", 2]] 
    with open(config_json, 'w') as j:
        json.dump(data, j, indent = 2)


# if test case 5.4 is being invoked, copy the desktop address_legend.csv to 
# cleaned folder with 467010, "..." alrd written in it
if TEST_CASE == "5.4":
    shutil.copy(add_leg_file, cleaned)


# to set TIMEGAP_THRESH to 5
if TEST_CASE == "5.6":
    with open(config_json, 'r') as j:
        data = json.load(j)
        data["TIMEGAP_THRESH"] = 5   
    with open(config_json, 'w') as j:
        json.dump(data, j, indent = 2)


if TEST_CASE == 'present':
    with open(config_json, 'r') as j:
        data = json.load(j)
        data["LOG_SCALE"] = ["I1", "I2", "I3"]
        data["TIME_DIFFERENCING"] = [["I1", 2], ["I2", 2], ["I3", 2]] 
    with open(config_json, 'w') as j:
        json.dump(data, j, indent = 2)
    shutil.copy(add_leg_file, cleaned)
    i = r'C:\Users\E707562\WorkSpace\test procedures\point 3\3.2 & 3.3 extra log & time_diff columns generated\Blk10ChaiCheeRd_Dup_Cols_BPump.csv'
    shutil.copy(i, staging)

    add_thing = {'467010': 'blk10chaicheerd'}
    with open(address_json, 'w') as j:
        json.dump(add_thing, j)

print("\nEverything is resetted")


# copying all csv files in TEST_CASE folder that was selected
try:
    for foldername in os.listdir(testP_dir):
        if TEST_CASE in foldername:

            # getting for source destination
            demo_folder = testP_dir + "\\" + foldername
            
            # getting the csv files within the source destination
            demo_files = glob.glob(f"{demo_folder}/*.csv")
            for i in demo_files:
                shutil.copy(i, staging)

            print(f"\n{TEST_CASE} test case files have successfully been copied to staging folder")

except FileNotFoundError as e:
    print("\nThe test case you entered did not exist but everything has been resetted nonetheless.") 