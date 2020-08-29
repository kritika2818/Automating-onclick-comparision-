import pymongo
import pandas as pd
import os
import json
import datetime
from pandas.tseries.offsets import BDay
from pymongo.database import Database


def get_json_path(filename, verbose=False):
    if verbose:
        print("Working directory of project ", os.getcwd())
    json_path = '../JSON'
    re_path = os.path.relpath(json_path, os.getcwd())
    path = os.path.join(re_path, filename)
    if verbose:
        print(re_path)
        print("Final JSON path", path)
    return path


def read_settings(config="ddac.json"):
    with open(get_json_path(config, verbose=False)) as config_file:
        config = json.load(config_file)
    config = config["DDAC_DB"]  #name in json file
    return config


def create_database(input, mycol):
    ddac_list = os.listdir(input)
    for file_name in range(len(ddac_list)):
        df = pd.read_csv(os.path.join(input, ddac_list[file_name]))
        data_json = json.loads(df.to_json(orient='records'))
        mycol.insert_many(data_json)
        print(ddac_list[file_name], "inserted in database")


previousdate = datetime.datetime.today() - BDay(1)
previousdate1 = previousdate.strftime("%Y%m%d")  #automatically detecting current date / previous date 


def extract_previous(mycol, previousdate1, output):
    # date = pd.datetime.today() - BDay(1)
    # previousdate = pd.datetime.today() - BDay(2)
    # previousdate1 = previousdate.strftime("%Y%m%d")
    a = mycol.find({"Date": {"$gt": int(previousdate1)}}) #mongodb command 
    df = pd.DataFrame(list(a))
    if "_id" in df:
        del df["_id"]
    return df.to_csv(os.path.join(output, "ddac" + previousdate1 + ".csv"))


todaydate = datetime.datetime.today()
todaydate1 = todaydate.strftime("%Y%m%d")    #automatically detecting current date / previous date 

def extract_current(mycol, todaydate1, output):
    # date = pd.datetime.today() - BDay(1)
    # previousdate = pd.datetime.today() - BDay(2)
    # previousdate1 = previousdate.strftime("%Y%m%d")
    a = mycol.find({"Date": int(todaydate1)})
    df = pd.DataFrame(list(a))
    if "_id" in df:
        del df["_id"]
    return df.to_csv(os.path.join(output, "ddac" + todaydate1 + ".csv"))


def compare(previousdate1, todaydate1):
    b = mycol.count_documents({"Date": int(previousdate1)})
    c = mycol.count_documents({"Date": int(todaydate1)})    #if you want to compare data inside database only (automatically)
    d = b / c
    e = d * 100
    if e > 95 and e < 110:
        print("comparision between previous and current  dates is approvable")
    else:
        print("error")


if __name__ == "__main__":
    config = read_settings()
    input = config["input"]
    output = config["output"]

    previous = config["previous_date"]
    current = config["current_date"]
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["ddac"]
    mycol = mydb["colddac"]

    c = mycol.count_documents({"Date": int(todaydate1)})
    #print(c)
    b = mycol.count_documents({"Date": int(previousdate1)})
    #print (b)
    #compare(previousdate1,todaydate1)
    #create_database(input,mycol)
    #extract_current(mycol, previousdate1, output)
    #extract_current(mycol, todaydate1, output)     
