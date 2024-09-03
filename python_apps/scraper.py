import os
from distutils.log import error
import requests
import sched
import time
from retry import retry
import datetime
import logging
import numpy as np
import pandas as pd
import sqlalchemy as sa

from connection_utils import connectDB, insertData, retrieveData 

DEBUG_MODE = False

# set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

####################
#
# Data processing
#
####################

def getData():
    """
    request the velib API, and return data
    """
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=10000&start=0&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
    z = requests.get(url)
    data = z.json()
    return data

def format(data):
    """
    format data, and split into stations / historical data
    """
    # extract info from json
    df = pd.DataFrame(data["records"])
    x = pd.DataFrame(df["fields"].to_dict()).T
    df = df.drop(columns=["fields", "datasetid"])
    df = pd.concat([df,x], axis=1)

    # split the 2 coordinates
    coordonnees = pd.DataFrame(np.hstack(df["coordonnees_geo"].values).reshape(-1,2), columns=["coordonnee_x","coordonnee_y"])
    df = pd.concat([df.drop(columns="coordonnees_geo"), coordonnees],axis=1)

    # split into stations data & historical data
    station_data = df[["stationcode", # primary key
        "name",
        "capacity",
        "nom_arrondissement_communes",
        "coordonnee_x",
        "coordonnee_y"
        ]]

    historical_data = df[["recordid", # primary key
        "stationcode", # link to the other table
        "duedate",
        "ebike",
        "mechanical",
        "numbikesavailable",
        "numdocksavailable",
        "is_renting",
        "is_installed",
        "is_returning"]].rename(columns={"recordid":"record_id"})

    # sort for insertion in db
    columns_order_1 = ['stationcode', 'name','nom_arrondissement_communes', 'capacity', 'coordonnee_x', 'coordonnee_y']   
    station_data = station_data.T.reindex(columns_order_1).T
    
    columns_order_2 = ['record_id', 'stationcode','ebike', 'mechanical', 'numbikesavailable', 'numdocksavailable', 'is_renting', 'is_installed', 'is_returning', 'duedate']
    historical_data = historical_data.T.reindex(columns_order_2).T

    # cast bool
    historical_data = historical_data.replace({"OUI":True, "NON":False})
    return station_data, historical_data

####################
#
# Database related
#
####################

def fillDB(engine):
    """
    execute the whole data pipeline:
    read from api, process, save in db
    save_station = True, if we want to save station data
    """
    logger.info(str(datetime.datetime.now())  + " - Data extract.")
    
    try:
        station_data, historical_data = format(getData())
        insertData(historical_data, engine, "historic","record_id", log_count=True)
        insertData(station_data, engine, "station","stationcode", log_count=True)
        
    except Exception as e:
        logger.warning("Data acquisition failed")
        logger.warning(e)
        return

scheduler = sched.scheduler(time.time, time.sleep)

def schedule_wrapper(period, duration, func, engine):
    """
    schedule our function
    source : https://stackoverflow.com/a/12136105/14843174
    """
    no_of_events = int( duration / period )
    for i in range( no_of_events ):
        delay = i * period
        scheduler.enter(delay, 1, func, (engine,)) #we save station data, and historical data

if __name__ == "__main__":
     
    # DB connection
    USER=os.getenv("POSTGRES_USER")
    PASSWORD=os.getenv("POSTGRES_PASSWORD")
    HOST="db_app" #"localhost"
    PORT=5432
    DATABASE=os.getenv("POSTGRES_DB")
    
    engine = connectDB(username=USER, password=PASSWORD, host=HOST, database=DATABASE)

    if DEBUG_MODE:
        ### FOR DEBUG : run the process by hand 
        
        import time
        time.sleep(3)

        # get fresh data
        station_data, historical_data = format(getData())
        
        # insert in db
        insertData(historical_data, engine, "historic","record_id", log_count=True)
        insertData(station_data, engine, "station","stationcode", log_count=True)

    else:
        ### request data from api, transform, and load in DB, at a given frequency
        delay = 20 # number of seconds
        total_duration = 30 * 24 * 3600 # number of seconds (30 days)
        schedule_wrapper(delay, total_duration, fillDB, engine)
        scheduler.run()
