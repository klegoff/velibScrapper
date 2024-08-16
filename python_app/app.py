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
import psycopg2
import sqlalchemy as sa

# set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# DB connection
USER="postgres" #os.getenv("POSTGRES_USER")
PASSWORD="password"#os.getenv("POSTGRES_PASSWORD") #"password"
HOST="db"#localhost
PORT=5432
DATABASE="postgres"#"velib_db"

####################
#
# Establish database
# connection
#
####################

@retry(tries=10, delay=30)
def connectDB(): 
    try:
        connection_url = sa.URL.create("postgresql",
                                       username=USER,
                                       password=PASSWORD,
                                       host=HOST,
                                       database=DATABASE)
        engine = sa.create_engine(connection_url, pool_recycle=3600)
        logger.warning(str(datetime.datetime.now()) + " - Connection to DB established.")
        return engine

    except Exception as e:
        logger.warning(str(datetime.datetime.now()) +" - Connection to DB failed, will retry.")
        raise e
        return None

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
    coordonnees = pd.DataFrame(np.concatenate(df["coordonnees_geo"].values).reshape(-1,2), columns=["coordonnee_x","coordonnee_y"])
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

def insertData(data, table_name, engine):
    """
    insert data in the database
    data = station_data or historical_data (type = dataframe)
    """
    with engine.begin() as conn:
        data.to_sql(name=table_name, con=conn, if_exists="append", index=False)

def retrieveData(engine, table="station", select_field="*"):
    """
    extract whole table from database
    table = "station" or "historic"
    """
    with engine.begin() as conn:

        data = pd.read_sql_query(sa.text(f"select {select_field} from {table}"), conn)

    return data 

def fillDB(engine, save_station = False):
    """
    execute the whole data pipeline:
    read from api, process, save in db
    save_station = True, if we want to save station data
    """
    logger.info(str(datetime.datetime.now())  + " - Data extract.")
    try:
        station_data, historical_data = format(getData())
        
    except Exception as e:
        logger.warning("Data acquisition failed")
        logger.warning(e)
        return
        
    if save_station:
        insertData(station_data, "stations", engine=engine)

    # delete pre-existing entries
    previous_ids = retrieveData(engine, "historic","record_id").record_id
    historical_data = historical_data.loc[~historical_data.record_id.isin(previous_ids)]
    insertData(historical_data, "historic", engine=engine)


scheduler = sched.scheduler(time.time, time.sleep)

def schedule_wrapper(period, duration, func, engine):
    """
    schedule our function
    source : https://stackoverflow.com/a/12136105/14843174
    """
    no_of_events = int( duration / period )
    for i in range( no_of_events ):
        delay = i * period
        if i == 0:
            scheduler.enter(delay, 1, func, (engine, True)) #we save station data, and historical data
        else:
            scheduler.enter(delay, 1, func, (engine, False)) # we save only historical data

if __name__ == "__main__":

    # Connect to database
    engine = connectDB()

    ### FOR DEBUG : run the process by hand 
    #station_data, historical_data = format(getData())

    # delete pre-existing entries
    #previous_ids = retrieveData(engine, "historic","record_id").record_id
    
    #print("n_entries:", previous_ids.shape[0])
    
    #historical_data = historical_data.loc[~historical_data.record_id.isin(previous_ids)]
    #insertData(historical_data, "historic", engine=engine)
    
    #new_ids = retrieveData(engine, "historic","record_id").record_id
    #print("n_entries:", new_ids.shape[0])

    ### request data from api, transform, and load in DB
    delay = 20 # number of seconds
    total_duration = 30 * 24 * 3600 # number of seconds (30 days)
    schedule_wrapper(delay, total_duration, fillDB, engine)
    scheduler.run()
