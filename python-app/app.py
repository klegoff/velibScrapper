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

# set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DB connection
USER="user"
PASSWORD="password"
HOST="host.docker.internal"
PORT="5432"
DATABASE="user"

####################
#
# Establish database
# connection
#
####################

@retry(tries=10, delay=30)
def connectDB(): 
    try:
        connection = psycopg2.connect(user=USER,password=PASSWORD,host=HOST,port=PORT,database=DATABASE)
        connection. autocommit = True
        cursor = connection.cursor()
        logger.info(str(datetime.datetime.now()) + " - Connection to DB established.")
        return cursor
        
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
# Database writing
#
####################

def insertStationData(station_data, cursor):
    """
    insert data in the database
    data = station_data (type = dataframe)
    """
    for _idx, row in station_data.iterrows():
        row = tuple(row)
        try:
            cursor.execute("""INSERT INTO station (stationcode, name, nom_arrondissement_communes, capacity, coordonnee_x, coordonnee_y) VALUES (%s, %s, %s, %s, %s, %s);""",row)
        except Exception as e:
            #print(e)
            pass

def insertHistoricalData(historical_data, cursor):
    """
    insert data in the database
    data = station_data (type = dataframe)
    """
    new_line_count = 0

    for _idx, row in historical_data.iterrows():
        row = tuple(row)
        try:
            cursor.execute("""INSERT INTO historic   (record_id,  stationcode, ebike, mechanical, numbikesavailable, numdocksavailable, is_renting, is_installed, is_returning, duedate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",row)
            new_line_count += 1

        except Exception as e:
            #print(e)
            pass

    logger.info("Number of new rows in HISTORIC  : " + str(new_line_count))

def fillDB(cursor, save_station = False):
    """
    execute the whole data pipeline:
    read from api, process, save in db
    save_station = True, if we want to save station data
    """
    logger.info(str(datetime.datetime.now())  + " - Data extract.")
    station_data, historical_data = format(getData())

    if save_station:
        insertStationData(station_data, cursor)
        insertHistoricalData(historical_data, cursor)
    
    else:
        insertHistoricalData(historical_data, cursor)


scheduler = sched.scheduler(time.time, time.sleep)

def schedule_wrapper(period, duration, func, cursor):
    """
    schedule our function
    source : https://stackoverflow.com/a/12136105/14843174
    """
    no_of_events = int( duration / period )
    for i in range( no_of_events ):
        delay = i * period
        if i == 0:
            scheduler.enter(delay, 1, func, (cursor, True)) #we save station data, and historical data
        else:
            scheduler.enter(delay, 1, func, (cursor, False)) # we save only historical data


if __name__ == "__main__":

    # Connect to database
    cursor = connectDB()

    # request data from api, transform, and load in DB
    schedule_wrapper(60, 30 * 24 * 3600, fillDB, cursor) # every minutes for
    scheduler.run()
