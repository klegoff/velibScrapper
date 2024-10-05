from retry import retry
import datetime
import logging
import pandas as pd
import sqlalchemy as sa

# set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

####################
#
# Establish database
# connection
#
####################

@retry(tries=10, delay=30)
def connectDB(username, password, host, database): 
    try:
        connection_url = sa.URL.create("postgresql",
                                       username=username,
                                       password=password,
                                       host=host,
                                       database=database)
        logger.warning(str(connection_url))
        engine = sa.create_engine(connection_url, pool_recycle=3600)#, echo=True, echo_pool="debug")
        logger.warning(str(datetime.datetime.now()) + " - Connection to DB established.")
        return engine

    except Exception as e:
        logger.warning(str(datetime.datetime.now()) +" - Connection to DB failed, will retry.")
        raise e
        return None

####################
#
# Query database
#
####################

def insertData(data, engine, table="historic", pkey="record_id", log_count=False):
    """
    insert data in the database
    data = station_data or historical_data (type = dataframe)
    """
    with engine.begin() as conn:
       
        # keep entries that aren't already in db
        current_ids = retrieveData(engine, table=table, select_field=pkey)
        if current_ids.shape[0] == 0:
            current_ids=[]
        else:
            current_ids = current_ids.iloc[:, 0].values.tolist()
        data = data.loc[~data[pkey].isin(current_ids)]

        # write them
        data.to_sql(name=table, con=conn, if_exists="append", index=False, schema="public")
    
        if log_count :
            previous_count = len(current_ids)
            logger.info(f"{str(datetime.datetime.now())} - {table} entries : {previous_count} + {data.shape[0]}")
        

def retrieveData(engine, table="station", select_field="*"):
    """
    extract whole table from database
    table = "station" or "historic"
    """
    with engine.connect() as conn:
        results = conn.execute(sa.text(f"select {select_field} from public.{table}"))
    return pd.DataFrame(results)

def retrieveData_stationcode(engine, stationcode = 16107):
    
    with engine.begin() as conn:
        query = f"SELECT * from historic WHERE stationcode = '{stationcode}'"
        results = conn.execute(sa.text(query))
    
    return pd.DataFrame(results)

