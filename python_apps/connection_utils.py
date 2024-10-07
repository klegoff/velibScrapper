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
        engine = sa.create_engine(connection_url, pool_recycle=3600, connect_args={'sslmode': "disable"})#, echo=True, echo_pool="debug")
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

def count_entries(engine, table):
    """
    count entries in the database
    engine = db engine (type = sqlalchemy.engine.base.Engine)
    table = table object (type = sqlalchemy.sql.schema.Table)
    """
    with engine.begin() as conn:
        q = sa.func.count().select().select_from(table)
        results = conn.execute(q)
    return (list(results)[0][0])

def insertData(data, engine, table, log_count=False):
    """
    insert data in the database
    data = station_data or historical_data (type = pd.DataFrame)
    engine = db engine (type = sqlalchemy.engine.base.Engine)
    table = table object (type = sqlalchemy.sql.schema.Table)
    """
    previous_count = count_entries(engine, table=table)
    
    with engine.begin() as conn:

        for record in data.to_dict("records"):
        
            statement = sa.dialects.postgresql.insert(table).values(record).on_conflict_do_nothing()
            conn.execute(statement)
            
        if log_count :
            new_count = count_entries(engine, table=table)
            message = f"{str(datetime.datetime.now())} - {table} new entries : {previous_count - new_count}"
            logger.info(message)

def retrieveData(engine, table):
    """
    extract whole table from database
    engine = db engine (type = sqlalchemy.engine.base.Engine)
    table = table object (type = sqlalchemy.sql.schema.Table)
    """
    with engine.connect() as conn:
        select = sa.select(table)
        return pd.DataFrame(conn.execute(select))

def retrieveData_stationcode(engine, historic_table, stationcode = "16107"):
    """
    extract historic for given station code
    table = table object (type = sqlalchemy.sql.schema.Table)
    stationcode = id for station (type = str)
    """
    with engine.begin() as conn:
        select = sa.select(historic_table).where(historic_table.c.stationcode == stationcode)
        return pd.DataFrame(conn.execute(select))

