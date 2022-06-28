import sys
import os
import logging
import http.client
import urllib.parse
import xml.dom.minidom
import pandas as pd
from zeep import Client, Settings
from zeep.transports import Transport
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import helpers
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import date


 
 ####################################
 #### CAMBIAR REPLACE POR APPEND ####
 ####################################
def append_new_records(df, config_data, mapstore_engine, table_name, logger):
    """Append the records from the past 3 days to the database table with toxicologic data.

    Args:
        df (pandas.core.frame.DataFrame): Dataframe with the new records.
        config_data (dict): config.json parameters.
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        table_name (str): Name of the output table on the database.
    
    Raises:
        SAWarning: Did not recognize type 'geometry' of column 'geom'
    """

    df.to_sql(table_name, 
                mapstore_engine, 
                if_exists = 'replace', 
                schema = config_data['mapstore']['schema'], 
                index = False)

    print("[OK] - " + table_name + " dataframe successfully copied to Mapstore database")
    logger.debug("[OK] - " + table_name.upper() + " DF_TO_DB")


def delete_old_records(db_engine, sql_query, logger):
    """Execute the given sql query on the database, which deletes all the records from the past 3 days.  

    Args:
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        sql_query (sqlalchemy.sql.elements.TextClause): SQL file query
    """

    with db_engine.connect().execution_options(autocommit=True) as con:
        con.execute(sql_query)
    print("[OK] - SQL query successfully executed")
    logger.debug("[OK] - EXECUTE_SQL_QUERY")

def open_sql_query(sql_file, logger):
    """Open the passed SQL query as a sqlalchemy text clause.

    Args:
        sql_file (str): Name of the .sql file to execute

    Returns:
        sqlalchemy.sql.elements.TextClause
    """

    with open("./sql_queries/" + sql_file, encoding = "utf8") as file:
        sql_query = text(file.read())
    print("[OK] - " + sql_file + " SQL file successfully opened")
    logger.debug("[OK] - " + sql_file.upper() + " OPEN_SQL_QUERY")
    return sql_query


def create_db_engine(db_connection, logger):
    """Create sqlalchemy database engine based on the database connection string.

    Args:
        db_connection (str): string with the databse connection.

    Returns:
        sqlalchemy.engine.base.Engine
    """

    db_engine = create_engine(db_connection)
    print("[OK] - SQLAlchemy engine succesfully generated")
    logger.debug("[OK] - CREATE_db_ENGINE")
    return db_engine

def create_db_connection(config_data, logger):
    """Create the database connection string based on the config file parameters.

    Args:
        config_data (dict): config.json parameters.

    Returns:
        str
    """

    db_connection = 'postgresql://{}:{}@{}:{}/{}'.format(
        config_data['mapstore']['user'],
        config_data['mapstore']['passwd'], 
        config_data['mapstore']['host'], 
        config_data['mapstore']['port'], 
        config_data['mapstore']['db'])
    print("[OK] - Connection string successfully generated")
    logger.debug("[OK] - CREATE_DB_CONNECTION")
    return db_connection   


### FUNCIONES PARA EXTRAER LA INFORMACIÓN NECESARIA DEL WEB SERVICE


def create_logger(log_file):
    """Create a logger based on the passed log file.

    Args:
        log_file (str): Path of the log file.

    Returns:
        class logging.RootLogger.
    """
    logging.basicConfig(filename = log_file,
                    format='%(asctime)s %(message)s',
                    filemode='a')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    return logger

def create_log_file(log_path):
    """Create the log folder if not exists. Get the log file name.

    Args:
        log_path (str): Path of the log folder.

    Returns:
        str
    """
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    log_file = log_path + "/mrsat_conector.log"
    return log_file 

def get_config(filepath=""):
    """Read the config.json file.

    Args:
        filepath (string):  config.json file path
    
    Returns:
        dict
    """

    if filepath == "":
        sys.exit("[ERROR] - Config filepath empty.")

    with open(filepath) as json_file:
        data = json.load(json_file)

    if data == {}:
        sys.exit("[ERROR] - Config file is empty.")

    return data

def get_parameters(argv):
    """Store the input parameters.

    Args:
        argv (list):  input parameters
    
    Returns:
        string: config.json path
    """

    config_filepath = argv[1]
    return config_filepath