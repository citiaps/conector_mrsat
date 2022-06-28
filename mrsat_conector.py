import sys
import os
import logging
import json
import pandas as pd
from zeep import Client, Settings, helpers
from zeep.transports import Transport
from requests import Session
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, text
from datetime import date, datetime



def append_new_records(df, config_data, db_engine, logger):
    """Append the records from the past 3 days to the database table with toxicologic data.

    Args:
        df (pandas.core.frame.DataFrame): Dataframe with the new records.
        config_data (dict): config.json parameters.
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        
    Raises:
        SAWarning: Did not recognize type 'geometry' of column 'geom'
    """

    df.to_sql(config_data['sernapesca']['table'], 
                db_engine, 
                if_exists = 'append', 
                schema = config_data['sernapesca']['schema'], 
                index = False)

    print("[OK] - new records successfully appended to " + config_data['sernapesca']['table'] + " table")
    logger.debug("[OK] - APPEND_NEW_RECORDS")


def delete_old_records(db_engine, sql_query, logger):
    """Execute the given sql query on the database, which deletes all the records from the past 3 days.  

    Args:
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        sql_query (sqlalchemy.sql.elements.TextClause): SQL file query
    """

    with db_engine.connect().execution_options(autocommit=True) as con:
        con.execute(sql_query)
    print("[OK] - Old records successfully deleted")
    logger.debug("[OK] - DELETE_OLD_RECORDS")

def open_sql_query(sql_file, logger):
    """Open the passed SQL query as a sqlalchemy text clause.

    Args:
        sql_file (str): Name of the .sql file to execute

    Returns:
        sqlalchemy.sql.elements.TextClause
    """

    with open("./sql_queries/" + sql_file, encoding = "utf8") as file:
        sql_query = text(file.read().format("entradas", "gestio_sp"))
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
    logger.debug("[OK] - CREATE_DB_ENGINE")
    return db_engine

def create_db_connection(config_data, logger):
    """Create the database connection string based on the config file parameters.

    Args:
        config_data (dict): config.json parameters.

    Returns:
        str
    """

    db_connection = '{}://{}:{}@{}:{}/{}'.format(
        config_data['sernapesca']['db_type'],
        config_data['sernapesca']['user'],
        config_data['sernapesca']['passwd'], 
        config_data['sernapesca']['host'], 
        config_data['sernapesca']['port'], 
        config_data['sernapesca']['db'])
    print("[OK] - Connection string successfully generated")
    logger.debug("[OK] - CREATE_DB_CONNECTION")
    return db_connection   

##############
def dict_to_df(response_dict):
    df = pd.DataFrame(response_dict["Sdtsnp"]["SDTSNP.SDTSNPItem"])
    return df

def reponse_to_dict(ws_response):
    response_dict = helpers.serialize_object(ws_response)
    return response_dict

def get_ws_response(config_data, client):
    ws_reponse = client.service.Execute(Usuario = config_data['webservice']['user'], 
                                        Password = config_data['webservice']['passwd'],
                                        Fechaconsulta = str(date.today()),
                                        Numerodias = 59)
    return ws_reponse

def make_client(ws_url, session, settings):
    client = Client(ws_url, transport = Transport(session= session), settings= settings)
    return client

def set_settings():
    settings = Settings(strict = False)
    return settings

def set_user_and_psswd(session, config_data):
    session.auth = (config_data['webservice']['user'], config_data['webservice']['passwd'])

def generate_session():
    session = Session()
    return session

def generate_ws_url_string(config_data):
    ws_url = config_data['webservice']['url']
    return ws_url

################

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


def main(argv):
    start = datetime.now()

    # Get parameters
    config_filepath = get_parameters(argv)

    # Get service config parameters
    config_data = get_config(config_filepath)

    # Create the log file if not exists
    log_file = create_log_file(config_data["log_path"])

    # Create the logger
    logger = create_logger(log_file)

    # Get the web service URL
    ws_url = generate_ws_url_string(config_data)

    # Generate the client's session
    session = generate_session()

    # Set the session's user and password
    set_user_and_psswd(session, config_data)

    # Set the session's settings
    settings = set_settings()

    # Make the webservice's consumer client
    client = make_client(ws_url, session, settings)

    # Get the WebService response
    ws_response = get_ws_response(config_data, client)

    # Transform the WebService response into a Python dictionary
    response_dict = reponse_to_dict(ws_response)

    # Transform python dict into Pandas DataFrame
    df = dict_to_df(response_dict)

    # Create string with the given database parameters
    db_connection = create_db_connection(config_data, logger)

    # Create sqlalchemy engine based on database parameters
    db_engine = create_db_engine(db_connection, logger)

    # Open the "delete_recent_records.sql" file
    sql_query = open_sql_query("delete_recent_records.sql", logger)

    # Delete the past 3 days records from the database table
    delete_old_records(db_engine, sql_query, logger)

    # Append the new records to the database table, extracted from the WebService
    append_new_records(df, config_data, db_engine, logger)

    end = datetime.now()

    print("[OK] - New records successfully appended to " + config_data['sernapesca']['table'] + f" table. Time elapsed: {end - start}")

if __name__ == "__main__":
    main(sys.argv)