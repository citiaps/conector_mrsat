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
    try:
        df.to_sql(config_data['sernapesca']['table'], 
                    db_engine, 
                    if_exists = 'append', 
                    schema = config_data['sernapesca']['schema'], 
                    index = False)

        print("[OK] - new records successfully appended to " + config_data['sernapesca']['table'] + " table")
        logger.debug("[OK] - APPEND_NEW_RECORDS")

    except Exception as e:
        print("[ERROR] - Appending the new records to the existing table")
        logger.error('[ERROR] - APPEND_NEW_RECORDS')
        sys.exit(2)


def delete_old_records(db_engine, sql_query, logger):
    """Executes the given sql query on the database, which deletes all the records from the past 3 days.  

    Args:
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        sql_query (sqlalchemy.sql.elements.TextClause): SQL file query
    """

    with db_engine.connect().execution_options(autocommit=True) as con:
        con.execute(sql_query)
    print("[OK] - Old records successfully deleted")
    logger.debug("[OK] - DELETE_OLD_RECORDS")

def open_sql_query(sql_file, logger):
    """Opens the passed SQL query as a sqlalchemy text clause.

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
    """Creates a sqlalchemy database engine based on the database connection string.

    Args:
        db_connection (str): string with the databse connection.

    Returns:
        sqlalchemy.engine.base.Engine
    """
    try:
        db_engine = create_engine(db_connection)
        print("[OK] - SQLAlchemy engine succesfully generated")
        logger.debug("[OK] - CREATE_DB_ENGINE")
        return db_engine
    
    except Exception as e:
        print("[ERROR] - Creating the database connection engine")
        print(e)
        logger.error('[ERROR] - CREATE_DB_ENGINE')
        sys.exit(2)

def create_db_connection(config_data, logger):
    """Creates the database connection string based on the config file parameters.

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
    


def dict_to_df(response_dict, logger):
    """Transforms the Python dict to a pandas DataFrame.

    Args:
        response_dict (collections.OrderedDict): past 60 days toxicologic records from the mrSAT WebService.

    Returns:
        pandas.core.frame.DataFrame
    """
    df = pd.DataFrame(response_dict["Sdtsnp"]["SDTSNP.SDTSNPItem"])
    print("[OK] - Python dictionary successfully transformed to pandas DataFrame")
    logger.debug("[OK] - DICT_TO_DF")
    return df

def response_to_dict(ws_response, logger):
    """Transforms the zeep response object to a Python dictionary.

    Args:
        ws_response (zeep.objects.WS_MRSNP.ExecuteResponse): past 60 days toxicologic records from the mrSAT WebService.

    Returns:
        collections.OrderedDict
    """
    response_dict = helpers.serialize_object(ws_response)
    print("[OK] - Web service response successfully transformed to python dictionary")
    logger.debug("[OK] - RESPONSE_TO_DICT")
    return response_dict

def get_ws_response(config_data, client, logger):
    """Gets the past 60 days toxicologic records from the mrSAT WebService as a zeep response object.

    Args:
        config_data (dict): config.json parameters.
        client (zeep.client.Client): zeep Client object.

    Returns:
        zeep.objects.WS_MRSNP.ExecuteResponse
    """
    ws_reponse = client.service.Execute(Usuario = config_data['webservice']['user'], 
                                        Password = config_data['webservice']['passwd'],
                                        Fechaconsulta = str(date.today()),
                                        Numerodias = 2)
    print("[OK] - Web Service response successfully gotten")
    logger.debug("[OK] - GET_WS_RESPONSE")
    return ws_reponse

def make_client(ws_url, session, settings, logger):
    """Generate the WebService client, based on the WS URL and the session and settings parameters.

    Args:
        ws_url (str): WebService URL.
        session (requests.sessions.Session): WebService client's session.
        settings(zeep.settings.Settings): WebService client's settings.

    Returns:
        zeep.client.Client
    """
    client = Client(ws_url, transport = Transport(session= session), settings= settings)
    print("[OK] - Web client successfully created")
    logger.debug("[OK] - MAKE_CLIENT")
    return client

def set_settings(logger):
    """Sets the WebService client's session settings.

    Returns:
        zeep.settings.Settings
    """
    settings = Settings(strict = False)
    print("[OK] - Web client settings successfully setted")
    logger.debug("[OK] - SET_SETTINGS")
    return settings

def set_user_and_psswd(session, config_data, logger):
    """Sets the session credentials, obtained from the config file.

    Args:
        session (requests.sessions.Session): Web Service client's session object
        config_data (dict): config.json parameters.
    """
    session.auth = (config_data['webservice']['user'], config_data['webservice']['passwd'])
    logger.debug("[OK] - SET_USER_AND_PSSWD")
    print("[OK] - User credentials successfully setted")

def generate_session(logger):
    """Generates the Web Service client's session object.

    Returns:
        requests.sessions.Session
    """
    session = Session()
    print("[OK] - Client's session successfully generated")
    logger.debug("[OK] - GENERATE_SESSION")
    return session

def generate_ws_url_string(config_data, logger):
    """Stores the WebService URL as a string, obtained from the config file.

    Args:
        config_data (dict): config.json parameters.

    Returns:
        str
    """
    ws_url = config_data['webservice']['url']
    print("[OK] - WebService URL string successfully generated")
    logger.debug("[OK] - GENERATE_WS_URL_STRING")
    return ws_url

def create_logger(log_file):
    """Creates a logger based on the passed log file.

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
    """Creates the log folder if not exists. Get the log file name.

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
    """Reads the config.json file.

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
    """Stores the input parameters.

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
    ws_url = generate_ws_url_string(config_data, logger)

    # Generate the client's session
    session = generate_session(logger)

    # Set the session's user and password
    set_user_and_psswd(session, config_data, logger)

    # Set the session's settings
    settings = set_settings(logger)

    # Make the webservice's consumer client
    client = make_client(ws_url, session, settings, logger)

    # Get the WebService response
    ws_response = get_ws_response(config_data, client, logger)

    # Transform the WebService response into a Python dictionary
    response_dict = response_to_dict(ws_response, logger)

    # Transform python dict into Pandas DataFrame
    df = dict_to_df(response_dict, logger)

    # Create string with the given database parameters
    db_connection = create_db_connection(config_data, logger)

    # Create sqlalchemy engine based on database parameters
    db_engine = create_db_engine(db_connection, logger)

    # Open the "delete_recent_records.sql" file
    sql_query = open_sql_query("delete_recent_records.sql", logger)

    # Delete the past 3 days records from the database table
    delete_old_records(db_engine, sql_query, logger)

    # Append the new records extracted from the WebServiceto the database table
    append_new_records(df, config_data, db_engine, logger)

    end = datetime.now()

    print("[OK] - New records successfully appended to " + config_data['sernapesca']['table'] + f" table. Time elapsed: {end - start}")

if __name__ == "__main__":
    main(sys.argv)
