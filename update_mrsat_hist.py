import sys
import os
import logging
import json
import pandas as pd
from zeep import Client, Settings, helpers
from zeep.transports import Transport
from requests import Session
from requests.auth import HTTPBasicAuth
from sqlalchemy import inspect
from sqlalchemy import create_engine, text
from datetime import date, datetime


def append_missing_records(df, config_data, db_engine, logger):
    """Append the missing records to the mrsat historic table.

    Args:
        df (pandas.core.frame.DataFrame): Dataframe with the new records.
        config_data (dict): config.json parameters.
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        
    Raises:
        SAWarning: Did not recognize type 'geometry' of column 'geom'
    """
    try:
        df.to_sql(config_data['sernapesca']['mrsat_hist'], 
                    db_engine, 
                    if_exists = 'append', 
                    schema = config_data['sernapesca']['schema'], 
                    index = False)

        logger.debug("[OK] - APPEND_MISSING_RECORDS")

    except Exception as e:
        print("[ERROR] - Appending the mising records to the existing table")
        logger.error('[ERROR] - APPEND_MISSING_RECORDS')
        sys.exit(2)

def create_date_column(df, logger):
    df["FechaActualización"] = datetime.now()
    return df

def insert_id_column(df, id_column, logger):
    df.insert(loc = 0, column = 'ID', value = id_column)
    return df

def create_id_column(max_id, n_rows_ws, logger):
    id_column = list(range(max_id, max_id + n_rows_ws))
    return id_column

def get_df_n_rows(df, logger):
    n_rows_ws = df.shape[0]
    return n_rows_ws

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

def get_ws_response(config_data, client, missing_days, logger):
    """Gets the past 60 days toxicologic records from the mrSAT WebService as a zeep response object.

    Args:
        config_data (dict): config.json parameters.
        n_days: Number of days to query based on the existence of the table.
        client (zeep.client.Client): zeep Client object.

    Returns:
        zeep.objects.WS_MRSNP.ExecuteResponse
    """
    ws_reponse = client.service.Execute(Usuario = config_data['webservice']['user'], 
                                        Password = config_data['webservice']['passwd'],
                                        Fechaconsulta = str(date.today()),
                                        Numerodias = missing_days)
    print("[OK] - Web Service response successfully gotten. " + str(missing_days + 1) + " days queried.")
    logger.debug("[OK] - GET_WS_RESPONSE")
    return ws_reponse

def create_client(ws_url, session, settings, logger):
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
    logger.debug("[OK] - CREATE_CLIENT")
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

def get_missing_days(max_date, logger):
    date_today = date.today()
    missing_dates = (date_today - max_date).days - 1
    return missing_dates

def get_max_date(executed_query, logger):
    max_date = executed_query.fetchone()[0].date()
    return max_date

def get_max_id(executed_query, logger):
    max_id = executed_query.fetchone()[0] + 1
    return max_id

def execute_sql_query(db_con, sql_query, logger):
    executed_query = db_con.execute(sql_query)
    return executed_query

def generate_connection(db_engine, logger):
    db_con = db_engine.connect().execution_options(autocommit=True)
    return db_con


def open_sql_query(sql_file, config_data, logger):
    """Open the given SQL file.
    Returns:
        sqlalchemy.sql.elements.TextClause
    """
    schema = config_data['sernapesca']['schema']
    table = config_data['sernapesca']['mrsat_hist']

    with open("./sql_queries/" + sql_file, encoding = "utf8") as file:
        sql_query = text(file.read().format(schema, table))
    print("[OK] - SQL file successfully opened")
    logger.debug("[OK] - OPEN_SQL_QUERY")
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

    # Case if the DB is SQL Server
    if config_data['sernapesca']['db'] == 'mssql+pyodbc':
        db_connection = db_connection + '?driver=SQL Server'

    print("[OK] - Connection string successfully generated")
    logger.debug("[OK] - CREATE_DB_CONNECTION")
    return db_connection

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

    log_file = log_path + "/update_mrsat_hist.log"
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