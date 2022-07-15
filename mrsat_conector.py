import sys
import os
import logging
import json
import pandas as pd
from zeep import Client, Settings, helpers
from zeep.transports import Transport
from requests import Session
from sqlalchemy import inspect
from sqlalchemy import create_engine, text
from datetime import date, datetime

def append_new_records(df, config_data, db_engine, n_days, config_table, logger):
    """Append the records from the past 2 days to the database tables.

    Args:
        df (pandas.core.frame.DataFrame): Dataframe with the new records.
        config_data (dict): config.json parameters.
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        n_days (int): Number of days queried to the WebService.
        table (str): Name of the table to use on the query, based on the config.json table name.
        
    Raises:
        SAWarning: Did not recognize type 'geometry' of column 'geom'
    """
    table = config_data['sernapesca'][config_table]
    schema = config_data['sernapesca']['schema']
    
    try:
        print("[LOADING] - Appending new records to " + table  + " table")
        df.to_sql(table, 
                    db_engine, 
                    if_exists = 'append', 
                    schema = schema, 
                    index = False)

        # Case if the table previously exists
        if n_days == 2:
            print("[OK] - new records successfully appended to " + table + " table")
        
        logger.debug("[OK] - APPEND_NEW_RECORDS")

    except Exception as e:
        print(e)
        print("[ERROR] - Appending the new records to the existing table")
        logger.error('[ERROR] - APPEND_NEW_RECORDS')
        sys.exit(2)

def create_date_column(df, logger):
    """Inserts column with the current datetime to the WS DataFrame.

    Args:
        df (pandas.core.frame.DataFrame): WebService response DataFrame.

    Returns:
        pandas.core.frame.DataFrame
    """

    df["FechaActualización"] = datetime.now()
    print("[OK] - Column of dates successfully inserted to the WS DataFrame")
    logger.debug("[OK] - CREATE_DATE_COLUMN")
    return df

def execute_sql_query(db_con, sql_query, logger):
    """Executes the given SQL query and stores the result as a sqlalchemy Cursor.
    
    Args:
        db_con (sqlalchemy.engine.base.Connection): Database connection.
        sql_query (sqlalchemy.sql.elements.TextClause): SQl query as sqlalchmey Text Clause.
    
    Returns:
        sqlalchemy.engine.cursor.LegacyCursorResult
    """
    try:
        executed_query = db_con.execute(sql_query)
        print("[OK] - SQL query successfully executed")
        logger.debug("[OK] - EXECUTE_SQL_QUERY")
        return executed_query

    except Exception as e:
        print("[ERROR] - Executing the SQL query")
        print(e)
        logger.error('[ERROR] - EXECUTE_SQL_QUERY')
        sys.exit(2)


def begin_connection(db_con, logger):
    """Begin a transaction to execute DELETE querys.

    Args:
        db_con(sqlalchemy.engine.base.Connection): Database connection.
    """
    trans = db_con.begin()
    print("[OK] - Transaction commit generated")
    logger.debug("[OK] - BEGIN_CONNECTION")
    return trans

def generate_connection(db_engine, logger):
    """Connects to the given sqlalchemy database engine.
    
    Args:
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
    
    Returns:
        sqlalchemy.engine.base.Connection
    """
    try:
        db_con = db_engine.connect().execution_options(autocommit=False)
        print("[OK] - Successfully connected to the database engine")
        logger.debug("[OK] - GENERATE_CONNECTION")
        return db_con

    except Exception as e:
        print("[ERROR] - Connecting to the database engine")
        print(e)
        logger.error('[ERROR] - GENERATE_CONNECTION')
        sys.exit(2)

def open_sql_query(sql_file, config_data, config_table, logger):
    """Opens the given SQL file.
    
    Args:
        sql_file (str): Name of the .sql file that contains the query.
        config_data (dict): config.json parameters.
        config_table (str): Name of the table to use on the query, based on the confif.json table name.
    
    Returns:
        sqlalchemy.sql.elements.TextClause
    """
    try:
        table = config_data['sernapesca'][config_table]
        schema = config_data['sernapesca']['schema']

        with open("./sql_queries/" + sql_file, encoding = "utf8") as file:
            # Formats the query based on the parameters 
            formatted_file = file.read().format(schema, table)
            sql_query = text(formatted_file)
        print("[OK] - SQL file successfully opened")
        logger.debug("[OK] - OPEN_SQL_QUERY")
        return sql_query

    except Exception as e:
        print("[ERROR] - Opening SQL file")
        print(e)
        logger.error('[ERROR] - OPEN_SQL_QUERY')
        sys.exit(2)


def insert_id_column(df, id_column, logger):
    """Inserts the list of IDs to the WS DataFrame.

    Args:
        df (pandas.core.frame.DataFrame): WebService response DataFrame.
        id_column (list): List of missing ID's

    Returns:
        pandas.core.frame.DataFrame
    """
    df.insert(loc = 0, column = 'ID', value = id_column)
    print("[OK] - Column of IDs successfully inserted to the DataFrame")
    logger.debug("[OK] - INSERT_ID_COLUMN")
    return df

def create_id_column(max_id, n_rows_ws, logger):
    """Creates columns of ID based on the maximum ID obtained from the database table and the number of 
    records from the WS response.

    Args:
        max_id (int): Maximum ID obtained from the database table.
        n_rows_ws (int): Number of records from the WebService response.

    Returns:
        list
    """

    id_column = list(range(max_id, max_id + n_rows_ws))
    id_column.reverse()
    print("[OK] - ID columns successfully created")
    logger.debug("[OK] - CREATE_ID_COLUMN")
    return id_column

def get_df_n_rows(df, logger):
    """Gets the number of rows of the WS query DataFrame.

    Args:
        df (pandas.core.frame.DataFrame): mrSAT WebService response DataFrame.

    Returns:
        int
    """

    n_rows_ws = df.shape[0]
    print("[OK] - WebService's DataFrame number of rows successfully calculated")
    logger.debug("[OK] - GET_DF_N_ROWS")
    return n_rows_ws

def get_max_id(executed_query, logger):
    """Extracts the maximum ID number from the table stored on the database.
    
    Args:
        executed_query (sqlalchemy.engine.cursor.LegacyCursorResult): 'get_max_id.sql' executed query.
    
    Returns:
        int
    """
    max_id = executed_query.fetchone()[0] + 1
    print("[OK] - Database table's maximum ID successfully obtained")
    logger.debug("[OK] - GET_MAX_ID")
    return max_id

def dict_to_df(response_dict, logger):
    """Transforms the Python dict to a pandas DataFrame.

    Args:
        response_dict (collections.OrderedDict): past 60 days toxicologic records from the mrSAT WebService.

    Returns:
        pandas.core.frame.DataFrame
    """
    
    # Total records queried to the Web Service
    total_records = response_dict['Totregistros']
    
    if total_records > 0:    
        response = response_dict["Sdtsnp"]["SDTSNP.SDTSNPItem"]
        df = pd.DataFrame(response)
        df.sort_values('FechaExtraccion', ascending=False)
        print("[OK] - Python dictionary successfully transformed to pandas DataFrame. " + str(total_records) + " total records.")
        logger.debug("[OK] - DICT_TO_DF")
        return df

    else:
        print("[WARNING] - There are no records for the consulted days")
        logger.debug("[WARNING] - DICT_TO_DF")
        sys.exit(2)

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

def get_ws_response(config_data, client, n_days, logger):
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
                                        Numerodias = n_days)
    print("[OK] - Web Service response successfully gotten. " + str(n_days) + " days queried.")
    logger.debug("[OK] - GET_WS_RESPONSE")
    return ws_reponse

def check_if_table_exists(db_engine, config_data, logger):
    """Check if the recent records table exists on the database. If exists, return number of days to query, else, exit the code. 

    Args:
        db_engine (sqlalchemy.engine.base.Engine): Database sqlalchemy engine.
        config_data (dict): config.json parameters.

    Returns:
        int
    """
    table = config_data['sernapesca']['last_days_table']
    schema = config_data['sernapesca']['schema']
    
    table_check = inspect(db_engine).has_table(table, schema)
    
    if table_check:
        n_days = 2

    else:
        print("[OK] - Recent records table doesn't exists. Check the installation manual to proceed")
        logger.error("[ERROR] - CHECK_IF_TABLE_EXISTS - RECENT RECORDS TABLE DOESN'T EXISTS")
        sys.exit(2)

    print("[OK] - Recent records table successfully checked")
    logger.debug("[OK] - CHECK_IF_TABLE_EXISTS")
    return n_days

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

def create_db_engine(db_connection, logger):
    """Creates a sqlalchemy database engine based on the database connection string.

    Args:
        db_connection (str): string with the databse connection.

    Returns:
        sqlalchemy.engine.base.Engine
    """
    try:

        conn_args={
		"TrustServerCertificate": "yes",
                "Echo": "True",
                "MARS_Connection": "yes"
	}
        db_engine = create_engine(db_connection, connect_args=conn_args)
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
    if config_data['sernapesca']['db_type'] == 'mssql+pyodbc':
        db_connection = db_connection + '?driver=ODBC+Driver+17+for+SQL+Server'

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

    logger = logging.getLogger('requests')
    logger = logging.getLogger("zeep").propagate = False
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

    # Gets parameters
    config_filepath = get_parameters(argv)

    # Gets service config parameters
    config_data = get_config(config_filepath)

    # Create the log file if not exists
    log_file = create_log_file(config_data["log_path"])

    # Creates the logger
    logger = create_logger(log_file)

    # Creates string with the given database parameters
    db_connection = create_db_connection(config_data, logger)

    # Creates sqlalchemy engine based on database parameters
    db_engine = create_db_engine(db_connection, logger)
    
    # Gets the web service URL
    ws_url = generate_ws_url_string(config_data, logger)

    # Generates the client's session
    session = generate_session(logger)

    # Sets the session's user and password
    set_user_and_psswd(session, config_data, logger)

    # Sets the session's settings
    settings = set_settings(logger)

    # Creates the webservice's consumer client
    client = create_client(ws_url, session, settings, logger)

    # Sets the number of days to query based on the existence of the recent records table
    n_days = check_if_table_exists(db_engine, config_data, logger)
    
    # Gets the WebService response
    ws_response = get_ws_response(config_data, client, n_days, logger)

    # Transforms the WebService response into a Python dictionary
    response_dict = response_to_dict(ws_response, logger)

    # Transforms python dict into Pandas DataFrame
    recent_df = dict_to_df(response_dict, logger)
    historic_df = dict_to_df(response_dict, logger)

    # Creates date column
    recent_df = create_date_column(recent_df, logger)
    historic_df = create_date_column(historic_df, logger)

    # Opens the SQL files
    delete_recent_60days = open_sql_query("delete_recent_records.sql", config_data, 'last_days_table', logger)
    delete_recent_historic= open_sql_query("delete_recent_records.sql", config_data, 'historic_table', logger)
    delete_oldest_60days = open_sql_query("delete_old_records.sql", config_data, 'last_days_table', logger)

    # Generates database connection
    db_con = generate_connection(db_engine, logger)

    # Begin transaction block
    trans = begin_connection(db_con, logger)
    
    # Deletes the past 2 days records from the database tables
    execute_sql_query(db_con, delete_recent_60days, logger)
    execute_sql_query(db_con, delete_recent_historic, logger)

    # Commit the DELETE querys to make changes on the DB
    trans.commit()

    # Opens the 'get_max_id.sql' file for historic and recent table 
    historic_id_query = open_sql_query("get_max_id.sql", config_data, 'historic_table', logger)
    recent_id_query = open_sql_query("get_max_id.sql", config_data, 'last_days_table', logger)

    # Execute the SQL query
    executed_historic_id_query = execute_sql_query(db_con, historic_id_query, logger)
    executed_recent_id_query = execute_sql_query(db_con, recent_id_query, logger)

    # Gets the maximum ID from the database table
    historic_max_id = get_max_id(executed_historic_id_query, logger)
    recent_max_id = get_max_id(executed_recent_id_query, logger)

    # Get the number of rows of the queried WS pandas DataFrame
    historic_n_rows = get_df_n_rows(historic_df, logger)
    recent_n_rows = get_df_n_rows(recent_df, logger)

    # Create columns with the ID's
    historic_id_column = create_id_column(historic_max_id, historic_n_rows, logger)
    recent_id_column = create_id_column(recent_max_id, recent_n_rows, logger)

    # Insert the ID column into the DataFrames
    historic_df = insert_id_column(historic_df, historic_id_column, logger)
    recent_df = insert_id_column(recent_df, recent_id_column, logger)

    # Appends the new records extracted from the WebService to the DB tables
    append_new_records(historic_df, config_data, db_engine, n_days, 'historic_table', logger)
    append_new_records(recent_df, config_data, db_engine, n_days, 'last_days_table', logger)

    # Begin transaction block
    trans = begin_connection(db_con, logger)
    
    # Delete the oldest records on the last_days_table
    execute_sql_query(db_con, delete_oldest_60days, logger)

    # Commit the APPEND to make changes on the DB
    trans.commit()

    end = datetime.now()

    print(f"[OK] - Script successfully executed. Time elapsed: {end - start}")

if __name__ == "__main__":
    main(sys.argv)
