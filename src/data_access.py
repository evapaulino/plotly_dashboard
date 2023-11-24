
"""
helper functions for data access for BesserEsser dashboard
"""


from sqlalchemy import create_engine
import psycopg2                        # pip install psycopg2-binary
from dotenv import dotenv_values       # pip install python-dotenv
from pandas import read_sql_query
from constants import DEBUG, ERR_PREFIX, DRIVER


def make_sqlalchemy_engine():
    """
    TODO: docs

    Undertakes 3 approaches to connect (in try-except blocks)
    try these maybe:
    "postgresql://user:pass@hostname/dbname"
    "postgresql+psycopg2://user:pass@hostname/database"

    also try:
    $ pip install snowflake-sqlalchemy
    $ pip install psycopg2-binary
    import psycopg2 
    """

    # What is your driver?
    DRIVER # is imported from constants.py
    # Change the DRIVER name in constants.py or transfer its definition here

    # SQL datbase credentials - define constants as they appear in your .env file
    HOST = 'host'
    PORT = 'port'
    DATABASE = 'database'      # maybe 'dbname' ?
    USER = 'user'              # maybe 'username' ?
    PASSWORD = 'password'      # maybe 'pass' ?

    env_dict = dotenv_values(".env")
    keys = [HOST, PORT, DATABASE, USER, PASSWORD]
    connect_args = {key: env_dict[key] for key in keys if key in env_dict}

    # Have the credentials been loaded?
    if not env_dict:
        raise FileNotFoundError(f"{ERR_PREFIX}SQL database credentials haven't been loaded")

    # Attempt 1
    try:
        engine = create_engine(f"{DRIVER}://user:pass@hostname/database", connect_args=connect_args)
        read_sql_query("SELECT 1;", con=engine)
    except: # all kinds of errors = bad practice but ...
        engine = None
    else:
        return engine

    #Attempt 2
    connect_args["dbname"] = connect_args[DATABASE]  # replace the key-name 'database' with 'dbname'
    del connect_args[DATABASE]                       # solution for the error from sqlalchemy   

    try:
        engine = create_engine(f"{DRIVER}://user:pass@hostname/database", connect_args=connect_args)
        read_sql_query("SELECT 1;", con=engine)
    except: # all kinds of errors = bad practice but ...
        engine = None
    else:
        return engine

    # Attempt 3
    try:
        from sqlalchemy import URL
    except ImportError:
        raise ImportError((f"{ERR_PREFIX}Undertook 3 different approaches to connect to your database. "
                           f"The last one implied importing the 'URL' class from sqlalchemy and it didn't work."))

    url_object = URL.create(
        DRIVER,             
        username=env_dict[USER],
        password=env_dict[PASSWORD],
        host=env_dict[HOST],
        database=env_dict[DATABASE])

    # no try-except block here to show the original error message from sqlalchemy if an Exception happens.
    return create_engine(url_object)


