
'''
The purpose of this module is to interact with Snowflake database:
submitting query either directly from python or using dbt
'''

import logging
import os
from typing import Any, Mapping, Sequence
import pandas as pd
import snowflake.connector
import sqlglot
from snowflake.connector.connection import SnowflakeConnection
from sqlglot import exp

from ..config import config_decorators
from ..config.config_variables import config_global_variables as var

logging.basicConfig(level=logging.INFO)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("sqlglot").setLevel(logging.ERROR)
current_snowflake_connection = None

@config_decorators.exit_program(log_filter=lambda args: {})
@config_decorators.retry_function(log_filter=lambda args: {})
def snowflake_connect(sr_snowflake_account: pd.Series) -> SnowflakeConnection:

    """
        The purpose of this function is to connect to Snowflake using the connector (if not already connected)
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to run a query
        Returns:
            the snowflake connection object with which we can run query
        Raises:
            Retry 3 times and exits the program if error connecting (with decorators)
    """

    global current_snowflake_connection  # Reference the global variable

    #We get environment keys (GitHub secrets)
    SNOWFLAKE_USERNAME = os.getenv('SNOWFLAKE_USERNAME')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')

    #if we are already connected no need to do it again
    if not(current_snowflake_connection and not current_snowflake_connection.is_closed()):
        if os.getenv("IS_TESTRUN") == '0':
            database = sr_snowflake_account['DATABASE_PROD']
        else:
            database = sr_snowflake_account.at['DATABASE_TEST']

        current_snowflake_connection = snowflake.connector.connect(
            user = SNOWFLAKE_USERNAME,
            password= SNOWFLAKE_PASSWORD,
            account=sr_snowflake_account['ACCOUNT'],
            warehouse=sr_snowflake_account['WAREHOUSE'],
            database=database,
            schema=var.LANDING_DATABASE_SCHEMA,
            role=var.ROLE_DATABASE,
            login_timeout=var.SNOWFLAKE_LOGIN_WAIT_TIME
        )
        logging.info("SNOWFLAKE -> CONNECTED")
    return current_snowflake_connection

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('query','params') })
@config_decorators.retry_function(log_filter=lambda args: {k: args[k] for k in ('query','params') })
def snowflake_execute(sr_snowflake_account: pd.Series, query: str, db_placeholder: str, params: Sequence[Any] | Mapping[str, Any] | None =None) -> pd.DataFrame | list | None:

    """
        The purpose of this function is to:
        - personalize a snowflake query 
        - run it
        - calculate the related dataframe if it is a select query
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            query (str): The query we want to run
            db_placeholder (str): to replace constant in the query string
            params (list): list of parameters to personalize the query with
        Returns:
            - if the query is a select query then return the dataframe related 
            - if the query is a show query then return a list
            - else return None
        Raises:
            Retry 3 times and exits the program if error executing or parsing the query (with decorators)
    """
    
    #We connect to Snowflake
    snowconnect = snowflake_connect(sr_snowflake_account)
    
    #We personalized #DATABASE# and run the query
    if os.getenv("IS_TESTRUN") == '0':
        database = sr_snowflake_account['DATABASE_PROD']
    else:
        database = sr_snowflake_account['DATABASE_TEST']
    with snowconnect.cursor() as snowCursor:
        query_personalized = query.replace(db_placeholder,database)
        snowCursor.execute(query_personalized,params)

        #We parse the query using sqlglot to possibly return the related dataframe/list
        query_root = sqlglot.parse_one(query_personalized.replace("%s", "NULL"), read="snowflake")
        if isinstance(query_root, exp.With):
            query_root = query_root.this

        #If it doesn't a sql keyword we raise an error
        if not hasattr(query_root, "key"):
            raise ValueError("The sql is not valid (doesn't have a keyword)")
        #If it is a select query we return the associated dataframe
        if query_root.key.upper() == "SELECT":
            df = snowCursor.fetch_pandas_all()
            return df
        #If it is a show query we return the associated list
        if query_root.key.upper() == "SHOW":
            lst = snowCursor.fetchall()
            return lst

@config_decorators.exit_program(log_filter=lambda args: {})
@config_decorators.retry_function(log_filter=lambda args: {})
def snowflake_execute_script(sr_snowflake_account: pd.Series, script: str, db_placeholder: str):

    """
        The purpose of this function is to:
        - personalize "#DATABASE#" in a snowflake list of queries gathered in a script
        - run them
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            script (str): A set of queries gathered in a script
            db_placeholder (str): to replace constant in the query string
        Raises:
            Retry 3 times and exits the program if error executing the script (with decorators)
    """
    
    #We connect to Snowflake
    snowconnect = snowflake_connect(sr_snowflake_account)
    
    #We personalized #DATABASE# and run the query
    if os.getenv("IS_TESTRUN") == '0':
        database = sr_snowflake_account['DATABASE_PROD']
    else:
        database = sr_snowflake_account['DATABASE_TEST']
    script_personalized = script.replace(db_placeholder,database)
    snowconnect.execute_string(script_personalized)