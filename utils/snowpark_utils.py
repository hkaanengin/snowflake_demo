from snowflake.snowpark import Session
import os
from typing import Optional

# Class to store a singleton connection option
class SnowflakeConnection(object):
    _connection = None

    @property
    def connection(self) -> Optional[Session]:
        return type(self)._connection

    @connection.setter
    def connection(self, val):
        type(self)._connection = val

def get_snowpark_session() -> Session:
    # if running in snowflake
    if SnowflakeConnection().connection:
        session = SnowflakeConnection().connection

    elif os.path.exists(os.path.expanduser('~/.snowsql/config')):
        snowpark_config = get_snowsql_config()
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()

    elif "SNOWSQL_ACCOUNT" in os.environ:
        from dotenv import load_dotenv

        load_dotenv()
        snowpark_config = {
            "account": os.getenv("accountname"),
            "user": os.getenv("username"),
            "password": os.getenv("password"),
            "role": os.getenv("dbname"),
            "warehouse": os.getenv("warehousename"),
            "database": os.getenv("rolename"),
            "schema": os.getenv("schema")
        }
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()

    if SnowflakeConnection().connection:
        return SnowflakeConnection().connection
    else:
        raise Exception("Unable to create a Snowpark session")


def get_snowsql_config(
    connection_name: str = 'kek',
    config_file_path: str = os.path.expanduser('~/.snowsql/config'),
) -> dict:
    import configparser

    snowsql_to_snowpark_config_mapping = {
        'account': 'account',
        'accountname': 'account',
        'username': 'user',
        'password': 'password',
        'rolename': 'role',
        'warehousename': 'warehouse',
        'dbname': 'database',
        'schemaname': 'schema'
    }
    try:
        config = configparser.ConfigParser(inline_comment_prefixes="#")
        connection_path = 'connections.' + connection_name

        config.read(config_file_path)
        session_config = config[connection_path]
        # Convert snowsql connection variable names to snowcli ones
        session_config_dict = {
            snowsql_to_snowpark_config_mapping[k]: v.strip('"')
            for k, v in session_config.items()
        }
        return session_config_dict
    except Exception:
        raise Exception(
            "Error getting snowsql config details"
        )
