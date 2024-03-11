import time
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''): #Checks if the table exists. Returns True if it does.
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_orders_table(session):
    _ = session.sql("CREATE TABLE HARMONIZED.ORDERS LIKE HARMONIZED.POS_FLATTENED_V").collect()
    _ = session.sql("ALTER TABLE HARMONIZED.ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def create_orders_stream(session):
    _ = session.sql("CREATE STREAM HARMONIZED.ORDERS_STREAM ON TABLE HARMONIZED.ORDERS").collect()

def merge_order_updates(session):
    _ = session.sql('ALTER WAREHOUSE KEK_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    source = session.table('HARMONIZED.POS_FLATTENED_V_STREAM')
    target = session.table('HARMONIZED.ORDERS')

    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    target.merge(source, target['ORDER_DETAIL_ID'] == source['ORDER_DETAIL_ID'], \
                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    _ = session.sql('ALTER WAREHOUSE KEK_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    
    if not table_exists(session, schema='HARMONIZED', name='ORDERS'):
        create_orders_table(session)
        create_orders_stream(session)
    merge_order_updates(session)

    return f"Successfully processed ORDERS"


if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    import utils.snowpark_utils as snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
