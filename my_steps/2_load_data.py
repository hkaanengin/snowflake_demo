from snowflake.snowpark import Session

USER_TABLES = ['user_detail']
USER_TABLES2 = ['user_detail']
FLIGHT_DETAILS = ['flight_details']
POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyalty']

TABLE_DICT = {
    "userdata1": {"schema": "RAW_USER", "tables": USER_TABLES},
    "userdata2": {"schema": "RAW_USER_2", "tables": USER_TABLES2},
    "flight_details": {"schema": "RAW_FLIGHT", "tables": FLIGHT_DETAILS},
    "pos": {"schema": "RAW_POS", "tables": POS_TABLES},
    "customer": {"schema": "RAW_CUSTOMER", "tables": CUSTOMER_TABLES}
}


def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    session.use_schema(schema)
    print(tname,s3dir, year, schema)
    if schema == 'RAW_POS' or schema == 'RAW_CUSTOMER':
        if year is None:
            location = "@external.frostbyte_raw_stage_orderdata/{}/{}".format(s3dir, tname)
        else:
            print('\tLoading year {}'.format(year)) 
            location = "@external.frostbyte_raw_stage_orderdata/{}/{}/year={}".format(s3dir, tname, year)
    else:
        location = "@external.frostbyte_raw_stage_userdata/{}".format(s3dir)
    
    df = session.read.option("compression", "snappy").parquet(location)
    df.copy_into_table("{}".format(tname))

def load_all_raw_tables(session):
    _ = session.sql("ALTER WAREHOUSE KEK_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    for s3dir, data in TABLE_DICT.items():
        table_names = data['tables']
        schema      = data['schema']
        for tname in table_names:
            print("Loading {}".format(tname))
            if tname in ['order_header', 'order_detail']:
                for year in ['2019', '2020', '2021']:
                    load_raw_table(session, tname=tname, s3dir=s3dir, year=year, schema=schema)
            
            load_raw_table(session, tname=tname, s3dir=s3dir, schema=schema)

    _ = session.sql("ALTER WAREHOUSE KEK_WH SET WAREHOUSE_SIZE = XSMALL").collect()


if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    import snowpark_utils

    session = snowpark_utils.get_snowpark_session()

    load_all_raw_tables(session)

    session.close()
