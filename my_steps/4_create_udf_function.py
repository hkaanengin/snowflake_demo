from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType, DoubleType

def create_udf_func(session):
    # convert_to_tr = udf(lambda x: x * 28, return_type= IntegerType(), input_types=IntegerType(), name="usd_to_tl", replace=True)
    # session.udf(convert_to_tr)
    session.use_schema('ANALYTICS')

    add_one = udf(lambda x: x+1, return_type=IntegerType(), input_types=[IntegerType()], name="my_udf", replace=True)
    udf(name="minus_one", is_permanent=True, stage_location="@my_stage", replace=True)

def create_udtf_func(session):
    # convert_to_tr = udf(lambda x: x * 28, return_type= IntegerType(), input_types=IntegerType(), name="usd_to_tl", replace=True)
    # session.udf(convert_to_tr)
    session.use_schema('ANALYTICS')

    add_one = udf(lambda x: x+1, return_type=IntegerType(), input_types=[IntegerType()], name="my_udf", replace=True)
    udf(name="minus_one", is_permanent=True, stage_location="@my_stage", replace=True)


if __name__ == "__main__":
    import snowpark_utils
    
    session = snowpark_utils.get_snowpark_session()

    create_udf_func(session)

    session.close()
