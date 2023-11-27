from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from datetime import datetime, timedelta


def create_user_view(session):
    session.use_schema('HARMONIZED')

    user_details = session.table("RAW_USER.USER_DETAIL")
    user_details2 = session.table("RAW_USER_2.USER_DETAIL")

    p_date = (datetime.now() -timedelta(days=5)).strftime("%Y-%m-%d")
    p_time = datetime.now().strftime("%H:%M:%S")
    
    user_details_union_df = user_details.union(user_details2)

    user_details_union_df = user_details_union_df.select(F.col('"first_name"'), \
                                                        F.col('"last_name"'), \
                                                        F.col('"email"'), \
                                                        F.col('"gender"'), \
                                                        F.col('"ip_address"'), \
                                                        F.col('"country"'), \
                                                        F.col('"birthdate"'), \
                                                        F.col('"salary"'), \
                                                        F.col('"title"'), \
                                                        F.col('"comments"'), \
                                                        F.col('"registration_dttm"')
                                                        )
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"first_name"'), "FIRST_NAME")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"last_name"'), "LAST_NAME")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"email"'), "EMAIL")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"gender"'), "GENDER")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"ip_address"'), "IP_ADDRESS")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"country"'), "COUNTRY")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"birthdate"'), "BIRTHDATE")
    user_details_union_df = user_details_union_df.with_column('"salary"', F.col('"salary"').cast("int")).with_column_renamed(F.col('"salary"'), "SALARY")

    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"title"'), "TITLE")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"comments"'), "COMMENTS")
    user_details_union_df = user_details_union_df.with_column_renamed(F.col('"registration_dttm"'), "REGISTRATION_DTTM")

    user_details_union_df = user_details_union_df.select(F.col("FIRST_NAME"), \
                                                        F.col("LAST_NAME"), \
                                                        F.col("EMAIL"), \
                                                        F.col("GENDER"), \
                                                        F.col("IP_ADDRESS"), \
                                                        F.col("COUNTRY"), \
                                                        F.col("BIRTHDATE"), \
                                                        F.call_udf("ANALYTICS.SALARY_TO_TL", F.col("SALARY")), \
                                                        F.col("TITLE"), \
                                                        F.col("COMMENTS"), \
                                                        F.col("REGISTRATION_DTTM")
                                                        )
    
    user_details_union_df = user_details_union_df.with_column("p_date", F.lit(p_date))
    user_details_union_df = user_details_union_df.with_column("p_time", F.lit(p_time))
    
    user_details_union_df.create_or_replace_view('UNION_USER_DETAIL')

    user_details_union_df.show(10)

def create_pos_view(session):
    session.use_schema('HARMONIZED')

    order_detail = session.table("RAW_POS.ORDER_DETAIL")
    order_header = session.table("RAW_POS.ORDER_HEADER")
    truck = session.table("RAW_POS.TRUCK")
    menu = session.table("RAW_POS.MENU")
    franchise = session.table("RAW_POS.FRANCHISE")
    location = session.table("RAW_POS.LOCATION")

    t_with_f = truck.join(franchise, truck['FRANCHISE_ID'] == franchise['FRANCHISE_ID'], rsuffix='_f')
    oh_w_t_and_l = order_header.join(t_with_f, order_header['TRUCK_ID'] == t_with_f['TRUCK_ID'], rsuffix='_t') \
                                .join(location, order_header['LOCATION_ID'] == location['LOCATION_ID'], rsuffix='_l')
    final_df = order_detail.join(oh_w_t_and_l, order_detail['ORDER_ID'] == oh_w_t_and_l['ORDER_ID'], rsuffix='_oh') \
                            .join(menu, order_detail['MENU_ITEM_ID'] == menu['MENU_ITEM_ID'], rsuffix='_m')
    final_df = final_df.select(F.col("ORDER_ID"), \
                            F.col("TRUCK_ID"), \
                            F.col("ORDER_TS"), \
                            F.to_date(F.col("ORDER_TS")).alias("ORDER_TS_DATE"), \
                            F.col("ORDER_DETAIL_ID"), \
                            F.col("LINE_NUMBER"), \
                            F.col("TRUCK_BRAND_NAME"), \
                            F.col("MENU_TYPE"), \
                            F.col("PRIMARY_CITY"), \
                            F.col("REGION"), \
                            F.col("COUNTRY"), \
                            F.col("FRANCHISE_FLAG"), \
                            F.col("FRANCHISE_ID"), \
                            F.col("FIRST_NAME").alias("FRANCHISEE_FIRST_NAME"), \
                            F.col("LAST_NAME").alias("FRANCHISEE_LAST_NAME"), \
                            F.col("LOCATION_ID"), \
                            F.col("MENU_ITEM_ID"), \
                            F.col("MENU_ITEM_NAME"), \
                            F.col("QUANTITY"), \
                            F.col("UNIT_PRICE"), \
                            F.col("PRICE"), \
                            F.col("ORDER_AMOUNT"), \
                            F.col("ORDER_TAX_AMOUNT"), \
                            F.col("ORDER_DISCOUNT_AMOUNT"), \
                            F.col("ORDER_TOTAL"))
    final_df.create_or_replace_view('POS_FLATTENED_V')



def create_pos_view_stream(session):
    session.use_schema('HARMONIZED')
    _ = session.sql('CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM \
                        ON VIEW POS_FLATTENED_V \
                        SHOW_INITIAL_ROWS = TRUE').collect()

if __name__ == "__main__":
    import snowpark_utils
    from datetime import datetime, timedelta
    
    session = snowpark_utils.get_snowpark_session()

    create_user_view(session)
    create_pos_view(session)
    create_pos_view_stream(session)

    session.close()
