from package.sql_data_operate import deal_sql_data as dsd
from package.db_connect import connect

def pushUserInfo():
    data = dsd.modifyUserInfo()
    data.to_sql("user_info", con=connect.mysql_engine(), if_exists="append", index=False)

pushUserInfo()