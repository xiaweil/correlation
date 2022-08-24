from package.sql_data_operate import deal_sql_data as dsd
from package.db_connect import connect
from package.develop_score import output_result as ore
def pushUserInfo():
    data = dsd.modifyUserInfo()
    data.to_sql("user_info", con=connect.mysql_engine(), if_exists="append", index=False)

def pushIndustrialScore():
    data = ore.output_scores()
    data.to_sql("evaluation_industrial", con=connect.mysql_engine(), if_exists="append", index=False)

# pushUserInfo()
pushIndustrialScore()