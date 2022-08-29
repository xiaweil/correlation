# coding:utf8
from package.sql_data_operate import deal_sql_data as dsd
from package.db_connect import connect
from package.develop_score import output_result as ore


# 保存userInfo到数据库
def pushUserInfo():
    data = dsd.modifyUserInfo()
    data.to_sql("user_info", con=connect.mysql_engine(), if_exists="append", index=False)


def pushIndustrialScore():
    data, areaData = ore.outputIndustryScores()
    data.to_sql("evaluation_industrial", con=connect.mysql_engine(), if_exists="append", index=False)
    areaData.to_sql("evaluation_regional", con=connect.mysql_engine(), if_exists="append", index=False)


def pushIndustryTrend():
    data = ore.outputSeasonScores()
    data.to_sql("industry_trend", con=connect.mysql_engine(), if_exists="append", index=False)

def pushYearIndustryTrend():
    data = ore.outputYearScores()
    data.to_sql("industry_trend", con=connect.mysql_engine(), if_exists="append", index=False)

def pushCreativityData():
    data = dsd.concatCreatityType()
    data.to_sql("creativity_type", con=connect.mysql_engine(), if_exists="append", index=False)

# pushUserInfo()
# pushIndustrialScore()
# pushYearIndustryTrend()

# pushCreativityData()