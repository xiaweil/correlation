# coding:utf8
import pandas as pd
import numpy as np
from package.sql_data_operate import deal_sql_data as dsd
from package.db_connect import connect
from package.develop_score import output_result as ore
from package.sql_data_operate import pull_sql_data as psd
from package.db_connect.connect import operateSqlData
from package.db_connect.base import User
from memory_profiler import profile

# 保存userInfo到数据库
@operateSqlData
def pushUserInfo(session):
    data = dsd.modifyUserInfo()
    data.where(data.notnull(), np.NAN)
    data["is_core"] = data["is_core"].astype(int)
    userInfo = psd.getUserInfo()
    userInfo = userInfo.iloc[:, 1:]
    existUser = userInfo["user_code"]
    data.loc[~(data["user_code"].isin(existUser)), "updateCode"] = 2
    res = data[data["updateCode"].isnull()]
    res = res.where(res.notnull(), None)
    for i in range(res.shape[0]):
        userId = res.loc[i, "user_code"]
        originUser = userInfo[userInfo["user_code"] == userId]
        if res.iloc[i, :-1].equals(originUser.iloc[0, :]):
            res.iloc[i, -1] = 0
            continue
        res.iloc[i, -1] = 1

        """
        # sql = f"update user_info set user_name = \"{res.iloc[i, 1]}\", sector =\"{res.iloc[i, 2]}\", address=\"{res.iloc[i, 3]}\"," \
        #       f"branch=\"{res.iloc[i, 4]}\", center = \"{res.iloc[i, 5]}\", voltage_level=\"{res.iloc[i, 6]}\", user_type=\"{res.iloc[i, 7]}\", " \
        #       f"district = \"{res.iloc[i, 8]}\", lon = \"{res.iloc[i, 9]}\", lat=\"{res.iloc[i, 10]}\", std_industry_name=\"{res.iloc[i, 11]}\", " \
        #       f"std_industry_id=\"{res.iloc[i, 12]}\", company_nature=\"{res.iloc[i, 13]}\", is_core=\"{res.iloc[i, 14]}\", " \
        #       f"key_industry_id = \"{res.iloc[i, 16]}\", build_date=\"{res.iloc[i, 17]}\" where user_code=\"{res.iloc[i, 0]}\""
         session.execute(sql)
        """

        user_info = session.query(User).filter(User.user_code == res.iloc[i, 0]).first()
        if pd.isnull(res.iloc[i, 1]) == False:
            user_info.user_name = res.iloc[i, 1]
        else:
            user_info.user_name = None

        if pd.isnull(res.iloc[i, 2]) == False:
            user_info.sector = res.iloc[i, 2]
        else:
            user_info.sector = None

        if pd.isnull(res.iloc[i, 3]) == False:
            user_info.address = res.iloc[i, 3]
        else:
            user_info.address = None

        if pd.isnull(res.iloc[i, 4]) == False:
            user_info.branch = res.iloc[i, 4]
        else:
            user_info.branch = None

        if pd.isnull(res.iloc[i, 5]) == False:
            user_info.center = res.iloc[i, 5]
        else:
            user_info.center = None

        if pd.isnull(res.iloc[i, 6]) == False:
            user_info.voltage_level = res.iloc[i, 6]
        else:
            user_info.voltage_level = None

        if pd.isnull(res.iloc[i, 7]) == False:
            user_info.user_type = res.iloc[i, 7]
        else:
            user_info.user_type = None

        if pd.isnull(res.iloc[i, 8]) == False:
            user_info.district = res.iloc[i, 8]
        else:
            user_info.district = None

        if pd.isnull(res.iloc[i, 9]) == False:
            user_info.lon = res.iloc[i, 9]
        else:
            user_info.lon = None

        if pd.isnull(res.iloc[i, 10]) == False:
            user_info.lat = res.iloc[i, 10]
        else:
            user_info.lat = None

        if pd.isnull(res.iloc[i, 11]) == False:
            user_info.std_industry_name = res.iloc[i, 11]
        else:
            user_info.std_industry_name = None

        if pd.isnull(res.iloc[i, 12]) == False:
            user_info.std_industry_id = res.iloc[i, 12]
        else:
            user_info.std_industry_id = None

        if pd.isnull(res.iloc[i, 13]) == False:
            user_info.company_nature = res.iloc[i, 13]
        else:
            user_info.company_nature = None

        if pd.isnull(res.iloc[i, 14]) == False:
            user_info.is_core = res.iloc[i, 14]
        else:
            user_info.is_core = None

        if pd.isnull(res.iloc[i, 15]) == False:
            user_info.key_industry_id = res.iloc[i, 15]
        else:
            user_info.key_industry_id = None

        if pd.isnull(res.iloc[i, 16]) == False:
            user_info.build_date = res.iloc[i, 16]
        else:
            user_info.build_date = None

        session.commit()

        """
        # for j in range(userInfo.shape[1]-1):
        #
        #     print(res.iloc[i, j])
        #     print(originUser.iloc[0, j+1])
        #     # print(res.iloc[i, j]==originUser.iloc[0, j+1])
        #     # if res.iloc[i, j]==np.nan and originUser.iloc[0, j+1]==np.nan and res.iloc[i, j].equals(originUser.iloc[0, j+1]):
        #     #     print()
        #     #     continue
        #     if res.iloc[i, j] != originUser.iloc[0, j+1]:
        #         res.loc[i, "updateCode"] = 1
        #         break
        #     res.loc[i, "updateCode"] = 0
        """

    insertData = data[data["updateCode"] == 2].iloc[:, :-1]
    if (~insertData.empty):
        insertData.to_sql("user_info", con=connect.mysql_engine(), if_exists="append", index=False)

@profile
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
pushIndustrialScore()

# pushYearIndustryTrend()

# pushCreativityData()
