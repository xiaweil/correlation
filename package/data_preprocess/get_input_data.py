# coding:utf8
import pandas as pd
from package.sql_data_operate import deal_sql_data as dsd
from package.sql_data_operate import pull_sql_data as psd


def generateInputData():
    # 获取电力数据表
    dataEle = dsd.dealElectricity()
    print(dataEle)
    # 获取user_info表
    user_info = psd.getUserInfoTemp()
    print(user_info)
    # 获取重点企业库kudata
    keyCompany = psd.getCompanyLibrary()
    print(keyCompany)

    # 拼接user_info和重点企业库，取出重点企业的Id
    user = pd.merge(keyCompany, user_info, how="left", left_on="companyName", right_on="user_name")
    keyCompanyId = user.loc[:, "user_code"]
    keyCompanyId = list(keyCompanyId)

    dataEle["isCoreCompany"]
    print(keyCompanyId)


generateInputData()
