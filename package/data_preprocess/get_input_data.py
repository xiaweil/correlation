# coding:utf8
import pandas as pd
from package.sql_data_operate import deal_sql_data as dsd
from package.sql_data_operate import pull_sql_data as psd


def generateInputData():
    # 获取电力数据表
    dataEle = dsd.dealElectricity()

    # 获取user_info表
    user_info = psd.getUserInfoTemp()

    # 获取重点企业库kudata
    keyCompany = psd.getCompanyLibrary()

    # 拼接user_info和重点企业库，取出重点企业的Id
    user = pd.merge(keyCompany, user_info, how="left", left_on="companyName", right_on="user_name")
    keyCompanyId = user.loc[:, "user_code"]
    keyCompanyId = list(keyCompanyId)

    dataEle.loc[dataEle["userId"].isin(keyCompanyId), "isCoreCompany"] = 1
    dataEle.loc[~dataEle["userId"].isin(keyCompanyId), "isCoreCompany"] = 0
    print("是否重点企业拼接完成")


    # 获取division, industryClass, eleType指标
    partUserInfoColumns = ["user_code", "user_name", "district", "user_type", "std_industry_name"]
    partUserInfo = user_info.loc[:, partUserInfoColumns]

    # 获取keyIndustryClass指标
    # 获取工商信息
    keyIndustryClass = psd.getCommercialInfo()
    partV3Data = pd.merge(partUserInfo, keyIndustryClass, how="left", left_on="user_name", right_on="cuser_name")
    v3InputData = pd.merge(dataEle, partV3Data, how="left", left_on="userId", right_on="user_code")
    print(v3InputData.columns)

generateInputData()
