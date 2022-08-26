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
    partUserInfoColumns = ["user_code", "user_name", "district", "user_type", "std_industry_name", "key_industry_name"]
    partUserInfo = user_info.loc[:, partUserInfoColumns]

    # 获取keyIndustryClass指标
    # 获取工商信息
    # keyIndustryClass = psd.getCommercialInfo()
    # partV3Data = pd.merge(partUserInfo, keyIndustryClass, how="left", left_on="user_name", right_on="cuser_name")
    v3InputData = pd.merge(dataEle, partUserInfo, how="left", left_on="userId", right_on="user_code")
    v3InputData.rename(columns={"std_industry_name": "industryClass", "key_industry_name": "keyIndustryClass",
                                "user_type": "eleType", "district": "division"}, inplace=True)
    v3InputData.drop(columns=["user_code", "user_name"], axis=1, inplace=True)
    columns = ['userId', 'month1', 'month2', 'month3', 'month4', 'month5', 'month6',
               'month7', 'month8', 'month9', 'month10', 'month11', 'month12',
               'month13', 'month14', 'month15', 'month16', 'month17', 'month18',
               'month19', 'month20', 'month21', 'month22', 'month23', 'month24', 'sum',
               'max', 'division', 'isCoreCompany', 'industryClass', 'eleType',
               'period', 'mean', 'keyIndustryClass']
    v3InputData = v3InputData.loc[:, columns]
    return v3InputData

# 获取季度数据输入表
def generateSeasonInputData():
    # 获取电力数据表
    dataEle = dsd.dealSeasonElectricity()
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
    partUserInfoColumns = ["user_code", "user_name", "district", "user_type", "std_industry_name", "key_industry_name"]
    partUserInfo = user_info.loc[:, partUserInfoColumns]

    # 获取keyIndustryClass指标
    # 获取工商信息
    # keyIndustryClass = psd.getCommercialInfo()
    # partV3Data = pd.merge(partUserInfo, keyIndustryClass, how="left", left_on="user_name", right_on="cuser_name")
    v3InputData = pd.merge(dataEle, partUserInfo, how="left", left_on="userId", right_on="user_code")
    v3InputData.rename(columns={"std_industry_name": "industryClass", "key_industry_name": "keyIndustryClass",
                                "user_type": "eleType", "district": "division"}, inplace=True)
    v3InputData.drop(columns=["user_code", "user_name"], axis=1, inplace=True)
    columns = ['userId', 'season1', 'season2', 'season3', 'sum',
               'max', 'division', 'isCoreCompany', 'industryClass', 'eleType',
               'period', 'mean', 'keyIndustryClass']
    v3InputData = v3InputData.loc[:, columns]
    return v3InputData

# 获取年度数据输入表
def generateYearInputData():
    # 获取电力数据表
    dataEle = dsd.dealYearElectricity()

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
    partUserInfoColumns = ["user_code", "user_name", "district", "user_type", "std_industry_name", "key_industry_name"]
    partUserInfo = user_info.loc[:, partUserInfoColumns]

    # 获取keyIndustryClass指标
    # 获取工商信息
    # keyIndustryClass = psd.getCommercialInfo()
    # partV3Data = pd.merge(partUserInfo, keyIndustryClass, how="left", left_on="user_name", right_on="cuser_name")
    v3InputData = pd.merge(dataEle, partUserInfo, how="left", left_on="userId", right_on="user_code")
    v3InputData.rename(columns={"std_industry_name": "industryClass", "key_industry_name": "keyIndustryClass",
                                "user_type": "eleType", "district": "division"}, inplace=True)
    v3InputData.drop(columns=["user_code", "user_name"], axis=1, inplace=True)
    columns = ['userId', 'year1', 'year2', 'year3', 'year4', 'year5', 'year6',
               'year7', 'year8', 'year9', 'year10', 'year11', 'year12', 'sum',
               'max', 'division', 'isCoreCompany', 'industryClass', 'eleType',
               'period', 'mean', 'keyIndustryClass']
    v3InputData = v3InputData.loc[:, columns]
    return v3InputData