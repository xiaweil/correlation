# -*- coding: utf-8 -*-
# @Time     : 2022/9/6 16:40
# @author   : yone
# @FileName : quarterQualityTable.py

from package.sql_data_operate.deal_sql_data import dealSeasonElectricity
import pandas as pd
from package.db_connect import connect
from datetime import datetime
from package.develop_score.factorAnalysisApi import mainVs
from package.develop_score.scoreAdjust import adjustScore
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from package.sql_data_operate import pull_sql_data as psd
from package.develop_score.zb_quantity_quality import finalMerge

engine = create_engine("mysql+pymysql://root:Csstsari107!@124.71.156.211:3307/shinan_cyjj?charset=utf8")
DBSession = sessionmaker(bind=engine)
session = DBSession()

startTime = ['2019-01', '2019-04', '2019-07', '2019-10', '2020-01', '2020-04', '2020-07',
             '2020-10', '2021-01', '2021-04', '2021-07', '2021-10']
endTime = ['2019-03', '2019-06', '2019-09', '2019-12', '2020-03', '2020-06', '2020-09',
           '2020-12', '2021-03', '2021-06', '2021-09', '2021-12']

def getUserInfoTemp():
    data = psd.getUserInfo()
    keyIndustry =psd.getKeyIndustry()
    data = pd.merge(data, keyIndustry, how="left", left_on="key_industry_id", right_on="kiId")
    columns = ["user_code", "user_name", "sector", "address", "center", "voltage_level", "user_type", "district",
               "lon", "lat", "std_industry_name", "std_industry_id", "company_nature", "is_core", "key_industry_name",
               "key_industry_id"]
    return data[columns]

def getYearData(start_date, end_date):
    sql = f"select cons_no, mr_date, dl from montheledata where mr_date >= \'{start_date}\' and mr_date<=\'{end_date}\'"
    print(sql)
    results = session.execute(sql)
    data = pd.DataFrame(results, columns=["cons_no", "mr_date", "dl"])
    return data

for i in range(len(startTime)):
    start_date = startTime[i]
    end_date = endTime[i]
    timeRange = pd.DataFrame([[start_date, end_date]], columns=["start_time", "end_time"])
    print(timeRange)
    data = getYearData(start_date=start_date, end_date=end_date)
    print(data)
    dataEle = dealSeasonElectricity(data=data)
    user_info = getUserInfoTemp()

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
    data = v3InputData

    val2019 = psd.getval2019()
    val2017 = psd.getval2017()
    voltage = psd.getVoltageData()
    industryLibrary = psd.getIndustryLibrary()
    data = data[data["industryClass"].isin(list(industryLibrary["industryClass"]))]

    result1 = finalMerge(data, voltage, val2019, val2017, 2)
    columns = ["division", "industryClass", "electricity_sum_l3", "electricity_avg_l3", "electricity_max_l3",
               "electricity_sum_max_l3", "output_sum_l3", "output_proportion_l3", "enterprise_sum_l3",
               "enterprise_proportion_l3", "proportion_district_l3", "proportion_industry_l3"]
    result = result1[columns]

    # FA计算得分
    print("开始因子分析")
    scores = mainVs(result)
    # 调整分数并计算综合得分
    print("因子分析完成")
    afterAdjustScore = adjustScore(scores.iloc[:, 2:])
    afterAdjustScore.columns = ["季度产业规模得分"]
    finalScore = pd.concat([scores.iloc[:, :2], afterAdjustScore], axis=1)

    results = finalScore
    industryMap = psd.getIndustryMap()
    results = pd.merge(results, industryMap, how="left", left_on="industryClass", right_on="std_industry_name")
    results.drop(columns=["std_industry_name"], inplace=True, axis=1)
    results.rename(
        columns={"division": "region_name", "industryClass": "std_industry_name", "季度产业规模得分": "scale_score_l1"},
        inplace=True)
    print("jgfshfjk")
    print(results)
    year = datetime.strptime(str(timeRange["end_time"][0]), "%Y-%m").strftime("%Y")
    quarter = datetime.strptime(str(timeRange["end_time"][0]), "%Y-%m").strftime("%m")

    """
    if quarter in [3, 6, 9, 12]:
        results["stat_time"] = f"{year}-Q{int(quarter) / 3}"
        results["frequency"] = 2
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results["create_time"] = time
        columns = ["region_name", "std_industry_id", "stat_time", "scale_score_l1", "frequency", "create_time"]
        results = results[columns]
        return results
    else:
        print("未到季度更新节点，数据不需要更新！")
    """

    results["stat_time"] = f"{year}0{int(int(quarter) / 3)}"
    results["frequency"] = 2
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results["create_time"] = time
    columns = ["region_name", "std_industry_id", "stat_time", "scale_score_l1", "frequency", "create_time"]
    results = results[columns]
    results.to_sql("industry_trend", con=connect.mysql_engine(), if_exists="append", index=False)

session.close()
