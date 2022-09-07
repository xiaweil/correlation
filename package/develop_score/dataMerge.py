# coding:utf8
import pandas as pd
from package.develop_score.zb_quantity_quality import finalMerge
from package.develop_score.zb_growth_stability import calFunZb
from package.develop_score.factorAnalysisApi import mainV2, mainVs
from package.develop_score.scoreAdjust import adjustScore
from package.data_preprocess import get_input_data as gid
from package.sql_data_operate import pull_sql_data as psd

def getIndustrialScore():
    # 输入
    data = gid.generateInputData()
    val2019 = psd.getval2019()
    val2017 = psd.getval2017()
    voltage = psd.getVoltageData()
    industryLibrary = psd.getIndustryLibrary()
    data = data[data["industryClass"].isin(list(industryLibrary["industryClass"]))]
    data = data[data["division"].notnull()]
    result1 = finalMerge(data, voltage, val2019, val2017)
    result2 = calFunZb(data)
    result2.rename(columns={'division': 'division1', 'industryClass': 'industryClass1'}, inplace=True)
    result = pd.merge(result1, result2, how="left", left_on=['division', 'industryClass'],
                      right_on=['division1', 'industryClass1'])
    print(result)
    result.drop(columns=['division1', 'industryClass1'], inplace=True)
    # result.to_csv("C://Users/yone/Desktop/endResults.csv", index=False)
    # print(result)
    # FA计算得分
    print("开始因子分析")
    scores = mainV2(result)
    # 调整分数并计算综合得分
    print("因子分析完成")
    afterAdjustScore = adjustScore(scores.iloc[:, 2:])
    afterAdjustScore.columns = ["产业规模得分", "发展质量得分", "产业增速得分", "产业稳定性得分"]
    afterAdjustScore["综合得分"] = 0.25 * (afterAdjustScore["产业规模得分"] + afterAdjustScore["发展质量得分"]
                                       + afterAdjustScore["产业增速得分"] + afterAdjustScore["产业稳定性得分"])
    finalScore = pd.concat([scores.iloc[:, :2], afterAdjustScore], axis=1)
    print("获取分数结果完成")
    return finalScore

# 获取季度产业规模得分
def getSeasonIndustrialScore():
    # 输入
    data = gid.generateSeasonInputData()
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
    return finalScore


# 获取年度产业规模得分
def getYearIndustrialScore():
    # 输入
    data = gid.generateYearInputData()
    val2019 = psd.getval2019()
    val2017 = psd.getval2017()
    voltage = psd.getVoltageData()
    industryLibrary = psd.getIndustryLibrary()
    data = data[data["industryClass"].isin(list(industryLibrary["industryClass"]))]

    result1 = finalMerge(data, voltage, val2019, val2017, 3)
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
    afterAdjustScore.columns = ["年度产业规模得分"]
    finalScore = pd.concat([scores.iloc[:, :2], afterAdjustScore], axis=1)
    return finalScore