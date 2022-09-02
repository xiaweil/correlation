# coding:utf8
from datetime import datetime
import pandas as pd
import numpy as np
from package.develop_score import dataMerge as dm
from package.sql_data_operate import pull_sql_data as psd


# 输出产业评估得分
def outputIndustryScores():
    # 计算分区域分产业得分
    timeRange = psd.getTaskTimeRange()
    results = dm.getIndustrialScore()
    results["start_time"] = datetime.strptime(timeRange["start_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    results["end_time"] = datetime.strptime(timeRange["end_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    industryMap = psd.getIndustryMap()
    results = pd.merge(results, industryMap, how="left", left_on="industryClass", right_on="std_industry_name")
    results.drop(columns=["std_industry_name"], inplace=True, axis=1)
    results.rename(columns={"division": "region_name", "industryClass": "std_industry_name", "产业规模得分": "scale_score_l1",
                            "发展质量得分": "quality_score_l1", "产业增速得分": "growth_score_l1", "产业稳定性得分": "stability_score_l1",
                            "综合得分": "overall_score"}, inplace=True)

    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results["create_time"] = time
    columns = ["region_name", "std_industry_name", "std_industry_id", "start_time", "end_time", "create_time",
               "overall_score", "scale_score_l1", "growth_score_l1", "quality_score_l1", "stability_score_l1"]
    results = results[columns]

    # 计算区域综合得分
    industryCountOfArea = results.groupby(["region_name"])["std_industry_name"].count().rename("count")

    areaIndustryScale = results.groupby(["region_name"])["scale_score_l1"].sum()
    areaIndustryGrowth = results.groupby(["region_name"])["growth_score_l1"].sum()
    areaDevelopQuality = results.groupby(["region_name"])["quality_score_l1"].sum()
    areaIndustryStability = results.groupby(["region_name"])["stability_score_l1"].sum()

    areaIndustryScaleScore = areaIndustryScale * np.log2(industryCountOfArea) / industryCountOfArea
    areaIndustryGrowthScore = areaIndustryGrowth * np.log2(industryCountOfArea) / industryCountOfArea
    areaDevelopQualityScore = areaDevelopQuality * np.log2(industryCountOfArea) / industryCountOfArea
    areaIndustryStabilityScore = areaIndustryStability * np.log2(industryCountOfArea) / industryCountOfArea
    areaIndustryOverallScore = (areaIndustryScaleScore + areaIndustryGrowthScore + areaDevelopQualityScore + areaIndustryStabilityScore) / 4
    areaResults = pd.concat([areaIndustryOverallScore, areaIndustryScaleScore, areaIndustryGrowthScore,
                             areaDevelopQualityScore, areaIndustryStabilityScore], axis=1).reset_index()
    areaResults.columns = ["region_name", "overall_score", "scale_score_l1", "growth_score_l1", "quality_score_l1",
                           "stability_score_l1"]
    areaResults.insert(1, "start_time",
                       datetime.strptime(timeRange["start_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d"))
    areaResults.insert(2, "end_time",
                       datetime.strptime(timeRange["end_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d"))
    areaResults.insert(3, "create_time", time)

    return results, areaResults

# 输出季度产业规模得分
def outputSeasonScores():
    results = dm.getSeasonIndustrialScore()
    industryMap = psd.getIndustryMap()
    results = pd.merge(results, industryMap, how="left", left_on="industryClass", right_on="std_industry_name")
    results.drop(columns=["std_industry_name"], inplace=True, axis=1)
    results.rename(
        columns={"division": "region_name", "industryClass": "std_industry_name", "季度产业规模得分": "scale_score_l1"},
        inplace=True)
    timeRange = psd.getSeasonTimeRange()
    year = datetime.strptime(timeRange["end_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%Y")
    quarter = datetime.strptime(timeRange["end_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%m")

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
    return results

# 输出年度产业规模得分
def outputYearScores():
    results = dm.getYearIndustrialScore()
    industryMap = psd.getIndustryMap()
    results = pd.merge(results, industryMap, how="left", left_on="industryClass", right_on="std_industry_name")
    results.drop(columns=["std_industry_name"], inplace=True, axis=1)
    results.rename(
        columns={"division": "region_name", "industryClass": "std_industry_name", "年度产业规模得分": "scale_score_l1"},
        inplace=True)
    timeRange = psd.getTaskTimeRange()
    year = datetime.strptime(timeRange["end_time"][0], "%Y-%m-%d %H:%M:%S").strftime("%Y")

    """
    if quarter in [3, 6, 9, 12]:
        results["stat_time"] = f"{year}"
        results["frequency"] = 2
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results["create_time"] = time
        columns = ["region_name", "std_industry_id", "stat_time", "scale_score_l1", "frequency", "create_time"]
        results = results[columns]
        return results
    else:
        print("未到季度更新节点，数据不需要更新！")
    """

    results["stat_time"] = f"{year}"
    results["frequency"] = 1
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results["create_time"] = time
    columns = ["region_name", "std_industry_id", "stat_time", "scale_score_l1", "frequency", "create_time"]
    results = results[columns]
    return results
