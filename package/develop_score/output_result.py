#coding:utf8
from datetime import datetime
import pandas as pd

from package.develop_score import dataMerge as dm
from package.sql_data_operate import pull_sql_data as psd

#输出产业评估得分
def output_scores():
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
    return results
