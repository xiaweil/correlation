# -*- coding: utf-8 -*-
# @Time     : 2022/9/6 15:16
# @author   : yone
# @FileName : yearQualityTable.py
import pandas as pd
from package.db_connect import connect
from package.db_connect.connect import operateSqlData
from package.data_preprocess.get_input_data import generateYearInputData
from package.sql_data_operate.deal_sql_data import dealYearElectricity
from package.develop_score.dataMerge import getYearIndustrialScore
from package.develop_score.output_result import outputYearScores
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("mysql+pymysql://root:Csstsari107!@124.71.156.211:3307/shinan_cyjj?charset=utf8")
DBSession = sessionmaker(bind=engine)
session = DBSession()

startTime = ['2019-01', '2020-01', '2021-01']
endTime = ['2019-12', '2020-12', '2021-12']


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
    # data = getYearData(start_date=start_date, end_date=end_date)
    # print(data)
    # res = dealYearElectricity(data=data)
    # res1 = generateYearInputData(dataEle=res)
    # res2 = getYearIndustrialScore(res1)
    # data = outputYearScores(results=res2, timeRange=timeRange)
    # data.to_sql("industry_trend", con=connect.mysql_engine(), if_exists="append", index=False)

session.close()




