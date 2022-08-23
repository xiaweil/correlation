# coding:utf8
import pandas as pd
from package.db_connect.connect import getSqlData
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 获取用户电压数据
@getSqlData
def getVoltageData(session):
    sql = "select user_code, voltage_level from user_info"
    result = session.execute(sql)
    voltage = pd.DataFrame(result, columns=["userIdVoltage", "voltageLevel"])
    return voltage


# 获取最新的单位产值能耗数据
@getSqlData
def getval2019(session):
    sql = "select industry, electricity_per_output from energy_consumption where year =" \
          "(select distinct year from energy_consumption order by year desc limit 1);"
    result = session.execute(sql)
    val2019 = pd.DataFrame(result, columns=["industry", "electricity_per_output_2019"])
    return val2019


# 获取次新的单位产值能耗数据
@getSqlData
def getval2017(session):
    sql = "select industry, electricity_per_output from energy_consumption where year =" \
          "(select distinct year from energy_consumption order by year desc limit 1,1);"
    result = session.execute(sql)
    val2017 = pd.DataFrame(result, columns=["industry", "electricity_per_output2017"])
    return val2017


@getSqlData
def getMonthElectricity(session):
    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date>= " \
          "(select distinct date_sub(mr_date,interval 2 year) from gdc_kie_deq_info_l0 order by mr_date desc limit 1);"
    result = session.execute(sql)
    data = pd.DataFrame(result)
    data["mr_date"] = data["mr_date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    monthData = data.groupby(["cons_no", "mr_date"])["dl"].sum()



getMonthElectricity()
