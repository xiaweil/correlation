# coding:utf8
import pandas as pd
from package.db_connect.connect import operateSqlData
from datetime import datetime
from dateutil.relativedelta import relativedelta


# 获取用户电压数据
# 读取电压映射表
@operateSqlData
def getVoltageData(session):
    sql = "select user_code, voltage_level from user_info"
    result = session.execute(sql)
    voltage = pd.DataFrame(result, columns=["userIdVoltage", "voltageLevel"])
    return voltage


# 获取最新的单位产值能耗数据
# 读取能耗信息表
@operateSqlData
def getval2019(session):
    sql = "select industry, electricity_per_output from energy_consumption where year =" \
          "(select distinct year from energy_consumption order by year desc limit 1);"
    result = session.execute(sql)
    val2019 = pd.DataFrame(result, columns=["industry", "electricity_per_output_2019"])
    return val2019


# 获取次新的单位产值能耗数据
# 读取能耗信息表
@operateSqlData
def getval2017(session):
    sql = "select industry, electricity_per_output from energy_consumption where year =" \
          "(select distinct year from energy_consumption order by year desc limit 1,1);"
    result = session.execute(sql)
    val2017 = pd.DataFrame(result, columns=["industry", "electricity_per_output2017"])
    return val2017


# 获取因子分析前的输入表
# 读取电力数据表
@operateSqlData
def getMonthElectricity(session):
    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date > " \
          "(select distinct date_sub(mr_date,interval 2 year) from gdc_kie_deq_info_l0 order by mr_date desc limit 1);"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["cons_no", "cons_name", "mr_date", "dl"])
    data["mr_date"] = data["mr_date"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    return data


# 通过中台l0，工商信息用电类别、行业表等获取user_info数据
@operateSqlData
def getUserInfoTemp(session):
    sql = "select a.consid user_code, a.consname user_name, tr.sector, a.elecaddr address, o.center, " \
          "vr.volt_level voltage_level, er.user_type, ci.district, ci.lon, ci.lat, " \
          "tr.std_industry_name_e std_industry_name, tr.id std_industry_id, k.企业性质 company_nature, " \
          "case when ci.core_industry != null then 1 else 0 end is_core," \
          " ci.std_industry_e key_industry_name, ki.Id key_industry_id from cstconsumer_s_l0 a " \
          "left join tradecode_ref tr on a.tradecode = tr.tradecode " \
          "left join electype_ref er on a.electypecode = er.electypecode " \
          "left join orgno_ref o on a.orgno = o.orgno " \
          "left join voltage_ref vr on a.voltcode = vr.voltcode " \
          "left join commercial_info_copy ci on a.consname = ci.user_name " \
          "left join kudata k on a.consname = k.企业名称 " \
          "left join key_industry ki on core_industry = ki.industry_name"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["user_code", "user_name", "sector", "address", "center", "voltage_level",
                                         "user_type", "district", "lon", "lat", "std_industry_name", "std_industry_id",
                                         "company_nature", "is_core", "key_industry_name", "key_industry_id"])
    return data


@operateSqlData
def getUserInfo(session):
    sql = "select * from user_info"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["user_code", "user_name", "sector", "address", "branch", "center", "voltage_level",
                                 "user_type", "district", "lon", "lat", "std_industry_name", "std_industry_id",
                                 "company_nature", "is_core", "key_industry_id"])
    return data

@operateSqlData
def getCompanyLibrary(session):
    sql = "select 企业名称 from kudata"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["companyName"])
    return data