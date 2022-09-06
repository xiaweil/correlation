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
    val2017 = pd.DataFrame(result, columns=["industry", "electricity_per_output_2017"])
    return val2017


# 获取因子分析前的输入表
# 读取电力数据表
@operateSqlData
def dayData2monthData(session):
    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date<=" \
          "(select if(day(b.latest_day)>=25, b.latest_day, last_month) from " \
          "(select last_day(subdate(max(mr_date), interval 1 month)) as last_month, max(mr_date) as latest_day from gdc_kie_deq_info_l0) b);"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["cons_no", "cons_name", "mr_date", "dl"])
    data["mr_date"] = data["mr_date"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    return data

@operateSqlData
def updateMonthData(session):
    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date > " \
          "(select subdate(str_to_date(concat(max(mr_date),'-01'), '%Y-%m-%d'), interval 2 month) from montheledata) and mr_date<= " \
          "(select if(day(b.latest_day)>=25, b.latest_day, last_month) from " \
          "(select last_day(subdate(max(mr_date), interval 1 month)) as last_month, max(mr_date) as latest_day from gdc_kie_deq_info_l0) b);"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["cons_no", "cons_name", "mr_date", "dl"])
    data["mr_date"] = data["mr_date"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    return data

@operateSqlData
def getMonthDate(session):
    sql = "select max(mr_date) from montheledata"
    result = session.execute(sql)
    return result.all()[0][0]

@operateSqlData
def getMonthElectricity(session):
    """
    没有月度表的解决方案

    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date > " \
          "(select date_sub(if(day(a.latest_day)>=25, a.latest_day, last_month), interval 2 year) from " \
          "(select last_day(subdate(max(mr_date), interval 1 month)) as last_month, max(mr_date) as latest_day from gdc_kie_deq_info_l0) a) " \
          "and mr_date <= (select if(day(b.latest_day)>=25, b.latest_day, last_month) from " \
          "(select last_day(subdate(max(mr_date), interval 1 month)) as last_month, max(mr_date) as latest_day from gdc_kie_deq_info_l0) b)"
    """

    sql = "select cons_no, cons_name, mr_date, dl from montheledata where mr_date >" \
          "(select subdate(max(concat(mr_date,  '-01')), interval 2 year) from montheledata)"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["cons_no", "cons_name", "mr_date", "dl"])

    """
    data["mr_date"] = data["mr_date"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    """

    return data


@operateSqlData
def getSeasonElectricity(session):
    """
    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date > " \
          "(select if(day(a.latest_day)>25, date_sub(date_add(a.latest_day,interval 1 day), interval 3 month), date_sub(last_month, interval 3 month)) from " \
          "(select last_day(subdate(max(mr_date), interval 1 month)) as last_month, max(mr_date) as latest_day from gdc_kie_deq_info_l0) a)"
    """

    sql = "select cons_no, cons_name, mr_date, dl from montheledata where mr_date >" \
          "(select subdate(max(concat(mr_date, '-01')), interval 3 month) from montheledata)"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["cons_no", "cons_name", "mr_date", "dl"])

    """
    data["mr_date"] = data["mr_date"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    """

    return data


@operateSqlData
def getYearElectricity(session):
    """
    sql = "select cons_no, cons_name, mr_date, dl from gdc_kie_deq_info_l0 where mr_date > " \
          "(select if(day(a.latest_day)>25, date_sub(a.latest_day, interval 1 year), date_sub(last_month, interval 1 year)) from " \
          "(select last_day(subdate(max(mr_date), interval 1 month)) as last_month, max(mr_date) as latest_day from gdc_kie_deq_info_l0) a) "
    """

    sql = "select cons_no, cons_name, mr_date, dl from montheledata where mr_date >" \
          "(select subdate(max(concat(mr_date, '-01')), interval 1 year) from montheledata)"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["cons_no", "cons_name", "mr_date", "dl"])

    """
    data["mr_date"] = data["mr_date"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m"))
    """

    return data

# # v1生成user_info表
# # 通过中台l0，工商信息用电类别、行业表等获取user_info数据
# @operateSqlData
# def getUserInfoTemp(session):
#     sql = "select a.consid user_code, a.consname user_name, tr.sector, a.elecaddr address, o.center, " \
#           "vr.volt_level voltage_level, er.user_type, ci.district, ci.lon, ci.lat, " \
#           "ci.std_industry_e std_industry_name, ii.id std_industry_id, k.企业性质 company_nature, " \
#           "case when ci.core_industry != null then 1 else 0 end is_core," \
#           "ci.core_industry key_industry_name, ki.Id key_industry_id from cstconsumer_s_l0 a " \
#           "left join tradecode_ref tr on a.tradecode = tr.tradecode " \
#           "left join electype_ref er on a.electypecode = er.electypecode " \
#           "left join orgno_ref o on a.orgno = o.orgno " \
#           "left join voltage_ref vr on a.voltcode = vr.voltcode " \
#           "left join commercial_info_copy ci on a.consname = ci.user_name  " \
#           "left join kudata k on a.consname = k.企业名称 " \
#           "left join key_industry ki on core_industry = ki.industry_name " \
#           "left join industry_info ii on ci.std_industry_e = ii.std_industry_name"
#     result = session.execute(sql)
#     data = pd.DataFrame(result, columns=["user_code", "user_name", "sector", "address", "center", "voltage_level",
#                                          "user_type", "district", "lon", "lat", "std_industry_name", "std_industry_id",
#                                          "company_nature", "is_core", "key_industry_name", "key_industry_id"])
#     return data

# v2生成user_info表
@operateSqlData
def getUserl0(session):
    sql = "select consid, consname, elecaddr, tradecode, electypecode, voltcode, orgno, builddate from cstconsumer_s_l0"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["consid", "consname", "elecaddr", "tradecode", "electypecode", "voltcode", "orgno", "build_date"])
    return data

@operateSqlData
def getTradeCode(session):
    sql = "select tradecode, sector, std_industry_name from tradecode_ref"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["tradecode", "sector", "std_industry_name"])
    return data

@operateSqlData
def getOrgno(session):
    sql = "select orgno, center from orgno_ref"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["orgno", "center"])
    return data

@operateSqlData
def getVoltageRef(session):
    sql = "select voltcode, volt_level from voltage_ref"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["voltcode", "volt_level"])
    return data

@operateSqlData
def getUsertype(session):
    sql = "select electypecode, user_type from electype_ref"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["electypecode", "user_type"])
    return data

@operateSqlData
def getRefNameIndustry(session):
    sql = "select * from ref_name_industry"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["user_name", "std_industry_name"])
    return data

@operateSqlData
def getRefNameDistrict(session):
    sql = "select * from ref_name_district"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["user_name", "district"])
    return data

@operateSqlData
def getRefNamePosition(session):
    sql = "select * from ref_name_position"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["user_name", "district", "lon", "lat"])
    return data

@operateSqlData
def getUserInfo(session):
    sql = "select * from user_info"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["id", "user_code", "user_name", "sector", "address", "branch", "center",
                               "voltage_level", "user_type", "district", "lon", "lat", "std_industry_name",
                               "std_industry_id", "company_nature", "is_core", "key_industry_id", "build_date"])
    return data

@operateSqlData
def getIndustryInfo(session):
    sql = "select Id, std_industry_name from industry_info"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["Id", "std_industry_name"])
    return data

@operateSqlData
def getKeyEnterprise(session):
    sql = "select company_name, nature from key_enterprise"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["company_name", "nature"])
    return data

# 获取企业库数据
@operateSqlData
def getCompanyLibrary(session):
    sql = "select company_name from key_enterprise"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["companyName"])
    return data


# 获取工商信息
# @operateSqlData
# def getCommercialInfo(session):
#     sql = "select user_name,core_industry from commercial_info_copy"
#     result = session.execute(sql)
#     data = pd.DataFrame(result, columns=["cuser_name", "core_industry"])
#     return data


@operateSqlData
def getIndustryLibrary(session):
    sql = "select industryClass from industry where isUse=1"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["industryClass"])
    return data


@operateSqlData
def getTaskTimeRange(session):
    sql = "select subdate(adddate(last_day(concat(max(mr_date), '-01')), interval 1 day), interval 2 year), " \
          "max(last_day(concat(mr_date, '-01'))) from montheledata)"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["start_time", "end_time"])
    return data


@operateSqlData
def getSeasonTimeRange(session):
    sql = "select max(last_day(concat(mr_date, '-01'))) from montheledata; "
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["end_time"])
    return data


@operateSqlData
def getIndustryMap(session):
    sql = "select distinct std_industry_name, std_industry_id from user_info"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["std_industry_name", "std_industry_id"])
    return data

@operateSqlData
def getKeyEnterpriseLibrary(session):
    sql = "select company_name, key_industry_name, address, district, nature, type key from key_enterprise"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["company_name", "industry", "address", "district", "nature", "type"])
    return data


@operateSqlData
def getKeyIndustry(session):
    sql = "select Id, industry_name from key_industry"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["kiId", "key_industry_name"])
    return data

@operateSqlData
def getCreativityEnterpriseData(session):
    sql = "select user_name, district_name, type from creativity_type"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["company_name", "district", "creativity_mode"])
    return data

@operateSqlData
def getRefStdKIndustry(session):
    sql = "select * from ref_std_k_industry"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["std_industry_name", "key_industry_name_1"])
    return data

@operateSqlData
def getRefSectorKIndustry(session):
    sql = "select * from ref_sector_k_industry"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["sector", "key_industry_name_2"])
    return data

@operateSqlData
def getRefNameKIndustry(session):
    sql = "select * from ref_name_k_industry"
    result = session.execute(sql)
    data = pd.DataFrame(result, columns=["user_name", "key_industry_name"])
    return data