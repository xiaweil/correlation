# coding:utf8
import pandas as pd
from package.sql_data_operate import pull_sql_data as psd
from datetime import datetime
import re


# 日度数据转月度数据并存入数据库
def toMonthData(operate):
    if operate == 0:
        data = psd.dayData2monthData()
    elif operate == 1:
        data = psd.updateMonthData()
    monthData = data.groupby(["cons_no", "cons_name", "mr_date"], as_index=False)["dl"].sum()
    monthData.rename(columns={"cons_no": "user_code", "cons_name": "user_name", "mr_date": "month",
                              "dl": "consumption"}, inplace=True)
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    monthData['create_time'] = time
    return monthData


# 处理数据并转成user_info    -------v2
def compile(sequence):
    regex = "(.*)\(.*\)$"
    return re.findall(regex, sequence)


def sCompile(sequence):
    regex = "(.*)\s(.*)$"
    return re.findall(regex, sequence)


def pointCompile(sequence):
    regex = "(.*)\.(.*)$"
    return re.findall(regex, sequence)


def dealOriginData():
    originData = psd.getUserl0()
    tradecode = psd.getTradeCode()
    industryClass = psd.getRefNameIndustry()
    division = psd.getRefNameDistrict()
    orgno = psd.getOrgno()
    voltage = psd.getVoltageRef()
    industryInfo = psd.getIndustryInfo()
    keyEnterprise = psd.getKeyEnterprise()
    keyEnterprise.drop_duplicates(subset=["company_name"], keep="first", inplace=True)
    userType = psd.getUsertype()
    position = psd.getRefNamePosition()

    originData["consname"] = originData["consname"].map(lambda x: compile(x)[0] if compile(x) != [] else x)
    originData["consname"] = originData["consname"].map(lambda x: compile(x)[0] if compile(x) != [] else x)
    originData["consname"] = originData["consname"].map(lambda x: sCompile(x)[0][0] if sCompile(x) != [] else x)
    originData["consname"] = originData["consname"].map(lambda x: pointCompile(x)[0][0] if pointCompile(x) != [] else x)

    # 拼接tradecode和ref_name_industry行业，以tradecode优先
    data = pd.merge(originData, tradecode, how="left", on="tradecode")
    data = pd.merge(data, industryClass, how="left", left_on="consname", right_on="user_name", suffixes=("_x", "_y"))
    data.loc[data["std_industry_name_x"].notnull(), "std_industry_name"] = data["std_industry_name_x"]
    data.loc[data["std_industry_name"].isnull(), "std_industry_name"] = data["std_industry_name_y"]

    data.drop(columns=["user_name", "tradecode", "std_industry_name_x", "std_industry_name_y"], inplace=True, axis=1)

    # 添加地区信息
    # ** test success
    areaList = ["松江", "闵行", "徐汇", "浦东", "杨浦", "青浦", "长宁", "普陀", "宝山", "嘉定", "奉贤", "崇明", "黄浦", "虹口",
                "金山", "静安", "卢湾", "闸北", "南汇", "漕河泾"]
    data = pd.merge(data, division, how="left", left_on="consname", right_on="user_name")
    for i in data[data["district"].isnull()].index:
        for j in range(len(areaList)):
            if re.match(f".*{areaList[j]}.*", data.iloc[i, 2]) != None:
                # or re.match(f".*{areaList[j]}.*", data.iloc[i, 1]) != None:
                data.iloc[i, -1] = areaList[j]
            else:
                continue
    areaDict = {"卢湾": "黄浦", "闸北": "静安", "南汇": "浦东", "漕河泾": "徐汇"}

    data["district"] = data["district"].apply(lambda x: areaDict.get(x) if areaDict.get(x) != None else x)
    data = pd.merge(data, userType, how="left", on="electypecode")
    data = pd.merge(data, orgno, how="left", on="orgno")
    data = pd.merge(data, voltage, how="left", on="voltcode")
    data = pd.merge(data, industryInfo, how="left", on="std_industry_name")
    data = pd.merge(data, keyEnterprise, how="left", left_on="consname", right_on="company_name")
    data.loc[data["company_name"].notnull(), "is_core"] = 1
    data["is_core"].fillna(0, inplace=True)

    data.drop(columns=["company_name", "electypecode", "voltcode", "orgno", "user_name"], inplace=True, axis=1)

    data = pd.merge(data, position, how="left", left_on=["consname", "district"], right_on=["user_name", "district"])
    # ['consid', 'consname', 'elecaddr', 'sector', 'std_industry_name',
    #        'district', 'user_type', 'center', 'volt_level', 'Id', 'nature',
    #        'is_core', 'user_name', 'lon', 'lat']

    data.drop(columns=["user_name"], axis=1, inplace=True)
    data.rename(columns={"consid": "user_code", "consname": "user_name", "elecaddr": "address",
                         "volt_level": "voltage_level", "Id": "std_industry_id", "nature": "company_nature"},
                inplace=True)

    return data


# 获取user_info的所有字段，修改并存入数据库 -----v1
def modifyUserInfo():
    data = dealOriginData()
    """
    data字段名
    ['user_code', 'user_name', 'address', 'sector', 'std_industry_name',
     'district', 'user_type', 'center', 'voltage_level', 'std_industry_id',
     'company_nature', 'is_core', 'lon', 'lat']
     """

    refStdKIndustry = psd.getRefStdKIndustry()
    refSectorKIndustry = psd.getRefSectorKIndustry()
    refNameKIndustry = psd.getRefNameKIndustry()
    keyIndustry = psd.getKeyIndustry()
    keyIndustry.rename(columns={"kiId": "key_industry_id"}, inplace=True)

    results = pd.merge(data, refNameKIndustry, how="left", on="user_name")
    results = pd.merge(results, refStdKIndustry, how="left", on="std_industry_name")
    results = pd.merge(results, refSectorKIndustry, how="left", on="sector")

    results.loc[results["key_industry_name"].isnull(), "key_industry_name"] = results["key_industry_name_1"]
    results.loc[results["key_industry_name"].isnull(), "key_industry_name"] = results["key_industry_name_2"]
    results.drop(columns=["key_industry_name_1", "key_industry_name_2"], axis=1, inplace=True)

    """
    用名称匹配得到的行业信息不准确，需要改善
    # for i in range(results.shape[0]):
    #     if re.match(".*船.*", results.loc[i, "user_name"]) != None and results.loc[i, "key_industry_name"] == "航空航天":
    #         results.loc[i, "key_industry_name"] = "船舶"
    #     elif re.match(".*飞机|机场|航空|航天.*", results.loc[i, "user_name"]) != None and results.loc[i, "key_industry_name"] != "航空航天":
    #         results.loc[i, "key_industry_name"] = "航空航天"
    #     else:
    #         continue
    """

    results = pd.merge(results, keyIndustry, how="left", on="key_industry_name")
    columns = ["user_code", "user_name", "sector", "address", "branch", "center", "voltage_level", "user_type",
               "district", "lon", "lat", "std_industry_name", "std_industry_id", "company_nature", "is_core",
               "key_industry_id", "build_date"]
    results.insert(4, "branch", None)
    # data.insert(data.shape[1], "key_industry_id", None)
    results = results[columns]
    print("user_info已生成！")
    return results


# 获取两年的月度数据
def dealElectricity():
    data = psd.getMonthElectricity()
    # monthData = data.groupby(["cons_no", "mr_date"])["dl"].sum()
    monthData = data[["cons_no", "mr_date", "dl"]]
    monthData.set_index(["cons_no", "mr_date"], drop=True, inplace=True)
    monthData = monthData.unstack()['dl'].rename_axis(columns=None).reset_index()
    __columnsName = [f"month{i}" for i in range(1, 25)]
    __columnsEle = __columnsName.copy()
    __columnsName.insert(0, "userId")
    monthData.columns = __columnsName
    monthData.loc[:, __columnsEle] = monthData.loc[:, __columnsEle].applymap(lambda x: None if x < 0 else x)

    # 计算两年用电量总和
    monthData["sum"] = monthData.loc[:, __columnsEle].sum(axis=1)
    # 计算两年中用电量最大值
    monthData["max"] = monthData.loc[:, __columnsEle].max(axis=1)
    # 筛选用电量为0的数据
    monthData = monthData[monthData["sum"] != 0]
    # 滑动窗口筛选连续出现六个月的数据
    tempEle = monthData.loc[:, __columnsEle]
    tempEleBool = tempEle.applymap(lambda x: 1 if x > 0 else 0)
    res = tempEleBool.rolling(window=8, min_periods=1, axis=1).sum().max(axis=1)
    res = pd.DataFrame(res, columns=["slideMax"])
    monthData = pd.concat([monthData, res], axis=1)
    monthData = monthData[monthData["slideMax"] >= 6].iloc[:, :-1]
    # 计算用电量数据非空月份总和
    period = tempEle.notnull().sum(axis=1)
    period = pd.DataFrame(period, columns=["period"])
    monthData = pd.concat([monthData, period], axis=1)
    # 计算用电量均值
    monthData["mean"] = monthData["sum"] / monthData["period"]
    print("电力数据拼接完成")
    return monthData


# 获取季度电力数据
def dealSeasonElectricity(data=psd.getSeasonElectricity()):
    # monthData = data.groupby(["cons_no", "mr_date"])["dl"].sum()
    monthData = data[["cons_no", "mr_date", "dl"]]
    monthData.set_index(["cons_no", "mr_date"], inplace=True)
    monthData = monthData.unstack()['dl'].rename_axis(columns=None).reset_index()
    __columnsName = [f"season{i}" for i in range(1, 4)]
    __columnsEle = __columnsName.copy()
    __columnsName.insert(0, "userId")
    monthData.columns = __columnsName
    monthData.loc[:, __columnsEle] = monthData.loc[:, __columnsEle].applymap(lambda x: None if x < 0 else x)

    # 计算两年用电量总和
    monthData["sum"] = monthData.loc[:, __columnsEle].sum(axis=1)
    # 计算两年中用电量最大值
    monthData["max"] = monthData.loc[:, __columnsEle].max(axis=1)
    # 筛选用电量为0的数据
    monthData = monthData[monthData["sum"] != 0]

    # 滑动窗口筛选连续出现六个月的数据
    tempEle = monthData.loc[:, __columnsEle]
    # tempEleBool = tempEle.applymap(lambda x: 1 if x > 0 else 0)
    # res = tempEleBool.rolling(window=8, min_periods=1, axis=1).sum().max(axis=1)
    # res = pd.DataFrame(res, columns=["slideMax"])
    # monthData = pd.concat([monthData, res], axis=1)
    # monthData = monthData[monthData["slideMax"] >= 6].iloc[:, :-1]

    # 计算用电量数据非空月份总和
    period = tempEle.notnull().sum(axis=1)
    period = pd.DataFrame(period, columns=["period"])

    monthData = pd.concat([monthData, period], axis=1)
    # 计算用电量均值
    monthData["mean"] = monthData["sum"] / monthData["period"]
    print("电力数据拼接完成")
    return monthData


# 获取年度产业数据
def dealYearElectricity(data=psd.getYearElectricity()):
    # monthData = data.groupby(["cons_no", "mr_date"])["dl"].sum()
    monthData = data[["cons_no", "mr_date", "dl"]]
    monthData.set_index(["cons_no", "mr_date"], inplace=True)
    monthData = monthData.unstack()['dl'].rename_axis(columns=None).reset_index()
    __columnsName = [f"year{i}" for i in range(1, 13)]
    __columnsEle = __columnsName.copy()
    __columnsName.insert(0, "userId")
    monthData.columns = __columnsName
    monthData.loc[:, __columnsEle] = monthData.loc[:, __columnsEle].applymap(lambda x: None if x < 0 else x)

    # 计算两年用电量总和
    monthData["sum"] = monthData.loc[:, __columnsEle].sum(axis=1)
    # 计算两年中用电量最大值
    monthData["max"] = monthData.loc[:, __columnsEle].max(axis=1)
    # 筛选用电量为0的数据
    monthData = monthData[monthData["sum"] != 0]
    # 滑动窗口筛选连续出现六个月的数据
    tempEle = monthData.loc[:, __columnsEle]
    tempEleBool = tempEle.applymap(lambda x: 1 if x > 0 else 0)
    res = tempEleBool.rolling(window=8, min_periods=1, axis=1).sum().max(axis=1)
    res = pd.DataFrame(res, columns=["slideMax"])
    monthData = pd.concat([monthData, res], axis=1)
    monthData = monthData[monthData["slideMax"] >= 6].iloc[:, :-1]
    # 计算用电量数据非空月份总和
    period = tempEle.notnull().sum(axis=1)
    period = pd.DataFrame(period, columns=["period"])
    monthData = pd.concat([monthData, period], axis=1)
    # 计算用电量均值
    monthData["mean"] = monthData["sum"] / monthData["period"]
    print("电力数据拼接完成")
    return monthData


"""
# 拼接成key_enterprise表，但还没有输出
def concatUserInfoAndKuData():
    data = psd.getUserInfo()
    keyIndustryData = psd.getKeyIndustry()
    data = data[data["is_core"] == 1]
    kuData = psd.getKeyEnterpriseLibrary()
    results = pd.merge(data, kuData, how="left", left_on="user_name", right_on="company_name")
    results = pd.merge(results, keyIndustryData, how="left", left_on="key_industry_id", right_on="kiId")
    columns = ["user_name", "user_code", "key_industry_name", "address_x", "district_x", "nature", "type"]
    results = results[columns]
    print(results)
"""
"""
def concatCreatityType():
    data = psd.getUserInfo()
    creativityEnterpriseData = psd.getCreativityEnterpriseData()
    results = pd.merge(data, creativityEnterpriseData, how="left", left_on="user_name", right_on="company_name")
    columns = ["user_code", "district_x", "creativity_mode"]
    results = results[columns]
    results.dropna(inplace=True)
    results.rename(columns={"district_x": "district_name", "creativity_mode": "type"}, inplace=True)
    return results

# concatUserInfoAndKuData()
# concatCreatityType()
"""
