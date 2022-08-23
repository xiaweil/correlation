# coding:utf8
import pandas as pd
from package.sql_data_operate import pull_sql_data as psd


def dealElectricity(data=psd.getMonthElectricity()):
    monthData = data.groupby(["cons_no", "mr_date"])["dl"].sum()
    monthData = monthData.unstack().rename_axis(columns=None).reset_index()
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
    print(monthData)
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
    return monthData


# 获取user_info的所有字段，修改并存入数据库
def modifyUserInfo(data=psd.getUserInfoTemp()):
    columns = ["user_code", "user_name", "sector", "address", "branch", "center", "voltage_level", "user_type",
               "district", "lon", "lat", "std_industry_name", "std_industry_id", "company_nature", "is_core",
               "key_industry_id"]
    data.insert(4, "branch", None)
    data = data.loc[:, columns]
    return data


