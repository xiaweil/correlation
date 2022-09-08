# coding:utf8
import pandas as pd

# 计算产业规模
def calIndustryScaleData(data, columns):
    #按区域按产业计算用电量总和
    electricityScale = data.groupby(["division", "industryClass"], as_index=False)["sum"].sum().reset_index()

    #按区域按产业计算平均用电量总和
    avgMonthElectricity = data.groupby(["division", "industryClass"], as_index=False)["mean"].sum().rename(
        columns={"mean": "monthAvgElectricity"})
    avgMonthElectricity.drop(["division", "industryClass"], axis=1, inplace=True)

    #计算最大用电量
    maxmax = data.groupby(["division", "industryClass"], as_index=False)[columns].max().max(axis=1)
    maxmax = pd.DataFrame(maxmax, columns={"maxmax"})
    summax = data.groupby(["division", "industryClass"], as_index=False)['max'].sum()
    summax.drop(["division", "industryClass"], axis=1, inplace=True)

    #计算总企业数
    totalCompany = data.groupby(["division", "industryClass"], as_index=False)['userId'].count()
    totalCompany.rename(columns={"userId": "companyTotal", "division": "xzqh", "industryClass": "hydl"}, inplace=True)

    results = pd.concat([electricityScale, maxmax, summax, totalCompany, avgMonthElectricity], axis=1)

    #计算区域企业总数
    areaToatlCompany = totalCompany.groupby(['xzqh'], as_index=False)['companyTotal'].sum().rename(
        columns={"companyTotal": "areaCompanyTotal"})

    results = pd.merge(results, areaToatlCompany, how="left", left_on="division", right_on="xzqh")
    results.drop(["xzqh_x", "xzqh_y", "hydl"], axis=1, inplace=True)

    #计算区域总用电量
    sameAreaTotalElectricity = electricityScale.groupby(['division'], as_index=False)['sum'].sum().rename(
        columns={"division": "qetdivision", "sum": "areaElectricityTotal"})

    results = pd.merge(results, sameAreaTotalElectricity, how="left", left_on="division", right_on="qetdivision")
    results.drop("qetdivision", inplace=True, axis=1)

    #计算相同产业总用电量
    sameIndustryTotalElectricity = electricityScale.groupby("industryClass", as_index=False)['sum'].sum().rename(
        columns={"industryClass": "industry", "sum": "industryElectricityTotal"})

    results = pd.merge(results, sameIndustryTotalElectricity, how="left", left_on="industryClass",
                       right_on="industry")
    results.drop("industry", inplace=True, axis=1)

    #计算相同产业企业总数
    """
    # sameIndustryTotal = totalCompany.groupby("hydl", as_index=False)['companyTotal'].sum().rename(
    #     columns={"companyTotal": "industryCompany"})
    """

    sameIndustryTotal = totalCompany.rename(columns={"companyTotal": "industryCompany"})
    results = pd.merge(results, sameIndustryTotal, how="left", left_on=["division", "industryClass"],
                       right_on=["xzqh", "hydl"])
    results.drop(columns=["xzqh","hydl"], inplace=True, axis=1)
    print("产业规模计算完成")
    return results


def __calDevelopQuality(data, valueConsumption2019, valueConsumption2017):
    data.loc[data["mean"] >= 500000, "highElectricityThreshold"] = 1
    data.loc[data["keyIndustryClass"].notnull(), "isCoreIndustry"] = 1
    data.fillna(0)

    #计算同一区域同一产业重点企业数量
    coreCompany = data.groupby(["division", "industryClass"], as_index=False)["isCoreCompany"].sum().rename(
        columns={"isCoreCompany": "key_enterprise_sum_l3"})

    #计算同一区域同一产业重点企业总用电量
    coreCompanyTotalElectricity = data[data["isCoreCompany"] == 1]
    coreCompanyTotalElectricity = coreCompanyTotalElectricity.groupby(["division", "industryClass"], as_index=False)[
        "sum"].sum()
    coreCompanyTotalElectricity.rename(
        columns={"sum": "keyElectricity", "division": "cctedivision", "industryClass": "ccteIndustryClass"},
        inplace=True)

    results = pd.merge(coreCompany, coreCompanyTotalElectricity, how="left", left_on=["division", "industryClass"],
                       right_on=["cctedivision", "ccteIndustryClass"])
    results.drop(["cctedivision", "ccteIndustryClass"], inplace=True, axis=1)

    #计算重点行业企业数量
    coreIndustry = data.groupby(["division", "industryClass"], as_index=False)["isCoreIndustry"].sum()
    coreIndustry.rename(columns={"isCoreIndustry": "core_industry_sum_l3", "division": "cidivision",
                                 "industryClass": "ciIndustryClass"}, inplace=True)

    results = pd.merge(results, coreIndustry, how="left", left_on=["division", "industryClass"],
                       right_on=["cidivision", "ciIndustryClass"])
    results.drop(["cidivision", "ciIndustryClass"], inplace=True, axis=1)

    #计算重点行业企业用电量
    coreIndustryTotalElectricity = data[data["isCoreIndustry"] == 1]
    coreIndustryTotalElectricity = coreIndustryTotalElectricity.groupby(["division", "industryClass"], as_index=False)[
        "sum"].sum()
    coreIndustryTotalElectricity.rename(
        columns={"sum": "coreElectricity", "division": "citedivision", "industryClass": "citeIndustryClass"},
        inplace=True)

    results = pd.merge(results, coreIndustryTotalElectricity, how="left", left_on=["division", "industryClass"],
                       right_on=["citedivision", "citeIndustryClass"])
    results.drop(["citedivision", "citeIndustryClass"], inplace=True, axis=1)

    #计算大工业企业数量
    bigGongyeQiye = data[data["eleType"] == "大工业用电"]
    bigGongyeQiyeQuantity = bigGongyeQiye.groupby(["division", "industryClass"], as_index=False)[
        "userId"].count()

    bigGongyeQiyeQuantity.rename(
        columns={"division": "bgqdivision", "industryClass": "bgqIndustryClass", "userId": "large_enterprise_sum_l3"},
        inplace=True)
    results = pd.merge(results, bigGongyeQiyeQuantity, how="left", left_on=["division", "industryClass"],
                       right_on=["bgqdivision", "bgqIndustryClass"])
    results.drop(["bgqdivision", "bgqIndustryClass"], inplace=True, axis=1)

    #计算大工业企业用电量
    bigGongyeQiyeElectricity = bigGongyeQiye.groupby(["division", "industryClass"], as_index=False)[
        "sum"].sum()
    bigGongyeQiyeElectricity.rename(
        columns={"division": "bgqedivision", "industryClass": "bgqeIndustryClass", "sum": "largeElectricity"},
        inplace=True)
    results = pd.merge(results, bigGongyeQiyeElectricity, how="left", left_on=["division", "industryClass"],
                       right_on=["bgqedivision", "bgqeIndustryClass"])
    results.drop(["bgqedivision", "bgqeIndustryClass"], inplace=True, axis=1)

    #2019单位产值能耗
    valueConsumption2019 = valueConsumption2019.loc[:, ["industry", "electricity_per_output_2019"]]

    results = pd.merge(results, valueConsumption2019, how="left", left_on="industryClass", right_on="industry")
    results.drop("industry", inplace=True, axis=1)

    # 2017单位产值能耗
    valueConsumption2017 = valueConsumption2017.loc[:, ["industry", "electricity_per_output_2017"]]

    results = pd.merge(results, valueConsumption2017, how="left", left_on="industryClass", right_on="industry")
    results.drop("industry", inplace=True, axis=1)

    #计算月N企业数量
    avgLtValue = data.groupby(["division", "industryClass"], as_index=False)['highElectricityThreshold'].sum().rename(
        columns={"division": "alvdivision", "industryClass": "alvIndustryClass",
                 "highElectricityThreshold": "high_consumption_sum_l3"}
    )
    results = pd.merge(results, avgLtValue, how="left", left_on=["division", "industryClass"],
                       right_on=["alvdivision", "alvIndustryClass"])
    results.drop(["alvdivision", "alvIndustryClass"], axis=1, inplace=True)

    # 计算月N企业用电量
    ltValueData = data[data["highElectricityThreshold"] == 1]
    ltValueElectricity = ltValueData.groupby(["division", "industryClass"], as_index=False)['sum'].sum().rename(
        columns={"division": "lvedivision", "industryClass": "lveIndustryClass", "sum": "highElectricity"}
    )
    results = pd.merge(results, ltValueElectricity, how="left", left_on=["division", "industryClass"],
                       right_on=["lvedivision", "lveIndustryClass"])
    results.drop(["lvedivision", "lveIndustryClass"], axis=1, inplace=True)
    results.drop_duplicates(inplace=True)

    print("计算发展质量完成")
    return results


def __dealVoltage(data, voltage, val2019, val2017):
    voltage["voltageLevel"] = voltage["voltageLevel"].fillna(0).map(lambda x: str(x).strip("交流").strip("kV"))
    voltage["voltageLevel"] = voltage["voltageLevel"].map(lambda x: int(x))

    data = pd.merge(data, voltage, how="left", left_on="userId", right_on="userIdVoltage")
    data.drop(["userIdVoltage"], inplace=True, axis=1)

    #计算大电压企业数量
    highVoltageCompany = data[data["voltageLevel"] > 10]
    highVoltageCompanyCount = highVoltageCompany.groupby(["division", "industryClass"], as_index=False)[
        "userId"].count()
    highVoltageCompanyCount.rename(
        columns={"division": "hvcdivision", "industryClass": "hvcIndustryClass", "userId": "head_voltage_sum_l3"},
        inplace=True)

    #计算大电压企业用电量
    highVoltageCompanyElectricity = highVoltageCompany.groupby(["division", "industryClass"], as_index=False)[
        "sum"].sum()
    highVoltageCompanyElectricity.rename(
        columns={"division": "hvcedivision", "industryClass": "hvceIndustryClass", "sum": "headElectricity"},
        inplace=True)

    highVoltageCompanyResults = pd.merge(highVoltageCompanyCount, highVoltageCompanyElectricity, how="left",
                                         left_on=["hvcdivision", "hvcIndustryClass"],
                                         right_on=["hvcedivision", "hvceIndustryClass"])

    results = __calDevelopQuality(data, val2019, val2017)
    results = pd.merge(results, highVoltageCompanyResults, how="left", left_on=["division", "industryClass"],
                       right_on=["hvcdivision", "hvcIndustryClass"])
    results.drop(["hvcdivision", "hvcIndustryClass", "hvcedivision", "hvceIndustryClass"], axis=1, inplace=True)

    print("计算第一部分完成")
    return results


def finalMerge(data, voltage, val2019, val2017, choice=1):
    if choice == 1:
        columns = [f"month{i}" for i in range(1, 25)]
    elif choice == 2:
        columns = [f"season{i}" for i in range(1, 4)]
    elif choice == 3:
        columns = [f"year{i}" for i in range(1,13)]
    industryScale = calIndustryScaleData(data=data, columns=columns)
    developQuality = __dealVoltage(data, voltage, val2019, val2017)
    results = pd.concat([industryScale, developQuality], axis=1)
    results.fillna(0, inplace=True)
    results = results.loc[:, ~results.columns.duplicated()]

    results["enterprise_proportion_l3"] = results["industryCompany"] / results["areaCompanyTotal"]
    results["proportion_district_l3"] = results["sum"] / results["areaElectricityTotal"]
    results["proportion_industry_l3"] = results["sum"] / results["industryElectricityTotal"]
    results["key_enterprise_proportion_l3"] = results["key_enterprise_sum_l3"] / results["companyTotal"]
    results["key_electricity_proportion_l3"] = results["keyElectricity"] / results["sum"]
    results["core_industry_proportion_l3"] = results["core_industry_sum_l3"] / results["companyTotal"]
    results["core_electricity_proportion_l3"] = results["coreElectricity"] / results["sum"]
    results["large_enterprise_proportion_l3"] = results["large_enterprise_sum_l3"] / results["companyTotal"]
    results["large_electricity_proportion_l3"] = results["largeElectricity"] / results["sum"]
    results["high_consumption_proportion_l3"] = results["high_consumption_sum_l3"] / results["companyTotal"]
    results["high_electricity_proportion_l3"] = results["highElectricity"] / results["sum"]
    results["head_voltage_proportion_l3"] = results["head_voltage_sum_l3"] / results["companyTotal"]
    results["head_electricity_proportion_l3"] = results["headElectricity"] / results["sum"]
    results["per_output_growth_l3"] = results["electricity_per_output_2019"] / results[
        "electricity_per_output_2017"] - 1
    results["output_sum_l3"] = results["sum"] * results["electricity_per_output_2019"]
    areaEco = results.groupby("division")["output_sum_l3"].sum().reset_index()
    areaEco.rename(columns={"division": "aedivision", "output_sum_l3": "areaValueTotal"}, inplace=True)
    results = pd.merge(results, areaEco, how="left", left_on="division", right_on="aedivision")
    results.drop("aedivision", axis=1, inplace=True)
    results["output_proportion_l3"] = results["output_sum_l3"] / results["areaValueTotal"]


    results.rename(columns={"sum": "electricity_sum_l3", "monthAvgElectricity": "electricity_avg_l3",
                            "maxmax": "electricity_max_l3",
                            "max": "electricity_sum_max_l3", "industryCompany": "enterprise_sum_l3",
                            "key_enterprise_sum_l3": "key_enterprise_sum_l3",
                            "core_industry_sum_l3": "core_industry_sum_l3",
                            "large_enterprise_sum_l3": "large_enterprise_sum_l3",
                            "high_consumption_sum_l3": "high_consumption_sum_l3",
                            "head_voltage_sum_l3": "head_voltage_sum_l3",
                            "electricity_per_output_2019": "electricity_per_output_l3"}, inplace=True)


    columns = ["division", "industryClass", "electricity_sum_l3", "electricity_avg_l3", "electricity_max_l3",
               "electricity_sum_max_l3", "output_sum_l3", "output_proportion_l3", "enterprise_sum_l3",
               "enterprise_proportion_l3", "proportion_district_l3", "proportion_industry_l3",
               "key_enterprise_sum_l3", "key_enterprise_proportion_l3", "key_electricity_proportion_l3",
               "core_industry_sum_l3", "core_industry_proportion_l3", "core_electricity_proportion_l3",
               "large_enterprise_sum_l3", "large_enterprise_proportion_l3", "large_electricity_proportion_l3",
               "high_consumption_sum_l3", "high_consumption_proportion_l3", "high_electricity_proportion_l3",
               "head_voltage_sum_l3", "head_voltage_proportion_l3", "head_electricity_proportion_l3",
               "electricity_per_output_l3", "per_output_growth_l3"]
    inputData = results[columns]
    return inputData
