# coding:utf8
import numpy as np
import pandas as pd
from factor_analyzer import FactorAnalyzer
from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity, calculate_kmo
from sklearn.preprocessing import MinMaxScaler


def __processingData(data):
    model = MinMaxScaler(feature_range=(0, 100))
    result = model.fit_transform(data)
    return result

def __checkModel(data):

    data = __processingData(data)

    # chi_square_value, p_value = calculate_bartlett_sphericity(data)
    # print("p值为：", p_value)
    #
    # kmo_all, kmo_model = calculate_kmo(data)
    # print("kmo值为：", kmo_model)
    # if p_value < 0.05 and kmo_model >= 0.5:
    #     __factorAnalysis(data)
    # else:
    #     print("数据不适合做因子分析")

    return __factorAnalysis(data)


def __factorAnalysis(data):

    # fa = FactorAnalyzer(1, rotation=None)
    # print("因子分析权重")
    # print(pd.DataFrame(data))
    # fa.fit(data)
    # A = fa.loadings_  # 计算因子载荷阵

    # weight = np.array(abs(A))
    #
    weight = np.random.random((len(data[0]), 1))

    ratio = []
    sumWeight = 0
    for i in range(len(weight)):
        sumWeight += weight[i][0]
    for i in range(len(weight)):
        ratio.append([weight[i][0] / sumWeight])


    title = ''
    for i in range(len(ratio)):
        title += str(round(ratio[i][0], 2))
        title += ';'
    scores = np.dot(data, ratio)

    scores = pd.DataFrame(scores, columns=[title])

    return scores


def mainV2(data):
    data['output_sum_l3'] = data['output_sum_l3'].apply(lambda x: np.log10(x))
    data['electricity_sum_l3'] = data['electricity_sum_l3'].apply(lambda x: np.log10(x))
    data['electricity_avg_l3'] = data['electricity_avg_l3'].apply(lambda x: np.log10(x))
    data['electricity_max_l3'] = data['electricity_max_l3'].apply(lambda x: np.log10(x))
    data['electricity_sum_max_l3'] = data['electricity_sum_max_l3'].apply(lambda x: np.log10(x))
    node = [2, 12, 28, 34, 39]
    results = data.iloc[:, 0:2]
    for i in range(len(node) - 1):
        nodeData = data.iloc[:, node[i]:node[i + 1]]
        print(f"加载第{i+1}节点数据完成")
        result = __checkModel(nodeData)
        results = pd.concat([results, result], axis=1)
    results.to_csv("../../data/weight.csv", index=False)
    return results

def mainVs(data):
    node = [2, 12]
    results = data.iloc[:, 0:2]
    for i in range(len(node) - 1):
        print(f"加载季节第{i + 1}节点数据完成")
        nodeData = data.iloc[:, node[i]:node[i + 1]]
        result = __checkModel(nodeData)
        results = pd.concat([results, result], axis=1)
    return results