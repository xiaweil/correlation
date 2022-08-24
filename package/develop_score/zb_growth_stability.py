#!/usr/bin/env python
# coding: utf-8

import pandas as pd

import warnings
warnings.filterwarnings("ignore")

# 映射关系的相关信息
yingshe = [201910, 201911, 201912, 202001, 202002, 202003, 202004, 202005, 202006, 202007, 202008, 202009, 202010,
           202011, 202012, 202101, 202102, 202103, 202104, 202105, 202106, 202107, 202108, 202109]


def __func(i, j, k, df1):
    # 计算月增速
    # 需要判断是否要做计算
    df1[k] = (df1[j] - df1[i]) / df1[i] * 100
    # 查看['202101'] != 0的数据分布，填充数据更小的值（原分布是无穷小）
    yuzhi = df1[df1[i] != 0][k].describe(percentiles=[.01, .03, .05, .1, .2, .3, .4, .5, .6, .7, .8, .9, .95, .97, .99])[17]
    # 取分布在3%的地方来填充无穷小的数
    df1.loc[(df1[k] > yuzhi), k] = yuzhi
    # '202101'和'202001'均为0时，用0填充
    df1.loc[((df1[i] == 0) & (df1[j] == 0)), k] = 0
    return df1


def __func_yf(df1, yingshe):
    # 确认月份
    # 冬季
    m1 = []
    m2 = []
    # 夏季
    m7 = []
    m8 = []
    # 平时
    m4 = []
    m5 = []
    m10 = []
    m11 = []

    for i in range(24):
        zuihou = str(yingshe[i])[-2:]
        jijie = df1.columns.values[1:25][i]
        if zuihou == '01':
            m1.append(jijie)
        else:
            if zuihou == '02':
                m2.append(jijie)
            else:
                if zuihou == '04':
                    m4.append(jijie)
                else:
                    if zuihou == '05':
                        m5.append(jijie)
                    else:
                        if zuihou == '07':
                            m7.append(jijie)
                        else:
                            if zuihou == '08':
                                m8.append(jijie)
                            else:
                                if zuihou == '10':
                                    m10.append(jijie)
                                else:
                                    if zuihou == '11':
                                        m11.append(jijie)
    return m1, m2, m4, m5, m7, m8, m10, m11


def __func_zyfzb(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12):
    # 正增速的月份个数
    x = [x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12]
    count = 0
    for i in range(12):
        if x[i] > 0:
            count += 1
    return count


def __func_zb(df, yingshe):
    # 找出第一年用电量全为空或者第二年用电量全为空的企业，删掉
    df['is_null'] = df.apply(lambda x: 1 if ((str(x['month1']) == 'nan' and str(x['month2']) == 'nan' and str(
        x['month3']) == 'nan' and str(x['month4']) == 'nan' and str(x['month5']) == 'nan' and str(
        x['month6']) == 'nan' and str(x['month7']) == 'nan' and str(x['month8']) == 'nan' and str(
        x['month9']) == 'nan' and str(x['month10']) == 'nan' and str(x['month11']) == 'nan' and str(
        x['month12']) == 'nan') or (str(x['month13']) == 'nan' and str(x['month14']) == 'nan' and str(
        x['month15']) == 'nan' and str(x['month16']) == 'nan' and str(x['month17']) == 'nan' and str(
        x['month18']) == 'nan' and str(x['month19']) == 'nan' and str(x['month20']) == 'nan' and str(
        x['month21']) == 'nan') and str(x['month22']) == 'nan' and str(x['month23']) == 'nan' and str(
        x['month24']) == 'nan') else 0, axis=1)
    df1 = df[df['is_null'] == 0]
    df1.fillna(0, inplace=True)

    # 企业的年累计用电量year1的1-12月【sum_year1】和year1的1-12月【sum_year2】
    df1['sum_year1'] = df1.iloc[:, 1:13].apply(lambda x: x.sum(), axis=1)
    df1['sum_year2'] = df1.iloc[:, 13:25].apply(lambda x: x.sum(), axis=1)
    # 企业的年累计增速
    df1['年累计增速'] = (df1['sum_year2'] - df1['sum_year1']) / df1['sum_year1'] * 100
    yuzhi = df1[df1['sum_year1'] != 0]['年累计增速'].describe(
        percentiles=[.01, .03, .05, .1, .2, .3, .4, .5, .6, .7, .8, .9, .95, .97, .99])[17]
    df1.loc[(df1['年累计增速'] > yuzhi), '年累计增速'] = yuzhi
    # 用上面数据【['sum_2021'] ！=0】条件下3%处的数据作为分布下限（原分布是无穷小），填充取值更小的数据

    # 产业增速
    # (1)年累计增速
    ###【区域 + 国民经济大类】下的【年累计增速】
    df1_all = df1[['division', 'industryClass', '年累计增速']].groupby(['division', 'industryClass']).mean()
    df1_all.reset_index(drop=False, inplace=True)

    # (2)年累计高增速企业占比
    # 可以根据分布来定义高速增长企业
    # df1['年累计增速'].describe(percentiles = [.01,.03,.05,.1,.2,.5,.6,.7,.8,.9,.92,.93,.95,.96,.99])
    # 取占全企业前8%的为高增速企业
    df1['is_kuai'] = df1['年累计增速'].apply(lambda x: 1 if x >= 50 else 0)
    # 对【'division', '国民经济行业大类（电力行业匹配）'】分组并对【'is_kuai'】求和获取每类下的【高增速企业】个数
    df3 = df1[['division', 'industryClass', 'is_kuai']].groupby(['division', 'industryClass']).sum()
    df3.reset_index(drop=False, inplace=True)
    # 对【'行政区划', '国民经济行业大类（电力行业匹配）'】分组并对【'userId'】求和获取每类下的【高增速企业】个数
    df2 = df1[['division', 'industryClass', 'userId']].groupby(['division', 'industryClass']).count()
    df2.reset_index(drop=False, inplace=True)
    df3['counts'] = df2['userId']
    df3['年累计高增速企业占比'] = df3['is_kuai'] / df3['counts'] * 100

    df3['all'] = df3.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df1_all['all'] = df1_all.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df_final = pd.merge(df3[['division', 'industryClass', '年累计高增速企业占比', 'all']], df1_all[['年累计增速', 'all']],
                        left_on='all', right_on='all', how='outer')
    df_final.rename(columns={'年累计增速': 'growth_rate_yearly_l3', '年累计高增速企业占比': 'high_growth_yearly_l3'}, inplace=True)

    # 月平均增速（同比）
    # 下面计算的时候直接用上面做过数据删除和缺失值填充的数据
    df1 = df[df['is_null'] == 0]
    queshi_columns = df1.iloc[:, 1:25].loc[:, df1.iloc[:, 1:25].isnull().all()].columns
    m1 = list(set(df1.iloc[:, 1:25].columns.values) - set(queshi_columns.values))
    for i in m1:
        df1[i].fillna(0, inplace=True)
    # 整体会出问题
    # df1[['month10','month11']].fillna(0,axis=0,inplace=True)
    columns_dict = {'month1': ['month13', '月增速_1'], 'month2': ['month14', '月增速_2'], 'month3': ['month15', '月增速_3'],
                    'month4': ['month16', '月增速_4'], 'month5': ['month17', '月增速_5'], 'month6': ['month18', '月增速_6'],
                    'month7': ['month19', '月增速_7'], 'month8': ['month20', '月增速_8'], 'month9': ['month21', '月增速_9'],
                    'month10': ['month22', '月增速_10'], 'month11': ['month23', '月增速_11'],
                    'month12': ['month24', '月增速_12']}
    m = ['month1', 'month2', 'month3', 'month4', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11',
         'month12']

    # 计算月增速
    for i in m:
        __func(i, columns_dict[i][0], columns_dict[i][1], df1)
    m2 = df1.iloc[:, -12:].loc[:, df1.iloc[:, -12:].isnull().all()].columns.values
    # 12个增速中缺失的列数
    le = len(df1.iloc[:, -12:].loc[:, df1.iloc[:, -12:].isnull().all()].columns)
    for i in m2:
        df1[i].fillna(0, inplace=True)
    # 这里的参数是9个月
    # 按一年汇总
    yf = 12 - le

    df1['月平均增速（同比）'] = (df1['月增速_1'] + df1['月增速_2'] + df1['月增速_3'] + df1['月增速_4'] + df1['月增速_5'] + df1['月增速_6'] + df1[
        '月增速_7'] + df1['月增速_8'] + df1['月增速_9'] + df1['月增速_10'] + df1['月增速_11'] + df1['月增速_12']) / yf

    # (3)月平均增速（同比）
    # 【区域 + 国民经济大类】下的【月平均增速（同比）】
    df0_all = df1[['division', 'industryClass', '月平均增速（同比）']].groupby(['division', 'industryClass']).mean()
    df0_all.reset_index(drop=False, inplace=True)
    df0_all['all'] = df0_all.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df_final1 = pd.merge(df_final, df0_all[['all', '月平均增速（同比）']], left_on='all', right_on='all', how='outer')
    df_final1.rename(columns={'月平均增速（同比）': 'growth_rate_yoy_l3'}, inplace=True)

    # (4)月最大增速（同比）
    df1['月最大增速（同比）'] = df1.apply(
        lambda x: max(x['月增速_1'], x['月增速_2'], x['月增速_3'], x['月增速_4'], x['月增速_5'], x['月增速_6'], x['月增速_7'], x['月增速_8'],
                      x['月增速_9'], x['月增速_10'], x['月增速_11'], x['月增速_12']), axis=1)
    # 【区域 + 国民经济大类】下的【月最大增速（同比）】
    df0_all = df1[['division', 'industryClass', '月最大增速（同比）']].groupby(['division', 'industryClass']).mean()
    df0_all.reset_index(drop=False, inplace=True)
    df0_all['all'] = df0_all.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df_final2 = pd.merge(df_final1, df0_all[['all', '月最大增速（同比）']], left_on='all', right_on='all', how='outer')
    df_final2.rename(columns={'月最大增速（同比）': 'growth_rate_max_l3'}, inplace=True)

    # (5)增长月份占比(正月份占比)
    df1['增长月份占比'] = list(
        map(__func_zyfzb, df1['月增速_1'], df1['月增速_2'], df1['月增速_3'], df1['月增速_4'], df1['月增速_5'], df1['月增速_6'],
            df1['月增速_7'], df1['月增速_8'], df1['月增速_9'], df1['月增速_10'], df1['月增速_11'], df1['月增速_12']))
    df1['增长月份占比'] = df1['增长月份占比'] / yf * 100
    # 【区域 + 国民经济大类】下的【年累计增速】
    df0_all = df1[['division', 'industryClass', '增长月份占比']].groupby(['division', 'industryClass']).mean()
    df0_all.reset_index(drop=False, inplace=True)
    df0_all['all'] = df0_all.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df_final3 = pd.merge(df_final2, df0_all[['all', '增长月份占比']], left_on='all', right_on='all', how='outer')
    df_final3.rename(columns={'增长月份占比': 'growth_month_proportion_l3'}, inplace=True)

    # (6)月平均高增速企业占比
    # 根据下面企业级的['月平均增速（同比）']查看前8%的企业级
    # df1['月平均增速（同比）'].describe(percentiles = [.1,.2,.4,.6,.7,.8,.9,.91,.92,.93,.94])
    df1['is_kuai'] = df1['月平均增速（同比）'].apply(lambda x: 1 if x >= 50 else 0)
    df3 = df1[['division', 'industryClass', 'is_kuai']].groupby(['division', 'industryClass']).sum()
    df3.reset_index(drop=False, inplace=True)
    df2 = df1[['division', 'industryClass', 'userId']].groupby(['division', 'industryClass']).count()
    df2.reset_index(drop=False, inplace=True)
    df3['counts'] = df2['userId']
    df3['月平均高增速企业占比'] = df3['is_kuai'] / df3['counts'] * 100
    df3['all'] = df3.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df_final4 = pd.merge(df_final3, df3[['all', '月平均高增速企业占比']], left_on='all', right_on='all', how='outer')
    df_final4.rename(columns={'月平均高增速企业占比': 'high_growth_yoy_l3'}, inplace=True)

    # 产业稳定性
    # 获取特定月份
    m1, m2, m4, m5, m7, m8, m10, m11 = __func_yf(df1, yingshe)
    df0 = df.fillna(0)
    df1 = df0.groupby(['division', 'industryClass']).sum()
    df1.reset_index(drop=False, inplace=True)
    # 年度
    df1['sum_year1'] = df1.iloc[:, 2:14].apply(lambda x: x.sum(), axis=1)
    df1['sum_year2'] = df1.iloc[:, 14:].apply(lambda x: x.sum(), axis=1)
    # 冬季
    df1['wsum_year1'] = df1[m1[0]] + df1[m2[0]]
    df1['wsum_year2'] = df1[m1[1]] + df1[m2[1]]
    # 夏季
    df1['ssum_year1'] = df1[m7[0]] + df1[m8[0]]
    df1['ssum_year2'] = df1[m7[1]] + df1[m8[1]]
    # 平时
    df1['osum_year1'] = df1[m4[0]] + df1[m5[0]] + df1[m10[0]] + df1[m11[0]]
    df1['osum_year2'] = df1[m4[1]] + df1[m5[1]] + df1[m10[1]] + df1[m11[1]]

    df2 = df0.groupby(['division']).sum()
    df2.reset_index(drop=False, inplace=True)
    # 年度
    df2['sum_year1s'] = df2.iloc[:, 1:13].apply(lambda x: x.sum(), axis=1)
    df2['sum_year2s'] = df2.iloc[:, 13:].apply(lambda x: x.sum(), axis=1)
    # 冬季
    df2['wsum_year1s'] = df2[m1[0]] + df2[m2[0]]
    df2['wsum_year2s'] = df2[m1[1]] + df2[m2[1]]
    # 夏季
    df2['ssum_year1s'] = df2[m7[0]] + df2[m8[0]]
    df2['ssum_year2s'] = df2[m7[1]] + df2[m8[1]]
    # 平时
    df2['osum_year1s'] = df2[m4[0]] + df2[m5[0]] + df2[m10[0]] + df2[m11[0]]
    df2['osum_year2s'] = df2[m4[1]] + df2[m5[1]] + df2[m10[1]] + df2[m11[1]]
    df3 = pd.merge(df1[['division', 'industryClass', 'sum_year1', 'sum_year2', 'wsum_year1', 'wsum_year2', 'ssum_year1',
                        'ssum_year2', 'osum_year1', 'osum_year2']], df2[
                       ['division', 'sum_year1s', 'sum_year2s', 'wsum_year1s', 'wsum_year2s', 'ssum_year1s',
                        'ssum_year2s', 'osum_year1s', 'osum_year2s']], left_on='division', right_on='division',
                   how='left')

    # (1)年度占比变化
    # 检查下面分母为0的个数
    # len(df3[(df3['sum_2021s'] == 0) | (df3['sum_2020s'] == 0)])
    df3['change_yearly_l3'] = df3['sum_year2'] / df3['sum_year2s'] * 100 - df3['sum_year1'] / df3['sum_year1s'] * 100
    # 需要考虑['sum_2021s']和['sum_2020s']为0的情况
    # (2)冬时占比变化
    df3['change_winter_l3'] = df3['wsum_year2'] / df3['wsum_year2s'] * 100 - df3['wsum_year1'] / df3[
        'wsum_year1s'] * 100
    # (3)夏时占比变化
    df3['change_summer_l3'] = df3['ssum_year2'] / df3['ssum_year2s'] * 100 - df3['ssum_year1'] / df3[
        'ssum_year1s'] * 100
    # (4)平时占比变化
    df3['change_others_l3'] = df3['osum_year2'] / df3['osum_year2s'] * 100 - df3['osum_year1'] / df3[
        'osum_year1s'] * 100
    df3['all'] = df3.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)
    df_final5 = pd.merge(df_final4,
                         df3[['all', 'change_yearly_l3', 'change_winter_l3', 'change_summer_l3', 'change_others_l3']],
                         left_on='all', right_on='all', how='outer')

    # 2、产业企业迁移情况
    # (1)企业变化占比（绝对迁移）
    # (2)企业变化数（绝对迁移）
    # (3)企业变化占比(相对迁移）
    # (4)企业变化数（相对迁移）
    df.fillna(0, inplace=True)

    # 变化企业数（绝对迁移）
    df['sum_year1'] = df.iloc[:, 1:13].apply(lambda x: x.sum(), axis=1)
    df['sum_year2'] = df.iloc[:, 13:25].apply(lambda x: x.sum(), axis=1)
    # 取绝对值
    df['is_ydbh'] = df.apply(lambda x: 1 if (((x['sum_year1'] == 0) and (x['sum_year2'] != 0)) or (
                (x['sum_year1'] != 0) and (x['sum_year2'] == 0))) else 0, axis=1)
    df1 = df[['division', 'industryClass', 'is_ydbh']].groupby(['division', 'industryClass']).sum()
    df1.reset_index(drop=False, inplace=True)
    df1.rename(columns={'is_ydbh': 'change_user_a_sum_l3'}, inplace=True)
    df1['all'] = df1.apply(lambda x: x['division'] + x['industryClass'], axis=1)

    # 变化企业数（相对迁移）
    # 不取绝对值
    df['is_ydbh_s'] = df.apply(lambda x: 1 if ((x['sum_year1'] == 0) and (x['sum_year2'] != 0)) else -1 if (
                (x['sum_year1'] != 0) and (x['sum_year2'] == 0)) else 0, axis=1)
    df1_s = df[['division', 'industryClass', 'is_ydbh_s']].groupby(['division', 'industryClass']).sum()
    df1_s.reset_index(drop=False, inplace=True)
    df1_s.rename(columns={'is_ydbh_s': 'change_user_r_sum_l3'}, inplace=True)
    df1_s['all'] = df1_s.apply(lambda x: x['division'] + x['industryClass'], axis=1)

    # 【区域+国民经济大类】下的企业数
    df2 = df[['division', 'industryClass', 'userId']].groupby(['division', 'industryClass']).count()
    df2.reset_index(drop=False, inplace=True)
    df2.rename(columns={'userId': 'user_counts'}, inplace=True)
    df2['all'] = df2.apply(lambda x: str(x['division']) + str(x['industryClass']), axis=1)

    df0 = pd.merge(df1, df2[['user_counts', 'all']], left_on='all', right_on='all', how='outer')
    df0['change_user_a_proportion_l3'] = df0['change_user_a_sum_l3'] / df0['user_counts'] * 100
    df_final6 = pd.merge(df_final5, df0[['all', 'change_user_a_sum_l3', 'change_user_a_proportion_l3']], left_on='all',
                         right_on='all', how='outer')

    df0_s = pd.merge(df1_s, df2[['user_counts', 'all']], left_on='all', right_on='all', how='outer')
    df0_s['change_user_r_proportion_l3'] = df0_s['change_user_r_sum_l3'] / df0_s['user_counts'] * 100
    df_final7 = pd.merge(df_final6, df0_s[['all', 'change_user_r_sum_l3', 'change_user_r_proportion_l3']],
                         left_on='all', right_on='all', how='outer')

    df_final7.drop(['all'], inplace=True, axis=1)
    df_final7 = df_final7[
        ['division', 'industryClass', 'growth_rate_yearly_l3', 'growth_rate_yoy_l3', 'growth_rate_max_l3',
         'growth_month_proportion_l3', 'high_growth_yearly_l3', 'high_growth_yoy_l3', 'change_yearly_l3',
         'change_winter_l3', 'change_summer_l3', 'change_others_l3', 'change_user_a_sum_l3',
         'change_user_a_proportion_l3', 'change_user_r_sum_l3', 'change_user_r_proportion_l3']]

    return df_final7

def calFunZb(data):
    data.drop(['sum', 'max', 'isCoreCompany', 'eleType', 'period', 'mean', 'keyIndustryClass'], inplace=True, axis=1)
    # 列的顺序不能变
    data.columns = ['userId', 'month1', 'month2', 'month3', 'month4', 'month5', 'month6', 'month7', 'month8', 'month9',
                  'month10', 'month11', 'month12', 'month13', 'month14', 'month15', 'month16', 'month17', 'month18',
                  'month19', 'month20', 'month21', 'month22', 'month23', 'month24', 'division', 'industryClass']
    final = __func_zb(data, yingshe)
    return final


