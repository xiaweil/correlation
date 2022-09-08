# @Time     : 2022/9/8 10:01
# @Auther   : Fukexin
# @Filename : base_index_convert.py


import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import DATE,CHAR,VARCHAR
from package.db_connect import connect

def index_convert(input_data_1):

    input_data = input_data_1.copy()

    # 输入表准备
    #读取指标基本信息
    engine = connect.mysql_engine()

    index_base = pd.read_sql('select * from index_info_base', engine)
    # 修改底层指标列名
    input_data.rename(columns = {'division':'district', 'industryClass':'std_industry_name'},inplace = True)

    # 将占比、增长率数值统一为百分比
    #将占比转换为百分比
    pct_list = list(index_base[index_base['pct_change']==1]['index_code'])
    #'enterprise_proportion_l3'
    for i in pct_list:
        input_data[i] = input_data[i]*100

    #将kwh转换为mwh
    kwh_list = list(index_base[index_base['kwh_change']==1]['index_code'])
    for j in kwh_list:
        input_data[j] = input_data[j]/1000

    #将元转换为万元
    input_data['output_sum_l3'] = input_data['output_sum_l3']/10000

    # 指标转置
    #转置
    result_cov = pd.DataFrame()
    for k in list(input_data['district'].unique()):
        data_d = input_data[input_data['district']==k]
        del data_d['district']
        data_d.set_index(['std_industry_name'], inplace = True)
        covert = data_d.stack().reset_index()
        covert.columns = ['std_industry_name', 'index_code', 'index_val']
        covert['district'] = k

        result_cov = pd.concat([result_cov, covert])
        result_cov = result_cov[['district', 'std_industry_name', 'index_code', 'index_val']]

    #修改nan→none
    result_cov.loc[result_cov['index_val'].isnull(), 'index_val'] = None

    # 写表
    # 清空数据表
    # con, cursor = connect.mysql_con()
    # try:
    #     cursor.execute('TRUNCATE table index_detail')
    # except Exception as err:
    #     print(err)
    result_cov.to_sql('index_detail', con=engine, index=False, chunksize=1000, if_exists='append')