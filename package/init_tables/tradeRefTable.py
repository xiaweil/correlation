# @Time     : 2022/9/7 17:58
# @Auther   : Fukexin
# @Filename : tradeRefTable.py

import pandas as pd
import numpy as np
import re
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import DATE,CHAR,VARCHAR
from package.db_connect import connect

def trade_code_ref(ref_17, ref_02, industry, ref_02_17):
    #拼接02年行业编码
    r1 = industry.merge(ref_02, on = 'tradecode', how = 'left')
    #保留拼接成功部分
    r1_s = r1[r1['sector'].notnull()]
    print('round 1: ' + str(r1_s.shape[0]))

    #筛选拼接不成功部分，与17年行业编码拼接
    r1_na = r1[r1['sector'].isnull()]
    del r1_na['sector']
    del r1_na['std_industry_name']
    r2 = r1_na.merge(ref_17, on = 'tradecode', how = 'left')
    #保留拼接成功部分
    r2_s = r2[r2['sector'].notnull()]
    print('round 2: ' + str(r2_s.shape[0]))

    #筛选拼接不成功部分，与17年行业名称拼接
    r2_na = r2[r2['sector'].isnull()]
    del r2_na['sector']
    del r2_na['std_industry_name']
    #处理名称中的中文符号和数字
    r2_na['sector'] = r2_na['tradename'].apply(lambda x: re.sub(u"([\x00-\xff])","",str(x), flags = re.I|re.S))
    r3 = r2_na.merge(ref_17[['sector','std_industry_name']], on = 'sector', how = 'left')
    #仅保留拼接成功部分
    r3_s = r3[r3['std_industry_name'].notnull()]
    print('round 3: ' + str(r3_s.shape[0]))

    #汇总拼接结果
    tradecode_ref = pd.concat([r1_s, r2_s, r3_s])

    #替换大类名称
    tradecode_ref_r = tradecode_ref.merge(ref_02_17, left_on = 'std_industry_name', right_on = 'name_2002', how = 'left')
    tradecode_ref_e = tradecode_ref_r[tradecode_ref_r['name_2002'].isnull()]
    tradecode_ref_e = tradecode_ref_e[['tradecode', 'tradename', 'sector', 'std_industry_name']]
    tradecode_ref_s = tradecode_ref_r[tradecode_ref_r['name_2002'].notnull()]
    tradecode_ref_s = tradecode_ref_s[['tradecode', 'tradename', 'sector', 'name_2017']]
    tradecode_ref_s.rename(columns = {'name_2017':'std_industry_name'}, inplace = True)
    tradecode_ref_m = pd.concat([tradecode_ref_e, tradecode_ref_s])

    #删除sector为空的部分
    tradecode_ref_final = tradecode_ref_m[tradecode_ref_m['sector']!='']
    tradecode_ref_final = tradecode_ref_final[['tradecode', 'sector','std_industry_name']]

    #trade_code结果去重
    tradecode_ref_final_1 = tradecode_ref_final[tradecode_ref_final['std_industry_name'].notnull()]
    tradecode_ref_final_1.drop_duplicates(subset = ['tradecode', 'sector'], inplace = True)
    tradecode_ref_final_2 = tradecode_ref_final[tradecode_ref_final['std_industry_name'].isnull()]
    tradecode_ref_final_2.drop_duplicates(subset = ['tradecode'], inplace = True)
    tradecode_ref_final_s = pd.concat([tradecode_ref_final_1, tradecode_ref_final_2])

    #修改nan→none
    tradecode_ref_final_s.loc[tradecode_ref_final_s['tradecode'].isnull(), 'tradecode'] = None
    tradecode_ref_final_s.loc[tradecode_ref_final_s['sector'].isnull(), 'sector'] = None
    tradecode_ref_final_s.loc[tradecode_ref_final_s['std_industry_name'].isnull(), 'std_industry_name'] = None

    return tradecode_ref_final_s

if __name__ == '__main__':
    # 读表
    engine = connect.mysql_engine()

    #trade_code和trade_name原始表
    sql_industry = '''
    SELECT DISTINCT  trade_code as tradecode, trade_name as tradename
    FROM f_app_l0
    '''
    industry = pd.read_sql(sql_industry, engine)

    #映射表
    ref_17 = pd.read_sql('select * from ref_17', engine)
    ref_02 = pd.read_sql('select * from ref_02', engine)
    ref_02_17 = pd.read_sql('select * from ref_02_17', engine)
    # industry = pd.read_sql('select * from trade_raw', engine)

    # 建立tradecode映射表
    result = trade_code_ref(ref_17, ref_02, industry, ref_02_17)

    # 写表
    # 清空数据表
    con, cursor = connect.mysql_con()
    # try:
    #     cursor.execute('TRUNCATE table tradecode_ref')
    # except Exception as err:
    #     print(err)
    result.to_sql('tradecode_ref', con=engine, index=False, chunksize=1000, if_exists='append')
    engine.dispose()







