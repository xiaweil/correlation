#!/usr/bin
# encoding: utf-8
"""=========================
@author: Bruce
@contact: wsq8932419@163.com
@software: PyCharm
@file: update.py
@time:  2020/10/16 13:58
=============================="""

import time
import datetime
import pandas as pd
from package.db_connect.connect import mysql_engine, mysql_con

def save_result(data):
    """
    保存原数据分析结果
    :param data:
    :return:
    """
    now = int(time.time())
    engine = mysql_engine()
    data.to_sql('res_table_'+str(now), con=engine, if_exists='replace',index=False)
    engine.dispose()
    res_table = 'res_table_'+str(now)
    return res_table


def save_algorithm_result(task_id,res_table,res_columns,res_columns_des):
    """
    保存算法结果
    :param data:
    :return:
    """
    #获取算法表里的所有task_id
    engine = mysql_engine()
    task_id_all = pd.read_sql("select task_id from algorithm_result", con=engine)['task_id']
    if task_id in task_id_all.values:
        #如果task_id存在，更新数据，覆盖前一个：
        update_sql_ = """UPDATE algorithm_result SET table_name='{}',field = '{}',description = '{}',create_date = now(),update_date = now()  where  task_id = {}""".format(res_table,res_columns,res_columns_des,task_id)
        conn, cursor = mysql_con()
        cursor.execute(update_sql_)
        conn.commit()
        cursor.close()
        conn.close()
    else:
        # 如果task_id不存在，插入新的数据：
        algorithm_res = {'task_id':task_id,'table_name':res_table,'field':res_columns,'description':res_columns_des,'create_date':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'update_date':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        algorithm_res = pd.DataFrame.from_dict(algorithm_res, orient='index').T
        algorithm_res.to_sql('algorithm_result', con=engine, if_exists="append",index=False)
        engine.dispose()

def update_task_details(task_id,res_table):
    update_sql = """UPDATE m_analysis_task SET end_time=now(), error="无", res_table="{}", state=2 WHERE id={}""".format(res_table,task_id)
    conn, cursor = mysql_con()
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()
    conn.close()

def update_error(e,task_id):
    update_sql = """UPDATE m_analysis_task SET end_time=now(), error="{}", state=3 WHERE id={}""".format(e,task_id)
    update_sql.replace("'", '"')
    conn, cursor = mysql_con()
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()
    conn.close()