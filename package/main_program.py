#!/usr/bin
# encoding: utf-8
"""=========================
@author: Bruce
@contact: wsq8932419@163.com
@software: PyCharm
@file: main_program.py
@time: 2020/7/21 13:42
=============================="""
import os
import sys
# BASE_DIR = os.path.dirname(os.path.dirname(__file__))
# print(BASE_DIR)
# sys.path.append(BASE_DIR)
sys.path.append("/root/intelligent_audit")

import pandas as pd
from package.abnormal_detection.knn import knn_detection
from package.abnormal_detection.abod import abod_detection
from package.abnormal_detection.hbos import hbos_detection
from package.abnormal_detection.iforest import iforest_detection
from package.abnormal_detection.loda import loda_detection
from package.abnormal_detection.lof import lof_detection
from package.abnormal_detection.mcd import mcd_detection
from package.abnormal_detection.ocsvm import ocsvm_detection
from package.abnormal_detection.pca import pca_detection
from package.abnormal_detection.topk import topk_detection
from package.abnormal_detection.gt import gt_detection
from package.abnormal_detection.cnn import multi_cnn
from package.abnormal_detection.light_load import light_load_detection
from package.abnormal_detection.overload import recent_overload_detection
from package.abnormal_detection.overload_delay import delay_overload_detection
from package.abnormal_detection.pays_off import pays_off
from package.data_preprocess.credential import read_stat_data, read_credential_data, read_cnn_data
from package.data_preprocess.credential import read_stat_data, read_credential_data
from package.db_connect.connect import mysql_engine

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

def algorithm_dict():
    """
    返回算法id和算法实现的函数名组成的字典
    :return:
    """
    engine = mysql_engine()
    df = pd.read_sql('select id,func_name from m_algorithm_details where deleted=0', con=engine)
    engine.dispose()
    dic = df.set_index('id').T.to_dict('records')
    return dic[0]

def model_dict():
    """
    返回算法id和算法实现的函数名组成的字典
    :return:
    """
    engine = mysql_engine()
    df = pd.read_sql('select id,func_name from m_model_details where deleted=0', con=engine)
    engine.dispose()
    dic = df.set_index('id').T.to_dict('records')
    return dic[0]


def param_dict(task_id):
    """
    返回算法id和算法实现的函数名组成的字典
    :return:
    """
    engine = mysql_engine()
    df = pd.read_sql('select name,default_val from m_task_param where task_id={}'.format(task_id), con=engine)
    engine.dispose()
    dic = dict(zip(df['name'], df['default_val']))
    return dic


def main_program(task_id, algorithm_or_model, algorithm_or_model_id):
    """
    :param task_id:任务id
    :param algorithm_or_model:模型
    :param algorithm_or_model_id:模型id
    :return:
    """
    model_dicts = model_dict()
    algorithm_dicts = algorithm_dict()

    # if algorithm_or_model == 'algorithm':
    #     param_dicts = param_dict(task_id)
    #     print(param_dicts)
    #     if task_data == 'm_analysis_data_line':
    #         df = read_lineloss_data(task_id)
    #         globals()[algorithm_dicts.get(algorithm_or_model_id)](df, task_id, param_dicts)
    #     else:
    #         df, train_data = read_credential_data(task_id)
    #         globals()[algorithm_dicts.get(algorithm_or_model_id)](df, train_data, task_id, param_dicts)
    # else:
    #     df, data ,summary_vec = read_cnn_data(task_id)
    #     globals()[model_dicts.get(algorithm_or_model_id)](df, data, summary_vec, task_id)
    #获取表名
    #从m_analysis_task里面根据task_id找表名
    engine = mysql_engine()
    task_data = pd.read_sql("select datasource_name from m_analysis_task where id = {}".format(task_id),con=engine)['datasource_name'][0]
    engine.dispose()
    ## 修改读表逻辑
    if algorithm_or_model == 'algorithm':
        param_dicts = param_dict(task_id)
        if task_data == 'm_analysis_data_credential':
            df, train_data = read_credential_data(task_id)
            globals()[algorithm_dicts.get(algorithm_or_model_id)](df, train_data, task_id, param_dicts)
        else:
            df = read_stat_data(task_id,task_data)
            globals()[algorithm_dicts.get(algorithm_or_model_id)](df, task_id, param_dicts,task_data)
    else:
        df, data ,summary_vec = read_cnn_data(task_id)
        globals()[model_dicts.get(algorithm_or_model_id)](df, data, summary_vec, task_id)
    print("OK!")

if __name__ == '__main__':
    #main_program(1007, 'algorithm', 12)
    main_program(1012, 'algorithm', 12)
    # main_program(146, 'm_analysis_private_enterprise', 'algorithm', 15)
    # main_program(714, 'm_analysis_data_overload', 'algorithm', 13)
    # main_program(715, 'm_analysis_data_overload', 'algorithm', 14)
    pass
