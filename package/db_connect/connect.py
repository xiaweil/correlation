#!/usr/bin
# encoding: utf-8
"""=========================
@author: Bruce
@contact: wsq8932419@163.com
@software: PyCharm
@file: connect.py
@time: 2020/7/21 10:43
=============================="""

from package.config import DBConfig
import pymysql
from sqlalchemy import create_engine
import mysql.connector

def mysql_engine():
    """
    创建sql_engine
    :return: engine
    """
    engine = create_engine('mysql+mysqlconnector://'
                           + DBConfig.user_mysql + ':'
                           + DBConfig.password_mysql + '@'
                           + DBConfig.host_mysql + ':'
                           + str(DBConfig.port_mysql) + '/'
                           + DBConfig.database_mysql)
    return engine


def mysql_con():
    """
    创建 conn, cursor
    :return: conn, cursor
    """
    try:
        conn = pymysql.connect(
            host=DBConfig.host_mysql,
            port=DBConfig.port_mysql,
            user=DBConfig.user_mysql,
            password=DBConfig.password_mysql,
            database=DBConfig.database_mysql,
            charset='utf8')
    except Exception as e:
        return e
    cursor = conn.cursor()
    return conn, cursor

# def connect_postgre():
#     try:
#         conn = psycopg2.connect(
#             host=DBConfig.host_postgre,
#             port=DBConfig.port_postgre,
#             user=DBConfig.user_postgre,
#             password=DBConfig.password_postgre,
#             database=DBConfig.database_postgre)
#     except Exception as e:
#         return e
#     cursor = conn.cursor()
#     engine = create_engine('postgresql://'
#                            + DBConfig.user_postgre + ':'
#                            + DBConfig.password_postgre + '@'
#                            + DBConfig.host_postgre + ':'
#                            + str(DBConfig.port_postgre) + '/'
#                            + DBConfig.database_postgre)
#     return conn, cursor, engine
