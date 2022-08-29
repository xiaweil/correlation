#!/usr/bin
# encoding: utf-8
"""=========================
@author: Bruce
@contact: wsq8932419@163.com
@software: PyCharm
@file: sd_model_lib_interface.py
@time: 2020/7/21 10:46
=============================="""

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
print(BASE_DIR)
sys.path.append(BASE_DIR)

import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.options
from tornado.options import options, define
from tornado.web import RequestHandler
from package.sql_data_operate import push_sql_data as pusd


define("port", default=8002, type=int, help="run server on the given port.")

class IndexHandler(RequestHandler):

    # 定义post方法
    def post(self):

        self.write("**开始执行分析任务**\n")
        # 获取参数
        pusd.pushIndustrialScore()
        self.write("任务完成")

class getSeasonScoreHandler(RequestHandler):
    def post(self):
        self.write("**开始执行分析任务**\n")
        pusd.pushIndustryTrend()
        self.write("任务完成")

# class EmotionAnalysis(RequestHandler):
#     """
#     综合评价权重应用
#     """
#     # 定义post方法
#     def post(self):
#         # 获取参数
#         l2_id = int(self.get_body_argument('l2_id'))
#         file_pwd = self.get_body_argument('file_pwd')
#         # 执行主程序
#         info = emotion_analysis(l2_id, file_pwd)
#         self.write(info)

if __name__ == '__main__':
    # 从配置文件导入参数
    # tornado.options.parse_config_file("./config")

    # 转换命令行参数 python opt.py --port=9000(转换端口)
    tornado.options.parse_command_line()

    # 建立应用接口
    app = tornado.web.Application([
        (r"/industrial_score", IndexHandler),
        (r"/seasonal_score", getSeasonScoreHandler)
    ])

    # 实例化httpserver
    http_server = tornado.httpserver.HTTPServer(app)

    # 定义端口
    http_server.listen(options.port)
    print("The Python interface is running......")
    # IO循环
    tornado.ioloop.IOLoop.current().start()




