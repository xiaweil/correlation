# -*- coding: utf-8 -*-
# @Time     : 2022/8/31 11:25
# @author   : yone
# @FileName : test.py
import re
# list = ['闵行', '松江']
# string = "松江区基地阿基"
# print(re.match(f".*{list[0]}.*",string))
from package.sql_data_operate import push_sql_data as pusd
pusd.pushIndustryTrend()