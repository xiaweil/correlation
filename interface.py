# -*- coding: utf-8 -*-
# @Time     : 2022/9/8 14:55
# @author   : yone
# @FileName : interface.py


from package.sql_data_operate import push_sql_data as push
from package.init_tables import tradeRefTable as trade
from package.init_tables.yearQualityTable import cal as ycal
from package.init_tables.quarterQualityTable import cal as qcal
# tradecode_ref
# trade.trade_code_ref()

# electricity_consumption
# push.pushMonthData()

# user_info
# push.pushUserInfo()

# key_enterprise
# push.pushKeyEnterprise()

# creativity_type
# push.pushCreativityData()

#industry_trend
# ycal()

#industry_trend 季度
# qcal()

# 评分
# push.pushIndustrialScore()
