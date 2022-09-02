# -*- coding: utf-8 -*-
# @Time     : 2022/9/2 9:26
# @author   : yone
# @FileName : base.py

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, INT, VARCHAR, INTEGER
from sqlalchemy.dialects.mysql import DOUBLE
Base = declarative_base()

# 定义user_info表结构
class User(Base):
    __tablename__ = "user_info"

    Id = Column(INT, primary_key=True)
    user_code = Column(VARCHAR(255))
    user_name = Column(VARCHAR(255))
    sector = Column(VARCHAR(255))
    address = Column(VARCHAR(255))
    branch = Column(VARCHAR(255))
    center = Column(VARCHAR(255))
    voltage_level = Column(VARCHAR(255))
    user_type = Column(VARCHAR(255))
    district = Column(VARCHAR(255))
    lon = Column(DOUBLE)
    lat = Column(DOUBLE)
    std_industry_name = Column(VARCHAR(255))
    std_industry_id = Column(INT)
    company_nature = Column(VARCHAR(255))
    is_core = Column(INT)
    key_industry_id = Column(INT)
    build_date = Column(VARCHAR(255))

