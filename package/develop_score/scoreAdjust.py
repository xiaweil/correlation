#coding:utf8
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def adjustScore(data):
    model = MinMaxScaler([40, 100])
    res = model.fit_transform(data)
    res = pd.DataFrame(res)
    return res
