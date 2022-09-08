import pandas as pd
from math import *
import matplotlib.pyplot as plt
from package.data_preprocess.get_input_data import generateInputData
plt.rcParams['font.sans-serif'] = ['SimHei']

data = generateInputData()
print(data.columns)
data = data.drop(columns=['userId','sum','max','isCoreCompany','eleType','period', 'mean', 'keyIndustryClass'])
data = pd.melt(data,id_vars=['industryClass','division'],value_name='consumption')
data = data.groupby(by=['division',"industryClass"]).sum().reset_index()
data['pro'] = data['consumption']/data['consumption'].sum()*100
structure = data.groupby('division').count()
sorted = data.sort_values(by='consumption',ascending=True).reset_index()
con = pd.DataFrame()
for i in range(20):
    con[i] =pd.merge(data,sorted.iloc[0:ceil(((i+1)/20)*sorted.shape[0]),:],on=['division','industryClass'],how='left').groupby(by='division').count().iloc[:,-1]

def score_change(x):
    ran = x.max() - x.min()
    change = 40 + ((x - x.min()) / ran) * 60
    return change

#企业得分部分
table = pd.read_excel("C:/Users/Administrator/Documents/实习/数道/国家电网上海市南供电公司/动因分析/score1.xlsx",sheet_name=2)
table = pd.DataFrame(table)
table["加权得分"] = score_change(table["加权得分"])
y = table["加权得分"]
cor = []
con[con.iloc[:,0] ==0] = 1
con = con.apply(lambda x:x.astype(float))
con.reset_index(drop=True,inplace=True)
for i in range(con.shape[1]):
    cor.append(y.corr(con.iloc[:,i]))


plt.plot(cor)
plt.xticks(range(20),range(0,100,5))
plt.title("产业数量得分相关系数")
plt.xlabel("用电量占比产业数量百分比")
plt.show()