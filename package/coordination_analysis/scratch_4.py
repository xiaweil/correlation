import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("mysql+pymysql://dateam2:Shudao2019!!@124.71.156.211:3307/shinan_cyjj")
DBSession = sessionmaker(bind=engine)
session = DBSession()
sql = "select std_industry_id,stat_time,scale_score_l1,region_name from industry_trend where region_name in ('闵行','徐汇') AND length(stat_time) = 6"
result = session.execute(sql)
region,id,time,score = [],[],[],[]
for r,i,j,k in result:
    region.append(r)
    id.append(i)
    time.append(j)
    score.append(k)
industry = pd.DataFrame([region,id,time,score])
industry = industry.T
industry = industry.rename(columns={0:'code',1:'stat',2:'score',3:'region'})
industry = pd.DataFrame(industry,columns=['region','code','stat','score'])
sql1 = "select Id,std_industry_name from industry_info"
result1 = session.execute(sql1)
id1,cl1 = [],[]
for i,j in result1:
    id1.append(i)
    cl1.append(j)
industryInfo = pd.DataFrame([id1,cl1])
industryInfo = industryInfo.T
industryInfo = industryInfo.rename(columns={0:'id',1:'industryClass'})
grad = pd.merge(industry,industryInfo,left_on='code',right_on='id')
grad = pd.DataFrame(grad,columns=['region','industryClass','stat','score'])
grad = grad.pivot_table(index=['region','industryClass'],columns='stat',values='score',aggfunc='first')
grad = pd.DataFrame(grad)
res = grad.reset_index()

class Coordination:
    def __init__(self,region_name):
        self.region_name = region_name

    def readFile(self,res):
        Data = res[res["region"] == self.region_name]

        del Data['region']
        column = Data['industryClass']
        del Data['industryClass']
        grade = pd.DataFrame(Data.T)
        grade.rename(columns=column, inplace=True)

        return grade

    def lagCoor(self,grade,stage):
        industry = ['医药制造业', '计算机、通信和其他电子设备制造业', '铁路、船舶、航空航天和其他运输设备制造业', '黑色金属冶炼和压延加工业', '化学原料和化学制品制造业', '化学纤维制造业',
                    '汽车制造业','电力、热力生产和供应业']
        region_name,coordination_name,coordination_code,industry_id_base,industry_id_comp,cycle,correlation,rank = [],[],[],[],[],[],[],[]
        for i in range(len(industry)):
            if industry[i] not in grade.columns:
                pass
            else:
                y = grade.iloc[:7-stage,list(grade.columns).index(industry[i])]
                for j in range(len(grade.columns)):
                    if industry[i] == grade.columns[j]:
                        pass
                    elif grade.columns[j] in industry[:i]:
                        pass
                    else:
                        region_name.append(self.region_name)
                        coordination_name.append(industry[i]+'&'+grade.columns[j])
                        industry_id_base.append(industry[i])
                        industry_id_comp.append(grade.columns[j])
                        cycle.append(stage*(-3))
                        if np.isnan(y.corr(grade.iloc[stage:,j])):
                            correlation.append(0)
                            rank.append('None')
                        else:
                            correlation.append(y.corr(grade.iloc[stage:,j]))
                            if abs(y.corr(grade.iloc[stage:,j])) < 0.7:
                                rank.append("低")
                            elif abs(y.corr(grade.iloc[stage:,j])) >= 0.7 and abs(y.corr(grade.iloc[stage:,j])) < 0.95:
                                rank.append("中")
                            elif abs(y.corr(grade.iloc[stage:,j])) >= 0.95:
                                rank.append("高")
        result = pd.DataFrame([region_name,coordination_name,coordination_code,cycle,correlation,rank,industry_id_base,industry_id_comp])

        return result

def unique(region_name,re):
    res = re
    region = Coordination(region_name)
    grade = region.readFile(res)
    result1,result2,result3 = region.lagCoor(grade,0),region.lagCoor(grade,1),region.lagCoor(grade,2)
    result = pd.concat([result1.T,result2.T,result3.T])
    result.rename(columns={0:'region_name',1:'coordination_name',2:'coordination_code',3:'cycle',4:'correlation',5:'rank',6:'base',7:'comp'},inplace=True)
    result.reset_index(drop=True,inplace=True)
    re = result[['coordination_name','correlation']].groupby(by='coordination_name',as_index=False).max()
    result['correlation'] = pd.to_numeric(result['correlation'])
    res = pd.merge(result,re,on=['coordination_name','correlation'],how='right')
    return res

sql = "select Id,std_industry_name from industry_info"
result = session.execute(sql)
id,cl = [],[]
for i,j in result:
    id.append(i)
    cl.append(j)
industry = pd.DataFrame([id,cl])
industry = industry.T
industry.rename(columns={0:'id',1:'industryClass'},inplace=True)

result = pd.concat([unique("闵行",res),unique("徐汇",res)])
result.reset_index(drop=True,inplace=True)
result = pd.merge(result,industry,left_on='base',right_on='industryClass',how='left')
result = result.rename(columns={'id':'industry_id_base'})
result = pd.merge(result,industry,left_on='comp',right_on='industryClass',how='left')
result = result.rename(columns={'id':'industry_id_comp'})
result = result.drop(columns=['base','comp','industryClass_x','industryClass_y'])
for i in range(len(result.iloc[:,0])):
    result.iloc[i,2] = str(result.iloc[i,6])+'&'+str(result.iloc[i,7])
result['id'] = range(1,len(result.iloc[:,0])+1)
result = pd.DataFrame(result,columns=['id','region_name', 'coordination_name', 'coordination_code','industry_id_base', 'industry_id_comp','cycle',
       'correlation', 'rank'])
print(result)
result.to_csv('C:/Users/Administrator/Documents/实习/数道/国家电网上海市南供电公司/协同分析/mme.csv')
#result.to_sql("industry_coordination", con=engine, if_exists="replace", index=False)

