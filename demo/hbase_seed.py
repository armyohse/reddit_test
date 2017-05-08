
import happybase
conn = happybase.Connection()
table = conn.table('historical_reddit')
from datetime import datetime
sec_to_day = int((datetime(2014,1,1) - datetime.fromtimestamp(0)).total_seconds())
features = [1,1,1,1,sec_to_day,1,1,False]
targets = [1,1,1]
post_title = 'o'
link_to_post = 'http://t'

from json import dumps
records = []
k = f'MR{sec_to_day:0>15}tt'
records.append([k,'d:m',[features,targets]])
records.append([k,'d:g',[post_title,link_to_post]])
k = f'WR{sec_to_day:0>15}hi'
records.append([k,'d:c',10])
records.append([k,'d:w','hi'])    
k = f'WR{sec_to_day:0>15}godaddy'
records.append([k,'d:c',190])
records.append([k,'d:w','godaddy'])    
conn = happybase.Connection()
table = conn.table('historical_reddit')
for k,cfq,v in records:
    table.put(k,{cfq:dumps(v)})
    
sec_to_day = int((datetime(2017,1,1) - datetime.fromtimestamp(0)).total_seconds())
features = [1,1,1,1,sec_to_day,1,1,True]
targets = [1,1,1]
post_title = 'ht'
link_to_post = 'http://vv'
predicted = [2,2,2]
records = []
k = f'MR{sec_to_day:0>15}ht'
records.append([k,'d:m',[features,targets]])
records.append([k,'d:g',[post_title,link_to_post]])
records.append([k,'d:p',predicted])
k = f'MR{sec_to_day:0>15}nth'
records.append([k,'d:m',[features,targets]])
records.append([k,'d:g',['oh no','http://gonth'])
records.append([k,'d:p',predicted])
k = f'MR{sec_to_day:0>15}vhth'
records.append([k,'d:m',[features,targets]])
records.append([k,'d:g',['oh mt','http://naeosnth']])
records.append([k,'d:p',predicted])
k = f'WR{sec_to_day:0>15}good'
records.append([k,'d:c',15])
records.append([k,'d:w','good'])    
conn = happybase.Connection()
table = conn.table('upstream_reddit')
for k,cfq,v in records:
    table.put(k,{cfq:dumps(v)})
    
