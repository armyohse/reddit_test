# coding: utf-8
host = '127.0.0.1'
port = 9090
port = int(port)

import happybase
conn = happybase.Connection(host=host,port=port)
hred = conn.table('historical_reddit')
ured = conn.table('upstream_reddit')
#hiter = hred.scan(row_prefix=b'W')
#hdata,udata = [],[]
#for key,data in hiter:
#    hdata.append((key,data))
uiter = ured.scan()
for key,data in uiter:
    udata.append((key,data))


from json import loads

subreddit = []
word = []
counter = []
#for key,val in hdata:
#    if b'd:w' in val and b'd:c' in val:
#        subreddit.append(chr(key[1]))
#        word.append((val[b'd:w']).decode())
#        counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))
w_hdata = dict(subreddit=subreddit,word=word,counter=counter)


from json import loads

subreddit = []
word = []
counter = []
for key,val in udata:
    if key.decode()[0] == 'M':
        continue
    if b'd:w' in val and b'd:c' in val:
        subreddit.append(chr(key[1]))
        word.append((val[b'd:w']).decode())
        counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))
w_udata = dict(subreddit=subreddit,word=word,counter=counter)



from json import loads

subreddit = []
general_info = []
features = []
targets = []
predicted = []
for key,val in udata:
    if key.decode()[0] == 'W':
        continue
    if b'd:m' in val and b'd:g' in val and b'd:p' in val:
        subreddit.append(chr(key[1]))
        gi = loads(val[b'd:g'])
        general_info.append(gi)
        f_t = loads(val[b'd:m'])
        features.append(f_t[0])
        targets.append(f_t[1])
        predicted.append(loads(val[b'd:p']))
m_udata = dict(subreddit=subreddit,general_info = general_info,
               features = features,targets = targets,predicted = predicted)



from bokeh.models.widgets import DataTable,TableColumn,Tabs,Panel,Slider,Button
from bokeh.layouts import Column,Row,WidgetBox
from bokeh.models import ColumnDataSource,CustomJS


m_udata_source = ColumnDataSource(m_udata)
w_udata_source = ColumnDataSource(w_udata)
w_hdata_source = ColumnDataSource(w_hdata)


w_cols = [
    TableColumn(field='subreddit',title='subreddit'),
    TableColumn(field='word',title='word'),
    TableColumn(field='counter',title='counter'),
]
subreddit = []
general_info = []
features = []
targets = []
predicted = []

m_cols = [
    TableColumn(field='subreddit',title='subreddit'),
    TableColumn(field='general_info',title='general_info'),
    TableColumn(field='features',title='features'),
    TableColumn(field='targets',title='targets'),
    TableColumn(field='predicted',title='predicted'),
]


w_hdata_table = DataTable(source=w_hdata_source,columns=w_cols,fit_columns=True,width=1000,sizing_mode="scale_both")
m_udata_table = DataTable(source=m_udata_source,columns=m_cols,fit_columns=True,width=1000,sizing_mode="scale_both")
w_udata_table = DataTable(source=w_udata_source,columns=w_cols,fit_columns=True,width=1000,sizing_mode="scale_both")

from datetime import date,timedelta
from bokeh.core.properties import Date

from pandas import DataFrame



def w_hdata_update(day_ago = 1):
    try:
        day_ago = int(float(slider.value))
        if day_ago < 1:
            day_ago = 1
        if day_ago > (date.today()-date.fromtimestamp(0)).days:
            day_ago = 1
    except Exception as e:
        print(e)
        return
    sec_to_day = int((date.today()-date.fromtimestamp(0) - timedelta(days=day_ago)).total_seconds())

    conn = happybase.Connection(host=host,port=port)
    hred = conn.table('historical_reddit')
    hdata = []
    hiter = hred.scan(row_prefix=f'WR{sec_to_day:0>15}'.encode())
    for key,data in hiter:
        hdata.append((key,data))
    hiter = hred.scan(row_prefix=f'WP{sec_to_day:0>15}'.encode())
    for key,data in hiter:
        hdata.append((key,data))
    subreddit = []
    word = []
    counter = []
    for key,val in hdata:
        if b'd:w' in val and b'd:c' in val:
            subreddit.append(chr(key[1]))
            word.append((val[b'd:w']).decode())
            counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))
    w_hdata = dict(subreddit=subreddit,word=word,counter=counter)
    w_hdata_source.data = w_hdata
    return 
   
slider = Slider(start=1,end=1000,step=1)


button = Button(label='show')
button.on_click(w_hdata_update)

tab_hdata = Panel(child=Column(slider,button,w_hdata_table),title='historical reddit')
tab_w_udata = Panel(child=Row(w_udata_table),title='word upstream reddit')
tab_m_udata = Panel(child=Row(m_udata_table),title='ml upstream reddit')
tabs = Tabs(tabs=[tab_hdata,tab_w_udata,tab_m_udata])


from bokeh.plotting import curdoc

doc = curdoc()
doc.add_root(tabs)

from functools import partial
from threading import Thread
from bokeh.plotting import curdoc

from tornado import gen


@gen.coroutine
def update(w_udata,m_udata):
    w_udata_source.data = w_udata
    m_udata_source.data = m_udata
    return

def udata_update():
    conn = happybase.Connection(host=host,port=port)
    ured = conn.table('upstream_reddit')
    udata = []
    uiter = ured.scan()
    for key,data in uiter:
        udata.append((key,data))

    subreddit = []
    word = []
    counter = []
    for key,val in udata:
        if key.decode()[0] == 'M':
            continue
        if b'd:w' in val and b'd:c' in val:
            subreddit.append(chr(key[1]))
            word.append((val[b'd:w']).decode())
            counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))
    w_udata = dict(subreddit=subreddit,word=word,counter=counter)

    subreddit = []
    general_info = []
    features = []
    targets = []
    predicted = []
    for key,val in udata:
        if key.decode()[0] == 'W':
            continue
        try:
            if b'd:g' in val and b'd:m' in val and b'd:p' in val:
                subreddit.append(chr(key[1]))
                gi = loads(val[b'd:g'])
                general_info.append(gi)
                f_t = loads(val[b'd:m'])
                features.append(f_t[0])
                targets.append(f_t[1])
                predicted.append(loads(val[b'd:p']))
        except:
            continue
    m_udata = dict(subreddit=subreddit,general_info = general_info,
                   features = features,targets = targets,predicted = predicted)
    return w_udata,m_udata

import time
def blocking_task():
    while True:
        time.sleep(10)
        w_udata,m_udata = udata_update()
        doc.add_next_tick_callback(partial(update, w_udata,m_udata))


thread = Thread(target=blocking_task)
thread.start()

