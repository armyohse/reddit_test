# coding: utf-8
host = 'ec2-54-174-217-97.compute-1.amazonaws.com'
port = 9090
port = int(port)

import happybase


from json import loads

subreddit = []
word = []
counter = []
w_hdata = dict(subreddit=subreddit,word=word,counter=counter)

subreddit = []
word = []
counter = []
w_udata = dict(subreddit=subreddit,word=word,counter=counter)

from json import loads

m_udata = {}
m_u_cols = ['subreddit','post_title','link_to_post','title_len','title_num_tokens','body_len','body_num_tokens','post_month','post_weekday','post_over18','num_comments','ups','downs']
for i in m_u_cols:
    m_udata[i] = []
 
from bokeh.models.widgets import DataTable,TableColumn,Tabs,Panel,Slider,Button,RangeSlider
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

m_cols = [
    TableColumn(field='subreddit',title='subreddit'),
    TableColumn(field='post_title',title='title'),
    TableColumn(field='link_to_post',title='link'),
    TableColumn(field='title_len',title='title length'),
    TableColumn(field='title_num_tokens',title='number of title tokens'),
    TableColumn(field='body_len',title='body length'),
    TableColumn(field='body_num_tokens',title='number of body tokens'),
    TableColumn(field='post_month',title='month'),
    TableColumn(field='post_weekday',title='weekday'),
    TableColumn(field='post_over18',title='post over 18'),
    TableColumn(field='num_comments',title='number of comments'),
    TableColumn(field='ups',title='up votes'),
    TableColumn(field='downs',title='down votes'),
]

w_hdata_table = DataTable(source=w_hdata_source,columns=w_cols,fit_columns=True,width=1200,row_headers=False)
m_udata_table = DataTable(source=m_udata_source,columns=m_cols,fit_columns=True,width=1200,row_headers=False)
w_udata_table = DataTable(source=w_udata_source,columns=w_cols,fit_columns=True,width=1200,row_headers=False)

from datetime import date,timedelta
from bokeh.core.properties import Date
from datetime import datetime,timezone

def w_hdata_update():
    try:
        day_s,day_e = start_slider.value,end_slider.value
        day_s = int(day_s)
        day_e = int(day_e)
        if day_s < 1:
            day_s = 1
        if day_e > 3593:
            day_e = 3593
        if day_e <= day_s:
            day_s = 1
            day_e = 2
    except Exception as e:
        print(e)
        return
    print(day_s,day_e)
    day_s_sec = int((datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=day_s)).timestamp())
    day_e_sec = int((datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=day_e)).timestamp())
    delta = int(timedelta(days=1).total_seconds())
    sec_to_day = day_e_sec + delta
    hdata = []
    #f'WP{sec_to_day:0>15}'
    while sec_to_day < day_s_sec:
        try:
            conn = happybase.Connection(host=host,port=port)
            hred = conn.table('historical_reddit')
            key = f'WR{sec_to_day:0>15}'.encode()
            hiter = hred.scan(row_prefix=key)
            for key,data in hiter:
                hdata.append((key,data))
            key = f'WP{sec_to_day:0>15}'.encode()
            hiter = hred.scan(row_prefix=key)
            for key,data in hiter:
                hdata.append((key,data))
        except Exception as e:
            print(e)
            pass
        sec_to_day += delta
    subreddit = []
    word = []
    counter = []
    from nltk.corpus import stopwords
    eswords = stopwords.words('english')
    for key,val in hdata:
        if b'd:w' in val and b'd:c' in val:
            w = (val[b'd:w']).decode()[1:-1]
            if not w in eswords:
                subreddit.append(chr(key[1]))
                word.append(w)
                counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))
    w_hdata = pd.DataFrame(dict(subreddit=subreddit,word=word,counter=counter)).sort_values('counter',ascending=False).to_dict('list')
    w_hdata_source.data = w_hdata
    return 
   
start_slider = Slider(start=1,end=3593,step=1)
end_slider = Slider(start=1,end=3593,step=1)

button = Button(label='show')
button.on_click(w_hdata_update)


import pandas as pd
from bokeh.charts import Chord,Bar
from bokeh.io import show, output_file

def getChordData():
    conn = happybase.Connection(host=host,port=port)
    udata = []
    try:
        from datetime import datetime,timezone 
        sec_to_day = int(datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
        conn = happybase.Connection(host=host,port=port)
        ured = conn.table('upstream_reddit')
        key = f'WR{sec_to_day:0>15}'.encode()
        uiter = ured.scan(row_prefix=key)
        for key,data in uiter:
            udata.append((key,data))
        key = f'WP{sec_to_day:0>15}'.encode()
        uiter = ured.scan(row_prefix=key)
        for key,data in uiter:
            udata.append((key,data))
    except Exception as e:
        print(e)
    subreddit = []
    word = []
    counter = []
    for key,val in udata:
        if b'd:w' in val and b'd:c' in val:
            subreddit.append(chr(key[1]))
            word.append((val[b'd:w']).decode()[1:-1])
            counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))

    import pandas as pd
    
    df_raw = pd.DataFrame({'topic':subreddit,'freq':counter,'word':word})
    from nltk.corpus import stopwords
    eswords = stopwords.words('english')
    try:
        df = df_raw [df_raw.word.map(lambda x: not x in eswords)]
    except:
        pass
    data = {'topic':['Python+RLanguage'],'freq':[10],'word':['happy']}
    try:
        top_size = 10
        top_r = df[df.topic == 'R'].sort_values('freq',ascending=False)[:top_size]
        top_p = df[df.topic == 'P'].sort_values('freq',ascending=False)[:top_size]
        chord_data = top_r.append(top_p)
        from random import randint
        def random_swap(row):
            if row.topic == 'P':
                row.topic = 'Python'
            else:
                row.topic = 'RLanguge'
            if randint(0,1):
                row.word,row.topic = row.topic,row.word
            return row
        chord_data = chord_data.apply(random_swap,axis=1)
        for c in chord_data.columns:
            data[c] = list(chord_data[c].values)
    except Exception as e:
        print(e)
        pass
    print(data)
    r_names = ['rlanguage','r','rlang']
    p_names = ['python','cython','jython']
    data_mutual = {'s':[],'t':[],'v':[]}
    try:
        p_ment = df_raw[(df_raw.topic == 'R') & df_raw.word.map(lambda x: x.lower() in p_names)]
        p_count = 10*p_ment.freq.sum() + 3
        r_ment = df_raw[(df_raw.topic == 'P') & df_raw.word.map(lambda x: x.lower() in r_names)]
        r_count = 10*r_ment.freq.sum() + 3
        print(p_ment)
        print(r_ment)
        data_mutual = {'s':['Python','RLanguage'],'t':['RLanguage','Python'], 'v':[r_count,p_count]}
    except Exception as e:
        print(e)
    print('data_mutual:',data_mutual)
    return data,data_mutual

tab_hdata = Panel(child=Column(start_slider,end_slider,button,w_hdata_table),title='historical reddit')
tab_w_udata = Panel(child=Row(w_udata_table),title='word upstream reddit')
tab_m_udata = Panel(child=Row(m_udata_table),title='ml upstream reddit')
panels = [tab_hdata,tab_w_udata,tab_m_udata]

chord_data,chord_mutual_data = getChordData()
try:
    chord_words =  Chord(chord_data, source="topic",target="word",value="freq")
    tab_chord = Panel(child=chord_words,title='Chord topic<>words')
    panels.append(tab_chord)
except Exception as e:
    print(e)
    pass

try:
    chord_mutual=  Chord(chord_mutual_data, source="s",target="t",value="v")
    tab_chord_mutual = Panel(child=chord_mutual,title='Mutual mentions')
    panels.append(tab_chord_mutual)
except Exception as e:
    print(e)
    pass

from random import randint
try:
    p_happiness = pd.DataFrame({'word':['awesome', 'cool', 'fun', 'happy', 'helpful', 'interesting'],'freq':[randint(0,100) for i in range(6)],'topic':['Python']*6})
    r_happiness = pd.DataFrame({'word':['awesome', 'cool', 'fun', 'happy', 'helpful', 'interesting'],'freq':[randint(0,100) for i in range(6)],'topic':['RLanguage']*6})
    common_happiness = pd.concat([p_happiness,r_happiness],axis=0)
    total_words_counter = common_happiness.freq.sum()
    common_happiness.freq = common_happiness.freq.map(lambda x: x/total_words_counter)
    happiness_bar = Bar(common_happiness, label='topic', values='freq', agg='sum', stack='word',
            title="Happy words usage by SUBREDDIT stacked by WORD", legend='top_right')
    happiness_panel = Panel(child=happiness_bar,title='Happy words')
    panels.append(happiness_panel)
except Exception as e:
    print(e)
    pass
tabs = Tabs(tabs=panels)

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
    udata = {'w':[],'m':[]}
    try:
        from datetime import datetime,timezone 
        sec_to_day = int(datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
        conn = happybase.Connection(host=host,port=port)
        ured = conn.table('upstream_reddit')
        key = f'MR{sec_to_day:0>15}'.encode()
        uiter = ured.scan(row_prefix=key)
        for key,data in uiter:
            udata['m'].append((key,data))
        key = f'MP{sec_to_day:0>15}'.encode()
        uiter = ured.scan(row_prefix=key)
        for key,data in uiter:
            udata['m'].append((key,data))
        key = f'WR{sec_to_day:0>15}'.encode()
        uiter = ured.scan(row_prefix=key)
        for key,data in uiter:
            udata['w'].append((key,data))
        key = f'WP{sec_to_day:0>15}'.encode()
        uiter = ured.scan(row_prefix=key)
        for key,data in uiter:
            udata['w'].append((key,data))
    except Exception as e:
        print(e)
    subreddit = []
    word = []
    counter = []
    from nltk.corpus import stopwords
    eswords = stopwords.words('english')
    for key,val in udata['w']:
        if b'd:w' in val and b'd:c' in val:
            w = (val[b'd:w']).decode()[1:-1]
            if not w in eswords:
                subreddit.append(chr(key[1]))
                word.append(w)
                counter.append(int.from_bytes(val[b'd:c'],byteorder='big'))
    w_udata = pd.DataFrame(dict(subreddit=subreddit,word=word,counter=counter)).sort_values('counter',ascending=False).to_dict('list')

    
    
    m_u_cols = ['subreddit','post_title','link_to_post','title_len','title_num_tokens','body_len','body_num_tokens','post_month','post_weekday','post_over18','num_comments','ups','downs']
    m_udata = dict()
    for c in m_u_cols:
        m_udata[c] = []
    for key,val in udata['m']:
        try:
            if b'd:g' in val and b'd:m' in val and b'd:p' in val:
                cols = []
                cols.append(chr(key[1]))
                gi = loads(val[b'd:g'])
                cols.extend(gi)
                f_t = loads(val[b'd:m'])
                p_t = loads(val[b'd:p']) 
                for f in f_t[0]:
                    cols.append(f)
                for t,p in zip(f_t[1],p_t):
                    cols.append(f'p:{int(p)},r:{t}')
                for c,v in zip(m_u_cols,cols):
                    m_udata[c].append(v)
        except Exception as e:
            print(e)
            continue
    return w_udata,m_udata

w_udata,m_udata = udata_update()
w_udata_source.data = w_udata
m_udata_source.data = m_udata

import time
def blocking_task():
    while True:
        time.sleep(10)
        w_udata,m_udata= udata_update()
        doc.add_next_tick_callback(partial(update, w_udata,m_udata))

thread = Thread(target=blocking_task)
thread.start()


