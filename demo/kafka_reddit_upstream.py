import praw
import pickle
import confluent_kafka
from base64 import b64encode


rit = praw.Reddit(client_id='TH_IDLRBRatLag',
    client_secret='KM6Y_-XHc434SWXb4A_bFzVtNUE',
    user_agent='reader')

conf = {'bootstrap.servers': '127.0.0.1:9092',
'queue.buffering.max.messages': 1000000,
        'queue.buffering.max.ms': 5000,
        'batch.num.messages': 100}
producer = confluent_kafka.Producer(**conf)


def transform(post):
    title_len = len(post.title)
    title_num_tokens = len(post.title.split())
    body_len = len(post.selftext)
    body_num_tokens = len(post.selftext.split())
    from datetime import datetime
    post_time = post.created_utc
    post_weekday = datetime.utcfromtimestamp(post_time).weekday()
    post_month = datetime.utcfromtimestamp(post_time).month
    post_over18 = post.over_18
    target1 = post.num_comments
    target2 = post.ups
    target3 = post.downs
    return ([title_len,title_num_tokens,body_len,body_num_tokens,post_month,post_weekday,post_over18],[target1,target2,target3])

def tokenize(post):
    return post.selftext.split()

def w_data(post):
    subreddit = post.subreddit.display_name[0].upper()
    from datetime import datetime
    sec_to_day = int((datetime.utcfromtimestamp(post.created_utc).date()-datetime.utcfromtimestamp(0).date()).total_seconds())
    postid = post.id
    post_title = post.title
    post_body = post.selftext
    return [subreddit,sec_to_day,postid,post_title,post_body]

def m_data(post):
    from datetime import datetime
    subreddit = post.subreddit.display_name[0].upper()
    sec_to_day = int((datetime.utcfromtimestamp(post.created_utc).date()-datetime.utcfromtimestamp(0).date()).total_seconds())
    postid = post.id
    post_title = post.title
    link_to_post = post.shortlink
    features,targets = transform(post)
    return [subreddit,sec_to_day,postid,post_title,link_to_post,features,targets]

from json import dumps

plang_rlang = rit.subreddit('Rlanguage+Python')
from datetime import datetime




from datetime import datetime
from datetime import timezone
from datetime import timedelta
from datetime import date
utc = timezone.utc
data_seed = None
end_time = datetime.now(utc)
start_time = datetime.now(utc).replace(hour=0,minute=0,second=0,microsecond=0)
while data_seed is None:
    try:
        query = f'timestamp:{int(start_time.timestamp())}..{int(end_time.timestamp())}'
        print(query)
        print(start_time,end_time,sep=' ')
        sea = plang_rlang.search(query=query,sort='new',limit=1000)            
        data_seed = [*sea]
        if data_seed:
            print(datetime.fromtimestamp(data_seed[0].created_utc,utc))
            print(datetime.fromtimestamp(data_seed[-1].created_utc,utc))
    except Exception as e:
        print(e)
from itertools import chain

for submission in chain(data_seed,plang_rlang.stream.submissions()):
    if datetime.fromtimestamp(submission.created_utc,utc).date() < datetime.now(utc).date():
        continue
    producer.produce('word_counting_upstream_data',value=dumps(w_data(submission)))     
    producer.produce('ml_upstream_data',value=dumps(m_data(submission)))
    producer.flush()
    print(m_data(submission))

