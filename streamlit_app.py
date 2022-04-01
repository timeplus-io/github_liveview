import streamlit as st
import time,datetime,pytz,os,json
from rx import operators as ops
import pandas as pd
import altair as alt
from PIL import Image

from timeplus import *

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,10,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Timeplus Real-time Insights for Github")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/github_liveview/blob/develop/streamlit_app.py) | [About Timeplus](https://timeplus.com)", unsafe_allow_html=True)

env = (
    Env()
    .schema(st.secrets["TIMEPLUS_SCHEMA"])
    .host(st.secrets["TIMEPLUS_HOST"])
    .port(st.secrets["TIMEPLUS_PORT"])
    .login(st.secrets["AUTH0_API_CLIENT_ID"], st.secrets["AUTH0_API_CLIENT_SECRET"])
)
Env.setCurrent(env)

def show_table_for_query(sql,table_name,row_cnt):
    st.code(sql, language="sql")
    query = Query().sql(sql).create()
    col = [h["name"] for h in query.header()]
    def update_table(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
            #hack show first column as more friendly datetime diff
            if(i==0):
                minutes=divmod((pytz.utc.localize(datetime.datetime.utcnow())-row[i]).total_seconds(),60)
                data[f]=f"{int(minutes[0])} min {int(minutes[1])} sec ago"

        df = pd.DataFrame([data], columns=col)
        if name not in st.session_state:
            st.session_state[name] = st.table(df)
        else:
            st.session_state[name].add_rows(df)
    stopper = Stopper()
    query.get_result_stream(stopper).pipe(ops.take(row_cnt)).subscribe(
        on_next=lambda i: update_table(i,table_name),
        on_error=lambda e: print(f"error {e}"),
        on_completed=lambda: stopper.stop(),
    )
    query.cancel().delete()

col1, col2, col3 = st.columns([3,3,1])

with col1:
    st.header('Recent events')
    show_table_for_query('SELECT created_at,actor,type,repo FROM github_events','live_events',3)

    st.header('New events every 10m')
    sql="""SELECT window_end AS time,count() AS count from tumble(table(github_events),10m) 
    WHERE _tp_time > date_sub(now(), 2h) GROUP BY window_end"""
    st.code(sql, language="sql")
    result=Query().execSQL(sql,1000)
    col = [h["name"] for h in result["header"]]
    df = pd.DataFrame(result["data"], columns=col)
    c = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='count:Q',tooltip=['time','count'],color=alt.value('#D53C97'))
    st.altair_chart(c, use_container_width=True)

with col2:
    st.header('New repos')
    show_table_for_query("""SELECT created_at,actor,repo,json_extract_string(payload,'master_branch') AS branch 
    FROM github_events WHERE type='CreateEvent'""",'new_repo',3)
    
    st.header('Default branch for new repos')
    sql="""SELECT json_extract_string(payload,'master_branch') AS branch,count(*) AS cnt
FROM table(github_events) WHERE _tp_time>date_sub(now(),1h) AND type='CreateEvent' 
GROUP BY branch ORDER BY cnt DESC LIMIT 3"""
    st.code(sql, language="sql")
    result=Query().execSQL(sql,100000)
    col = [h["name"] for h in result["header"]]
    df = pd.DataFrame(result["data"], columns=col)
    base = alt.Chart(df).encode(theta=alt.Theta('cnt:Q',stack=True),color=alt.Color('branch:N',legend=None),tooltip=['branch','cnt'])
    pie=base.mark_arc(outerRadius=80)
    text = base.mark_text(radius=110, size=16).encode(text='branch:N')
    st.altair_chart(pie+text, use_container_width=True)

    st.header('Hot repos')
    sql="""SELECT window_end AS time,repo, group_array(distinct actor) AS followers
FROM hop(github_events,5m,30m) 
WHERE type ='WatchEvent' GROUP BY window_end,repo HAVING length(followers)>1 
SETTINGS seek_to='-30m'
"""
    show_table_for_query(sql,'star_table',8)

# show a changing single value for total events
with col3:
    st.header('Event count')
    st.code("select count(*) from github_events emit periodic 1s", language="sql")
    with st.empty():
        #show the initial events first
        sql="select count(*) from table(github_events)"
        cnt=Query().execSQL(sql,10000)["data"][0][0]
        st.metric(label="Github events", value="{:,}".format(cnt))
        st.session_state.last_cnt=cnt

        #create a streaming query to update counts
        sql=f"select {cnt}+count(*) as events from github_events emit periodic 1s"
        query = Query().sql(sql).create()
        def update_row(row):
            delta=row[0]-st.session_state.last_cnt
            if (delta>0):
                st.metric(label="Github events", value="{:,}".format(row[0]), delta=row[0]-st.session_state.last_cnt, delta_color='inverse')
                st.session_state.last_cnt=row[0]
        stopper = Stopper()
        query.get_result_stream(stopper).pipe(ops.take(200)).subscribe(
            on_next=lambda i: update_row(i),
            on_error=lambda e: print(f"error {e}"),
            on_completed=lambda: stopper.stop(),
        )
        query.cancel().delete()