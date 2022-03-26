import streamlit as st
import time,os,json
from rx import operators as ops
import pandas as pd
import altair as alt
from PIL import Image

from timeplus import *

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,15,1])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Timeplus Real-time Insights for Github")
with col_link:
    st.markdown("[About us](https://timeplus.com)", unsafe_allow_html=True)

env = (
    Env()
    .schema(st.secrets["TIMEPLUS_SCHEMA"])
    .host(st.secrets["TIMEPLUS_HOST"])
    .port(st.secrets["TIMEPLUS_PORT"])
    .login(st.secrets["AUTH0_API_CLIENT_ID"], st.secrets["AUTH0_API_CLIENT_SECRET"])
)
Env.setCurrent(env)

def show_table_for_query(sql,table_name,row_cnt):
    query = Query().sql(sql).create()
    col = [h["name"] for h in query.header()]
    def update_table(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
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
    show_table_for_query('select created_at,actor,type,repo from github_events','live_events',3)

    st.header('New events every 10m')
    sql="select window_end as time,count() as count from tumble(table(github_events),10m) group by window_end emit last 2d"
    result=Query().execSQL(sql,100)
    col = [h["name"] for h in result["header"]]
    df = pd.DataFrame(result["data"], columns=col)
    c = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='count:Q',tooltip=['time','count'],color=alt.value('#D53C97'))
    st.altair_chart(c, use_container_width=True)

    st.header('New repos')
    show_table_for_query("select created_at,actor,repo,json_extract_string(payload,'master_branch') AS branch from github_events WHERE type='CreateEvent'",'new_repo',4)
    st.header('Default branch for new repos')

    sql="""SELECT json_extract_string(payload,'master_branch') AS branch,count(*) AS cnt
FROM table(github_events) WHERE type='CreateEvent' GROUP BY branch ORDER BY cnt DESC LIMIT 3"""
    result=Query().execSQL(sql,10000)
    col = [h["name"] for h in result["header"]]
    df = pd.DataFrame(result["data"], columns=col)
    base = alt.Chart(df).encode(theta=alt.Theta('cnt:Q',stack=True),color=alt.Color('branch:N',legend=None))
    pie=base.mark_arc(outerRadius=120)
    text = base.mark_text(radius=150, size=20).encode(text='branch:N')
    st.altair_chart(pie+text, use_container_width=False)

with col2:
    st.header('Hot repos')
    sql="""SELECT window_end as time,repo, group_array(distinct actor) AS followers
FROM hop(github_events,1m,30m) 
WHERE type ='WatchEvent' GROUP BY window_end,repo HAVING length(followers)>1 
emit last 4h
"""
    show_table_for_query(sql,'star_table',8)

# show a changing single value for total events
with col3:
    st.header('Event count')
    with st.empty():
        #show the initial events first
        sql="select count(*) from table(github_events) emit periodic 1s"
        cnt=Query().execSQL(sql)["data"][0][0]
        st.metric(label="Github events", value="{:,}".format(cnt))
        st.session_state.last_cnt=cnt

        #create a streaming query to update counts
        sql=f"select {cnt}+count(*) as events from github_events"
        query = Query().sql(sql).create()
        def update_row(row):
            delta=row[0]-st.session_state.last_cnt
            if (delta>0):
                st.metric(label="Github events", value="{:,}".format(row[0]), delta=row[0]-st.session_state.last_cnt, delta_color='inverse')
                st.session_state.last_cnt=row[0]
        stopper = Stopper()
        query.get_result_stream(stopper).subscribe(
            on_next=lambda i: update_row(i),
            on_error=lambda e: print(f"error {e}"),
            on_completed=lambda: stopper.stop(),
        )
        #query.cancel().delete()