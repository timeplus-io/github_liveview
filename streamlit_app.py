import streamlit as st
import time
import os
import json
from rx import operators as ops

import pandas as pd
import altair as alt

from timeplus import (
    Stream,
    Query,
    Env,
    Source,
    Stopper,
    StreamColumn,
    KafkaSource,
    KafkaSink,
    KafkaProperties,
    SourceConnection,
    GeneratorConfiguration,
    GeneratorField,
    GeneratorSource,
    SourceConnection,
    SlackSink,
    SlackSinkProperty,
    SMTPSink,
    SMTPSinkProperty,
)

from PIL import Image

st.set_page_config(layout="wide")

image = Image.open("detailed-analysis@2x.png")
st.image(image, caption="Timeplus Real-time Insights for Github")
#st.write("Fast + Powerful Real-Time Analytics Made Intuitive.")

env = (
    Env()
    .schema(st.secrets["TIMEPLUS_SCHEMA"])
    .host(st.secrets["TIMEPLUS_HOST"])
    .port(st.secrets["TIMEPLUS_PORT"])
    .login(st.secrets["AUTH0_API_CLIENT_ID"], st.secrets["AUTH0_API_CLIENT_SECRET"])
)
Env.setCurrent(env)

col1, col2, col3 = st.columns([3,3,1])

with col1:
    st.header('New events every 10m')
    sql="select window_end as time,count() as count from tumble(table(github_events),10m) group by window_end emit last 2d"
    result=Query().execSQL(sql,100)
    col = [h["name"] for h in result["header"]]
    df = pd.DataFrame(result["data"], columns=col)
    c = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='count:Q',tooltip=['time','count'],color=alt.value('#D53C97'))
    st.altair_chart(c, use_container_width=True)

with col2:
    st.header('Hot repos')
    sql="""SELECT window_end as time,repo, group_array(distinct actor) AS followers
FROM hop(github_events,1m,30m) 
WHERE type ='WatchEvent' GROUP BY window_end,repo HAVING length(followers)>1 
emit last 4h
"""
    query = Query().sql(sql).create()
    col = [h["name"] for h in query.header()]
    def update_row2(row):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
        df = pd.DataFrame([data], columns=col)
        if "star_table" not in st.session_state:
            st.session_state["star_table"] = st.table(df)
        else:
            st.session_state.star_table.add_rows(df)
    stopper = Stopper()
    query.get_result_stream(stopper).pipe(ops.take(16)).subscribe(
        on_next=lambda i: update_row2(i),
        on_error=lambda e: print(f"error {e}"),
        on_completed=lambda: stopper.stop(),
    )
    query.cancel().delete()

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
                st.metric(label="Github events", value="{:,}".format(row[0]), delta=row[0]-st.session_state.last_cnt)
                st.session_state.last_cnt=row[0]
        stopper = Stopper()
        query.get_result_stream(stopper).subscribe(
            on_next=lambda i: update_row(i),
            on_error=lambda e: print(f"error {e}"),
            on_completed=lambda: stopper.stop(),
        )
        #query.cancel().delete()

