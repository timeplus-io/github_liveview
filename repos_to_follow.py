import streamlit as st
import os
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

with st.container():
    #a live bar chart
    sql="""SELECT repo,count(*) AS events FROM github_events GROUP BY repo HAVING events>1 EMIT last 1d"""
    st.code(sql, language="sql")
    query = Query().sql(sql).create()
    col = [h["name"] for h in query.header()]
    def update_row(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]

        df = pd.DataFrame([data], columns=col)
        if name not in st.session_state:
            bars=alt.Chart(df).mark_bar().encode(x='events:Q',y=alt.Y('repo:N',sort='-x'),tooltip=['events','repo'])
            #text = bars.mark_text(align='left',baseline='middle',dx=3 ).encode(text='events:Q')
            st.session_state[name] = st.altair_chart(bars, use_container_width=True)
        else:
            st.session_state[name].add_rows(df)
    stopper = Stopper()
    query.get_result_stream(stopper).pipe(ops.take(20000)).subscribe(
        on_next=lambda i: update_row(i,"chart"),
        on_error=lambda e: print(f"error {e}"),
        on_completed=lambda: stopper.stop(),
    )
    query.cancel().delete()