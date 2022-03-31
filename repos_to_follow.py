import streamlit as st
import os
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
    st.markdown("[Source Code](https://github.com/timeplus-io/github_liveview/blob/develop/repos_to_follow.py) | [Full Dashboard](https://share.streamlit.io/timeplus-io/github_liveview/develop/streamlit_app.py) | [Live events](https://share.streamlit.io/timeplus-io/github_liveview/develop/liveview.py) | [About Timeplus](https://timeplus.com)", unsafe_allow_html=True)

env = (
    Env()
    .schema(st.secrets["TIMEPLUS_SCHEMA"])
    .host(st.secrets["TIMEPLUS_HOST"])
    .port(st.secrets["TIMEPLUS_PORT"])
    .login(st.secrets["AUTH0_API_CLIENT_ID"], st.secrets["AUTH0_API_CLIENT_SECRET"])
)
Env.setCurrent(env)

sql="""SELECT max_k(repo,10,cnt) AS max_cnt FROM 
(SELECT repo,count(*) AS cnt FROM github_events GROUP BY repo SETTINGS seek_to='-4h' )
"""
#sql="""SELECT repo,count(*) AS events FROM github_events GROUP BY repo HAVING events>5 EMIT periodic 10s SETTINGS seek_to='-1h'"""
st.code(sql, language="sql")
query = Query().sql(sql).create()
with st.empty():
    def update_row(row):
        rows=[]
        for x in row[0]:
            data = {}
            data['repo']=x[0]
            data['events']=x[1]
            rows.append(data)

        df = pd.DataFrame(rows, columns=["repo","events"])
        st.altair_chart(alt.Chart(df).mark_bar().encode(x='events:Q',y=alt.Y('repo:N',sort='-x'),tooltip=['events','repo']), use_container_width=True)
    stopper = Stopper()
    query.get_result_stream(stopper).pipe(ops.take(3)).subscribe(
        on_next=lambda i: update_row(i),
        on_error=lambda e: print(f"error {e}"),
        on_completed=lambda: stopper.stop(),
    )
    query.cancel().delete()