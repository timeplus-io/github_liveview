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
    .audience(st.secrets["TIMEPLUS_AUDIENCE"])
    .login(st.secrets["AUTH0_API_CLIENT_ID"], st.secrets["AUTH0_API_CLIENT_SECRET"])
)
Env.setCurrent(env)

sql="""SELECT repo,count(*) AS events FROM table(github_events) WHERE _tp_time>date_sub(now(),4h) GROUP BY repo ORDER BY events DESC LIMIT 10
"""
st.code(sql, language="sql")
result=Query().execSQL(sql,1000)
col = [h["name"] for h in result["header"]]
df = pd.DataFrame(result["data"], columns=col)
st.altair_chart(alt.Chart(df).mark_bar().encode(x='events:Q',y=alt.Y('repo:N',sort='-x'),tooltip=['events','repo']), use_container_width=True)