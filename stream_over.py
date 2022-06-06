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
    st.markdown("[Source Code](https://github.com/timeplus-io/github_liveview/blob/develop/stream_over.py) | [About Timeplus](https://timeplus.com)", unsafe_allow_html=True)

env = (
    Env().schema(st.secrets["TIMEPLUS_SCHEMA"]).host(st.secrets["TIMEPLUS_HOST"]).port(st.secrets["TIMEPLUS_PORT"])
    .token(st.secrets["TIMEPLUS_TOKEN"])
    .audience(st.secrets["TIMEPLUS_AUDIENCE"]).client_id("TIMEPLUS_CLIENT_ID").client_secret("TIMEPLUS_CLIENT_SECRET")
)

st.header('Event count: today vs yesterday (every 5sec)')
sql="""
SELECT date_add(window_end,1d) AS time,count(*) AS cnt FROM tumble(table(github_events),5s) 
WHERE _tp_time BETWEEN date_sub(to_start_of_hour(now()),24h) AND date_sub(to_start_of_hour(now()),23h)
GROUP BY window_end
"""
st.text('purple line: yesterday')
st.code(sql, language="sql")
result=Query().execSQL(sql,1000)
col = [h["name"] for h in result["header"]]
df = pd.DataFrame(result["data"], columns=col)
c = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#D53C97'))

sql="""
SELECT window_end AS time,count(*) AS cnt FROM tumble(github_events,5s) WHERE _tp_time > to_start_of_hour(now())
GROUP BY window_end SETTINGS seek_to='-1h'
"""
query = Query().sql(sql).create()
st.text('green line: today')
st.code(sql, language="sql")
chart_st=st.empty()
rows=[]
def update_row(row):
    try:
        rows.append(row)
        col = [h["name"] for h in result["header"]]
        df = pd.DataFrame(rows, columns=col)
        c2 = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#52FFDB'))
        with chart_st:
            st.altair_chart(c+c2, use_container_width=True)
    except BaseException as err:
        with chart_st:
            st.error(f"Got an error while rendering chart. Please refresh the page.{err=}, {type(err)=}")
            query.cancel().delete()
query.get_result_stream().pipe(ops.take(600)).subscribe(
    on_next=lambda i: update_row(i),
    on_error=lambda e: print(f"error {e}"),
    on_completed=lambda: query.stop(),
)
query.cancel().delete()

