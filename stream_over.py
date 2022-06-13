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
sql_yesterday="""
with cte as(select group_array(time) as timeArray, moving_sum(cnt) as cntArray from (SELECT date_add(window_end,1d) AS time,count(*) AS cnt FROM tumble(table(github_events),1h) WHERE _tp_time BETWEEN yesterday() AND to_start_of_day(now()) GROUP BY window_end ORDER BY time))select t.1 as time, t.2 as cnt from (select array_join(array_zip(timeArray,cntArray)) as t from cte)
"""
st.markdown('<font color=#D53C97>purple line: yesterday</font>',unsafe_allow_html=True)
st.code(sql_yesterday, language="sql")
sql2="""
with cte as (select group_array(time) as timeArray,moving_sum(cnt) as cntArray from(SELECT window_end AS time,count(*) AS cnt FROM tumble(github_events,5s) GROUP BY window_end))select t.1 as time, t.2 as cnt from (select array_join(array_zip(timeArray,cntArray)) as t from cte)
"""
st.markdown('<font color=#52FFDB>green line: today</font>',unsafe_allow_html=True)
st.code(sql2, language="sql")

# draw line for yesterday, 24 hours
result=Query().execSQL(sql_yesterday)
col = [h["name"] for h in result["header"]]
df = pd.DataFrame(result["data"], columns=col)
chart_yesterday = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#D53C97'))

# draw half line for today
sql_today_til_now="""
with cte as(select group_array(time) as timeArray, moving_sum(cnt) as cntArray from (SELECT window_end AS time,count(*) AS cnt FROM tumble(table(github_events),1h) WHERE _tp_time > to_start_of_day(now()) GROUP BY window_end ORDER BY time))select t.1 as time, t.2 as cnt from (select array_join(array_zip(timeArray,cntArray)) as t from cte)
"""
result=Query().execSQL(sql_today_til_now)
col = [h["name"] for h in result["header"]]
df = pd.DataFrame(result["data"], columns=col)
# cache the last count in this result
last_cnt=3000000
chart_today_til_now = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#52FFDB'))

chart_st=st.empty()
with chart_st:
    st.altair_chart(chart_yesterday+chart_today_til_now, use_container_width=True)

# draw second half of the line for upcoming data
sql2=f"""
with cte as (select group_array(time) as timeArray,moving_sum(cnt) as cntArray from(SELECT window_end AS time,count(*) AS cnt FROM tumble(github_events,5s) GROUP BY window_end))select t.1 as time, t.2+{last_cnt} as cnt from (select array_join(array_zip(timeArray,cntArray)) as t from cte)
"""
query = Query().sql(sql2).create()
rows=[]
def update_row(row):
    try:
        rows.append(row)
        col = [h["name"] for h in result["header"]]
        df = pd.DataFrame(rows, columns=col)
        chart_live = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#664CFC'))
        with chart_st:
            st.altair_chart(chart_yesterday+chart_today_til_now+chart_live, use_container_width=True)
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

