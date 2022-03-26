import streamlit as st
import time
import os
import json
from rx import operators as ops

import pandas as pd

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

with st.container():
    sql="select count(*) from table(github_events)"
    cnt=Query().execSQL(sql)["data"][0][0]
    st.metric(label="Github events", value="{:,}".format(cnt))

    sql=f"select now(), {cnt}+count(*) as events from github_events"
    query = Query().sql(sql).create()
    col = [h["name"] for h in query.header()]
    def update_row(row):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
            if(i>0):
                data[f] = "{:,}".format(row[i])
        df = pd.DataFrame([data], columns=col)
        if "cnt_table" not in st.session_state:
            query_result_table = st.table(df)
            st.session_state["cnt_table"] = query_result_table
        else:
            st.session_state.cnt_table.add_rows(df)
    stopper = Stopper()
    query.get_result_stream(stopper).pipe(ops.take(6)).subscribe(
        on_next=lambda i: update_row(i),
        on_error=lambda e: print(f"error {e}"),
        on_completed=lambda: stopper.stop(),
    )
    query.cancel().delete()