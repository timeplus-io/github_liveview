import streamlit as st
import time
import os
import json
from rx import operators as ops
from config import config
from case import case

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

image = Image.open("playground.png")


st.image(image, caption="Timeplus")
st.write("Fast + powerful real-time analytics made intuitive.")
st.write("See https://timeplus.com")


def select_env(environment):
    env_config = config[environment]
    env = (
        Env()
        .schema(env_config["schema"])
        .host(env_config["host"])
        .port(env_config["port"])
    )
    Env.setCurrent(env)
    st.sidebar.text(env.base_url())
    st.sidebar.write(env.info())


environment = st.sidebar.selectbox(
    "Select which environment to use", ("playground",)
)

selected_case = st.sidebar.selectbox("Select query case", tuple(case.keys()))

select_env(environment)

with st.container():
    sql = st.text_area("query", value=case[selected_case][1])
    if "sql" not in st.session_state:
        st.session_state["sql"] = sql

    if sql != st.session_state["sql"]:
        st.session_state["sql"] = sql
        if "table" in st.session_state:  # delete session table in case new SQL input
            del st.session_state["table"]

    if sql != "":
        query = Query().sql(sql).create()
        # st.write(query.header())
        col = [h["name"] for h in query.header()]

        def update_row(row):
            data = {}
            for i, f in enumerate(col):
                data[f] = row[i]
            df = pd.DataFrame([data], columns=col)
            if "table" not in st.session_state:
                query_result_table = st.table(df)
                st.session_state["table"] = query_result_table
            else:
                st.session_state.table.add_rows(df)

        stopper = Stopper()
        result = []
        query.get_result_stream(stopper).pipe(ops.take(10)).subscribe(
            on_next=lambda i: update_row(i),
            on_error=lambda e: print(f"error {e}"),
            on_completed=lambda: stopper.stop(),
        )

        query.cancel().delete()
