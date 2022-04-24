# github_liveview

A streamlit app to demonstrate how to easily build real-time app with Timeplus SDK. 

![Screnshot](https://miro.medium.com/max/1400/1*co2PwufPMW_sAlhnsPIt8g.png)

Check the blog [Building a Real-time App for Github in 2 Minutes](https://medium.com/www-timeplus-com/build-a-real-time-app-for-github-in-2-minutes-aec375463f61) for details.

The live demo of the main dashboard: https://share.streamlit.io/timeplus-io/github_liveview/develop

Other 2 examples with a single query:

* [liveview.py](liveview.py) list the latest GitHub events in a table
* [repos_to_follow.py](repos_to_follow.py) show the top 10 repos with most events in the past 4 hours and show them in a bar chart



## Basic workflow to call Timeplus API

First, you need to sign up the beta progarm of Timeplus on https://timeplus.com to get a tenant in your preferred cloud region and get an API token.

* To prepare the Timeplus API client, call `Env.setCurrent(Env().schema(tp_schema).host(tp_host).port(tp_port).token(tp_token))`
* To add data to a stream, call `Stream().name(stream_name).insert(rows)`
* To run a streaming query, call `q=Query().sql(stream_query).create()` then `q.get_result_stream().pipe(ops.take(MAX_ROW)).subscribe(..)`

Check https://pypi.org/project/timeplus/ for more details.

## How to run the code locally

There are 2 parts of the demo

* github_demo.py to continously call GitHub Event API and push data into Timeplus
* a few streamlit scripts to run streaming queries and visualize them as dashboard/table/charts

### Setup data loader

[github_demo.py](github_demo.py) is the python script to load real-time data from Github and send to your Timeplus tenant. To run this script, you need to create a Github Personal Access Token (PAT) and set it as the envirement variable`GITHUB_TOKEN`

You also need to expose Timeplus token and connections as environment variables. e.g.

```shell
export TIMEPLUS_SCHEMA=https TIMEPLUS_HOST=acme.beta.timeplus.com TIMEPLUS_PORT=443 \
TIMEPLUS_TOKEN="eyJhbGciOiJSUzI1NiIsInR5cCI6Ik"
```

Make you have Python 3.9.x installed. Install the following packages `pip install timeplus websocket-client rx loguru PyGithub`

Run `python github_demo.py` to start loading GitHub events to Timeplus. This script will create data stream if necessary.

### Visualize the GitHub live data

Follow https://docs.streamlit.io/library/get-started/installation to install streamlit. We recommend to use `pipenv`

Create a `secrets.toml` under the `.streamlit` folder to securely store your API token and connection to your Timeplus tenant. For example:

```toml
TIMEPLUS_HOST="acme.beta.timeplus.com"
TIMEPLUS_SCHEMA="https" 
TIMEPLUS_PORT=443
TIMEPLUS_TOKEN="eyJhbGciOiJSUzI1NiIsInR5cCI6Ik.."
```

Run with `streamlit run streamlit_app.py` to show the full dashboard. You can also run `streamlit run liveview.py  `  or `streamlit run repos_to_follow.py ` 

