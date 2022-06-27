import os, sys, json, time, datetime
from requests.exceptions import ReadTimeout
from github import (Github,enable_console_debug_logging,GithubException,RateLimitExceededException)
from timeplus import (Env,Stream,StreamColumn)
from timeplus.error import TimeplusAPIError

# Note, this script is no longer used. We now collect github events and push to Confluent Cloud

def log(msg):
    print(f"{datetime.datetime.now()} {msg}")

tp_schema = os.environ.get("TIMEPLUS_SCHEMA")
tp_host = os.environ.get("TIMEPLUS_HOST")
tp_port = os.environ.get("TIMEPLUS_PORT")
tp_apikey = os.environ.get("TIMEPLUS_API_KEY")

def login_tp():
    env = (
        Env().schema(tp_schema).host(tp_host).port(tp_port).api_key(tp_apikey)
    )
    return
login_tp()
try:
    s = (
        Stream().name("github_events")
        .column(StreamColumn().name("id").type("string"))
        .column(StreamColumn().name("created_at").type("datetime"))
        .column(StreamColumn().name("actor").type("string"))
        .column(StreamColumn().name("type").type("string"))
        .column(StreamColumn().name("repo").type("string"))
        .column(StreamColumn().name("payload").type("string"))
        .ttl_expression("created_at + INTERVAL 1 WEEK")
    )
    if(s.get() is None):
        s.create()
        log(f"Created a new stream {s.name()}")
except BaseException as err:
    sys.exit(f"Failed to list or create data streams from {tp_schema}://{tp_host}:{tp_port}. Please make sure you are connecting to the right server. {err=}, {type(err)=}")
g = Github(os.environ.get("GITHUB_TOKEN"),per_page=100)
try:
    user = g.get_user()
    log(f"Login successfully as {user.login}")
except GithubException:
    sys.exit("Please set the github personal access token for GITHUB_TOKEN")

#hacky way to avoid adding duplicated event id. PyGithub doesn't support etag
known_ids=set()
while(True):
    try:
        events = g.get_events()
        add_count=0
        total_count=0
        for e in events:
            total_count=total_count+1
            if e.id not in known_ids:
                known_ids.add(e.id)
                add_count=add_count+1
                s.insert([[e.id,e.created_at,e.actor.login,e.type,e.repo.name,json.dumps(e.payload)]])
        log(f"added {add_count} events, skipped {total_count-add_count} duplicate ones. Waiting 2 seconds to fetch again (ctrl+c to abort)")
        time.sleep(2)
    except KeyboardInterrupt:
        log("Good bye!")
        break
    except GithubException as err:
        log(f"Got GitHub error {err=}. Retry in 10 minutes")
        time.sleep(600)
    except ReadTimeout as err:
        log(f"Got timeout error {err=}. Re-login Timeplus and retry now")
        login_tp()
        time.sleep(10)
    except BaseException as err:
        log(f"Got other exceptions {err=}, {type(err)=}. Retry in 10 minutes")
        login_tp()
        time.sleep(600)