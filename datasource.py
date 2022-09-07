import _thread as thread
import json
import os

import rel
import websocket
from confluent_kafka import Producer

import ccloud_lib

BLACKLIST_TAGS = (
    "snapshot",
    "subscriptions",
    "heartbeat",
)


def on_message(ws, message):
    m = json.loads(message)
    if m["type"] in BLACKLIST_TAGS:
        return

    if m.get("trades"):
        return

    producer.produce(topic, value=message)
    producer.flush()
    print(message)


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        ws.send(logon_msg)

    thread.start_new_thread(run, ())


if __name__ == "__main__":
    # Read arguments and configurations and initialize
    topic = os.environ.get("KAFKA_TOPIC")+"_"+os.environ.get("EXCHANGE_SOURCE_NAME")
    conf = ccloud_lib.read_ccloud_config("confluent.config")
    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    logon_msg = os.environ.get("EXCHANGE_LOGON_MSG")
    ws = websocket.WebSocketApp(
        os.environ.get("EXCHANGE_SOURCE_URL"),
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection

    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
