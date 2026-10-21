import json

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, TopicPartition, KafkaException

KAFKA_BROKERS = "localhost:9092"
FILTER_TOPIC = "position-filter-triggered"


def fetch_events():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "streamlit-filter-dashboard",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(FILTER_TOPIC, timeout=5)
        topic = metadata.topics[FILTER_TOPIC]
        if topic.error is not None:
            raise KafkaException(topic.error)

        # Read every partition from the start up to its current end offset.
        assignments = []
        remaining = {}
        for partition_id in topic.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(FILTER_TOPIC, partition_id), timeout=5
            )
            if high > low:
                assignments.append(TopicPartition(FILTER_TOPIC, partition_id, low))
                remaining[partition_id] = high
        if not assignments:
            return []
        consumer.assign(assignments)

        events = []
        while remaining:
            message = consumer.poll(2.0)
            if message is None:
                break
            if message.error():
                continue
            try:
                event = json.loads(message.value())
            except (TypeError, json.JSONDecodeError):
                event = {"raw": message.value().decode("utf-8", "replace")}
            event["partition"] = message.partition()
            event["offset"] = message.offset()
            events.append(event)
            if message.offset() + 1 >= remaining.get(message.partition(), 0):
                remaining.pop(message.partition(), None)
        return events
    finally:
        consumer.close()


@st.cache_resource
def source_event_cache():
    return {}


def fetch_source_events(refs):
    # Kafka records are immutable, so fetched source events can be cached forever.
    cache = source_event_cache()
    missing = [ref for ref in refs if ref not in cache]
    if missing:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "streamlit-filter-dashboard",
            "enable.auto.commit": False,
        })
        try:
            for topic, partition, offset in missing:
                consumer.assign([TopicPartition(topic, partition, offset)])
                message = consumer.poll(5.0)
                if (
                    message is None
                    or message.error()
                    or (message.topic(), message.partition(), message.offset())
                    != (topic, partition, offset)
                ):
                    continue
                try:
                    cache[(topic, partition, offset)] = json.loads(message.value())
                except (TypeError, json.JSONDecodeError):
                    continue
        finally:
            consumer.close()
    return {ref: cache[ref] for ref in refs if ref in cache}


st.set_page_config(page_title="Filter Triggered Events", layout="wide")
st.title("Filter Triggered Events")
st.caption(f"Topic: `{FILTER_TOPIC}` @ `{KAFKA_BROKERS}`")

auto_refresh = st.sidebar.toggle("Auto refresh", value=True)
interval = st.sidebar.number_input("Refresh interval (s)", 1, 60, 5)


@st.fragment(run_every=interval if auto_refresh else None)
def show_events():
    if st.button("Refresh"):
        st.rerun(scope="fragment")
    try:
        events = fetch_events()
    except KafkaException as exc:
        st.error(f"Could not read from Kafka: {exc}")
        return

    st.metric("Events", len(events))
    if not events:
        st.info("No events yet.")
        return

    refs = [
        (e["sourceTopic"], e["sourcePartition"], e["sourceOffset"])
        for e in events
        if {"sourceTopic", "sourcePartition", "sourceOffset"} <= e.keys()
    ]
    try:
        sources = fetch_source_events(refs)
    except KafkaException as exc:
        st.warning(f"Could not read source events: {exc}")
        sources = {}
    for event in events:
        source = sources.get(
            (event.get("sourceTopic"), event.get("sourcePartition"), event.get("sourceOffset"))
        )
        if source:
            event["latitude"] = source.get("latitude")
            event["longitude"] = source.get("longitude")

    df = pd.DataFrame(events)

    if {"latitude", "longitude"} <= set(df.columns):
        points = df[["latitude", "longitude"]].apply(pd.to_numeric, errors="coerce")
        if "timestamp" in df.columns:
            points["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
            points = points.sort_values("timestamp", kind="stable")
        points = points.dropna(subset=["latitude", "longitude"])
        # Latest point is last so it is drawn on top of the others.
        points["color"] = "#ff0000"
        if not points.empty:
            points.iloc[-1, points.columns.get_loc("color")] = "#00ff00"
        st.subheader("Original event positions")
        st.map(points, latitude="latitude", longitude="longitude", color="color", zoom=1)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        df = df.sort_values("timestamp", ascending=False)
    st.dataframe(df, use_container_width=True, hide_index=True)


show_events()
