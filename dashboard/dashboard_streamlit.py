import os
import pandas as pd
import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession
SILVER_PATH = os.path.abspath("data/silver/sensor_data_medallion")

@st.cache_data(ttl=60)
def load_silver():
    df = spark.read.format("delta").load(SILVER_PATH)
    pdf = df.toPandas()
    return pdf
# ----------------------------
# Spark Setup
# ----------------------------
spark = SparkSession.builder \
    .appName("GoldViewer") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ----------------------------
# Load Gold Table
# ----------------------------
GOLD_PATH = os.path.abspath("data/gold/sensor_data_medallion")

@st.cache_data(ttl=60)
def load_data():
    df = spark.read.format("delta").load(GOLD_PATH)
    pdf = df.toPandas()
    pdf["hour"] = pdf["hour"].astype(int)
    return pdf

# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config("📊 Gold Layer Dashboard", layout="wide")
st.title("📊 Gold Layer: Aggregated Zone Data")

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

pdf = load_data()

# Sidebar stats
st.sidebar.metric("Records", pdf["record_count"].sum())
st.sidebar.metric("Zones", pdf["zone"].nunique())
st.sidebar.metric("Hours", pdf["hour"].nunique())

# Zone filter
zones = pdf["zone"].unique().tolist()
selected = st.multiselect("Select Zones", zones, default=zones)
pdf = pdf[pdf["zone"].isin(selected)]

# Line Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Average CO₂ by Hour")
    fig = px.line(pdf, x="hour", y="avg_co2", color="zone", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Average Noise by Hour")
    fig = px.line(pdf, x="hour", y="avg_noise", color="zone", markers=True)
    st.plotly_chart(fig, use_container_width=True)

# Record Count Chart
st.subheader("Record Count by Zone")
fig = px.bar(pdf, x="zone", y="record_count", color="zone")
st.plotly_chart(fig, use_container_width=True)

# Data Table
st.subheader("Raw Aggregated Gold Data")
st.dataframe(pdf.sort_values(["zone", "hour"]), use_container_width=True)

st.subheader("🗺️ Sensor Locations (Silver Layer)")

silver_df = load_silver()

# Optional: show only recent or selected zones
silver_df = silver_df[silver_df["zone"].isin(selected)]

fig = px.scatter_mapbox(
    silver_df,
    lat="latitude",
    lon="longitude",
    color="zone",
    hover_data=["zone", "pollution_category", "co2_ppm", "noise_db", "timestamp"],
    zoom=13,
    height=500
)

fig.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":0,"l":0,"b":0})
st.plotly_chart(fig, use_container_width=True)
