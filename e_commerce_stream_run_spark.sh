#!/bin/bash
set -e

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "client_e-commerce - E-commerce_stream" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.7.3 \
  client_e_commerce_e_commerce_stream_pipeline.py
