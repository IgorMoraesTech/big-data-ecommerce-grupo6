#!/usr/bin/env bash
set -euo pipefail
hdfs dfs -mkdir -p /bigdata/ecommerce/raw
hdfs dfs -mkdir -p /bigdata/ecommerce/curated
hdfs dfs -ls -R /bigdata/ecommerce || true
