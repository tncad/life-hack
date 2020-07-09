#!/bin/bash

source /etc/environment
source ~/.profile

spark-shell --packages com.databricks:spark-xml_2.12:0.9.0
