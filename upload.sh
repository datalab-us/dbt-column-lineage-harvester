#!/bin/bash

gcloud auth application-default set-quota-project elastic-edm-dev
gcloud config set project elastic-edm-dev

bq load --source_format=PARQUET dea__stg.dbt_model_lineage ./dbt_model_lineage.parquet
bq load --source_format=PARQUET dea__stg.dbt_column_lineage ./dbt_column_lineage.parquet
