#!/bin/bash

gcloud auth application-default set-quota-project $GCP_PROJECT_ID
gcloud config set project $GCP_PROJECT_ID

bq load --source_format=PARQUET dea__stg.dbt_model_lineage ./dbt_model_lineage.parquet
bq load --source_format=PARQUET dea__stg.dbt_column_lineage ./dbt_column_lineage.parquet
