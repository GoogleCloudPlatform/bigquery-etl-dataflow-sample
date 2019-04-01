#!/bin/bash

if [ _$1 == _nested ]
then
    CLASS_NAME=BQETLNested
else
    CLASS_NAME=BQETLSimple 
fi

check_required_parameter () {
  if [ _$1 == _ ]
  then
    echo "$2 was not provided"
    exit
  fi
}


check_required_parameter "$PROJECT_ID" PROJECT_ID
check_required_parameter "$STAGING_BUCKET" STAGING_BUCKET
check_required_parameter "$DATASET" DATASET
check_required_parameter "$DESTINATION_TABLE" DESTINATION_TABLE
check_required_parameter "$ZONE" ZONE


mvn compile exec:java -e \
  -Dexec.mainClass=com.google.cloud.bqetl.${CLASS_NAME} \
  -Dexec.args="--project=${PROJECT_ID} \
    --loadingBucketURL=gs://solutions-public-assets/bqetl  \
    --tempLocation=gs://${STAGING_BUCKET}/temp \
    --gcpTempLocation=gs://${STAGING_BUCKET}/gcpTemp \
    --runner=DataflowRunner \
    --jobName=etl-into-bigquery-${CLASS_NAME} \
    --numWorkers=20 \
    --maxNumWorkers=70 \
    --bigQueryTablename=${PROJECT_ID}:${DATASET}.${DESTINATION_TABLE} \
    --diskSizeGb=100 \
    --zone=${ZONE} \
    --workerMachineType=n1-standard-1"

