#!/bin/bash
#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
check_required_parameter "$DATASET" DATASET
check_required_parameter "$DESTINATION_TABLE" DESTINATION_TABLE
check_required_parameter "$REGION" REGION


mvn compile exec:java -e \
  -Dexec.mainClass=com.google.cloud.bqetl.${CLASS_NAME} \
  -Dexec.args="\
    --project=${PROJECT_ID} \
    --loadingBucketURL=gs://solutions-public-assets/bqetl  \
    --runner=DataflowRunner \
    --jobName=etl-into-bigquery-${CLASS_NAME} \
    --numWorkers=10 \
    --maxNumWorkers=20 \
    --bigQueryTablename=${PROJECT_ID}:${DATASET}.${DESTINATION_TABLE} \
    --region=${REGION} \
    "

