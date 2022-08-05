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

case "$1" in
"nested")
  CLASS_NAME=BQETLNested
  USE_LOOKUPS=""
  ;;
"simple")
  CLASS_NAME=BQETLSimple
  USE_LOOKUPS=""
  ;;
"simple-with-lookups")
  CLASS_NAME=BQETLSimple
  USE_LOOKUPS="--performLookups"
  ;;
*)
  echo "Pipeline type not specified (simple|simple-with-lookups|nested)"
  exit
  ;;
esac

check_required_value() {
  if [ _$1 == _ ]; then
    echo "$2 was not provided"
    exit
  fi
}

check_required_value "$PROJECT_ID" PROJECT_ID
check_required_value "$DATASET" DATASET
check_required_value "$DESTINATION_TABLE" DESTINATION_TABLE
check_required_value "$REGION" REGION
check_required_value "$SERVICE_ACCOUNT" SERVICE_ACCOUNT
check_required_value "$DATAFLOW_TEMP_BUCKET" DATAFLOW_TEMP_BUCKET

echo "Executing: "
set -x

mvn compile exec:java -e \
  -Dexec.mainClass=com.google.cloud.bqetl.${CLASS_NAME} \
  -Dexec.args="\
    --project=${PROJECT_ID} \
    --loadingBucketURL=gs://solutions-public-assets/bqetl  \
    --runner=DataflowRunner \
    --numWorkers=5 \
    --maxNumWorkers=10 \
    --bigQueryTablename=${PROJECT_ID}:${DATASET}.${DESTINATION_TABLE} \
    --region=${REGION} \
    --serviceAccount=${SERVICE_ACCOUNT} \
    --gcpTempLocation=${DATAFLOW_TEMP_BUCKET}/dftemp/ \
    --tempLocation=${DATAFLOW_TEMP_BUCKET}/temp/ \
    ${USE_LOOKUPS} \
    "
