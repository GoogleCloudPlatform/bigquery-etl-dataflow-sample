/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bqetl.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** The specific pipeline options for this project. */
public interface BQETLOptions extends PipelineOptions {
  @Description("Location of artist credit name json.")
  @Default.String("gs://mb-data")
  String getLoadingBucketURL();

  void setLoadingBucketURL(String loadingBucketURL);

  @Description("Big Query table name")
  @Default.String("musicbrainz_recordings_by_artist")
  String getBigQueryTablename();

  void setBigQueryTablename(String bigQueryTablename);

  @Description("Overwrite BigQuery table")
  @Default.Boolean(false)
  Boolean getOverwriteBigQueryTable();

  void setOverwriteBigQueryTable(Boolean overwriteBigQueryTable);

  @Description("Perform lookups for gender and area")
  @Default.Boolean(false)
  Boolean getPerformLookups();

  void setPerformLookups(Boolean performLookups);

}
