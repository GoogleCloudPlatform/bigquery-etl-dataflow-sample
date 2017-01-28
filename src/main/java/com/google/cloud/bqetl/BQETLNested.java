/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.bqetl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bqetl.mbdata.MusicBrainzDataObject;
import com.google.cloud.bqetl.mbdata.MusicBrainzTransforms;
import com.google.cloud.bqetl.mbschema.FieldSchemaListBuilder;
import com.google.cloud.bqetl.options.BQETLOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class BQETLNested {
  private static final Logger logger = LoggerFactory.getLogger(BQETLNested.class);

  public static void main(String[] args) {
    PipelineOptionsFactory.register(BQETLOptions.class);

    // get the custom options
    BQETLOptions BQETLOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BQETLOptions.class);
    Pipeline p = Pipeline.create(BQETLOptions);

    // load the line delimited JSON into keyed PCollections
    PCollection<KV<Long, MusicBrainzDataObject>> artists = MusicBrainzTransforms.loadTable(p, "artist", "id",
        MusicBrainzTransforms.lookup("area", "id", "name", "area"),  // removed begin_area from list of destinationKeys to replace
        MusicBrainzTransforms.lookup("gender", "id", "name", "gender"));  // moved second gender to the end as a destinationKey to replace
    PCollection<KV<Long, MusicBrainzDataObject>> artistCreditName = MusicBrainzTransforms.loadTable(p, "artist_credit_name", "artist_credit");
    PCollection<KV<Long, MusicBrainzDataObject>> recordingsByArtistCredit = MusicBrainzTransforms.loadTable(p, "recording", "artist_credit");

    // changed innerJoin result name from nested recordings to recordings
    PCollection<MusicBrainzDataObject> recordingCredits = MusicBrainzTransforms.innerJoin("recordings", artistCreditName, recordingsByArtistCredit);
    PCollection<MusicBrainzDataObject> artistsWithRecordings = MusicBrainzTransforms.nest(artists, MusicBrainzTransforms.by("artist_credit_name_artist", recordingCredits), "recordings");

    // create the table schema for Big Query
    TableSchema bqTableSchema = bqSchema();
    // transform the joined MusicBrainzDataObject results into BQ Table rows
    PCollection<TableRow> tableRows = MusicBrainzTransforms.transformToTableRows(artistsWithRecordings, bqTableSchema);
    // write the tablerows to Big Query
    try {
      tableRows.apply(BigQueryIO.Write
          .named("Write")
          .to(BQETLOptions.getBigQueryTablename())
          .withSchema(bqTableSchema)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    } catch (Exception e) {
      logger.error("error writing to BQ: ", e);
    }
    p.run();
  }

  private static TableSchema bqSchema() {
    return FieldSchemaListBuilder.create()
        .intField("artist_id")
        .stringField("artist_gid")
        .stringField("artist_name")
        .stringField("artist_sort_name")
        .intField("artist_begin_date_year")
        .intField("artist_begin_date_month")
        .intField("artist_begin_date_day")
        .intField("artist_end_date_year")
        .intField("artist_end_date_month")
        .intField("artist_end_date_day")
        .intField("artist_type")
        /*Switch these two lines when using mapping table for artist_area */
        .stringField("artist_area")
        //.intField("artist_area")
        /*Switch these two lines when using mapping table for artist_gender */
        .stringField("artist_gender")
        //.intField("artist_gender")
        .intField("artist_edits_pending")
        .timestampField("artist_last_updated")
        .stringField("artist_comment")
        .boolField("artist_ended")
        .intField("artist_begin_area")
        .field(FieldSchemaListBuilder.create()
            .intField("artist_credit_name_artist_credit")
            .intField("artist_credit_name_position")
            .intField("artist_credit_name_artist")
            .stringField("artist_credit_name_name")
            .stringField("artist_credit_name_join_phrase")
            .intField("recording_id")
            .stringField("recording_gid")
            .stringField("recording_name")
            .intField("recording_artist_credit")
            .intField("recording_length")
            .stringField("recording_comment")
            .intField("recording_edits_pending")
            .timestampField("recording_last_updated")
            .boolField("recording_video")
            .repeatedRecord("artist_recordings")).schema();
  }
}
