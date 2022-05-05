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

package com.google.cloud.bqetl;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bqetl.mbdata.MusicBrainzDataObject;
import com.google.cloud.bqetl.mbdata.MusicBrainzTransforms;
import com.google.cloud.bqetl.mbschema.FieldSchemaListBuilder;
import com.google.cloud.bqetl.options.BQETLOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * This is a pipeline that denormalizes exported data from the musicbrainz dataset to create a
 * flattened, denormalized Big Query table of artists' recordings that repeats artist information
 * for each of their credited recordings.
 *
 * <p>In addition to standard Pipeline parameters, this main program takes the following additional
 * parameters: --bigQueryTablename= <project>:<dataset>.<tablename>
 * --loadingBucketURL=gs://<bucketname>
 *
 * <p>An example of how to run this pipeline: mvn compile exec:java \ -Dexec.mainClass=BQETLSimple \
 * -Dexec.args="--project=jlb-onboarding \ --loadingBucketURL=gs://mb-data \
 * --stagingLocation=gs://mb-data \ --runner=BlockingDataflowPipelineRunner \ --numWorkers=185 \
 * --maxNumWorkers=500 \ --bigQueryTablename=example_project:example_dataset.example_table \
 * --diskSizeGb=1000 \ --workerMachineType=n1-standard-1"
 */
public class BQETLSimple {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(BQETLOptions.class);

    /*
     * get the custom options
     */
    BQETLOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BQETLOptions.class);
    Pipeline p = Pipeline.create(options);

    /*
     * load the line delimited JSON files into keyed PCollections
     */
    PCollection<KV<Long, MusicBrainzDataObject>> artists;
    if (options.getPerformLookups()) {
      // [START loadArtistsWithLookups]
      artists =
          MusicBrainzTransforms.loadTable(
              p,
              "artist",
              "id",
              MusicBrainzTransforms.lookup("area", "id", "name", "area", "begin_area"),
              MusicBrainzTransforms.lookup("gender", "id", "name", "gender"));
      // [END loadArtistsWithLookups]
    } else {
      artists = MusicBrainzTransforms.loadTable(p, "artist", "id");
    }
    PCollection<KV<Long, MusicBrainzDataObject>> artistCreditName =
        MusicBrainzTransforms.loadTable(p, "artist_credit_name", "artist");
    PCollection<KV<Long, MusicBrainzDataObject>> recordingsByArtistCredit =
        MusicBrainzTransforms.loadTable(p, "recording", "artist_credit");

    /*
     * perform inner joins
     */
    // [START artist_artist_credit_join]
    PCollection<MusicBrainzDataObject> artistCredits =
        MusicBrainzTransforms.innerJoin("artists with artist credits", artists, artistCreditName);
    // [END artist_artist_credit_join]
    // [START byCall]
    PCollection<KV<Long, MusicBrainzDataObject>> artistCreditNamesByArtistCredit =
        MusicBrainzTransforms.by("artist_credit_name_artist_credit", artistCredits);
    // [END byCall]
    // [START joinCall]
    PCollection<MusicBrainzDataObject> artistRecordings =
        MusicBrainzTransforms.innerJoin(
            "joined recordings", artistCreditNamesByArtistCredit, recordingsByArtistCredit);
    // [END joinCall]

    /*
     * create the table schema for Big Query
     */
    TableSchema bqTableSchema = bqSchema(options.getPerformLookups());
    /*
     *  transform the joined MusicBrainzDataObject results into BQ Table rows
     */
    // [START transformToTableRowCall]
    PCollection<TableRow> tableRows =
        MusicBrainzTransforms.transformToTableRows(artistRecordings, bqTableSchema);
    // [END transformToTableRowCall]
    /*
     * write the tablerows to Big Query
     */
    // [START bigQueryWrite]
    tableRows.apply(
        "Write to BigQuery",
        BigQueryIO.writeTableRows()
            .to(options.getBigQueryTablename())
            .withSchema(bqTableSchema)
            .withCustomGcsTempLocation(StaticValueProvider.of(options.getTempLocation()))
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    // [END bigQueryWrite]

    p.run().waitUntilFinish();
  }

  private static TableSchema bqSchema(boolean usingAreaGenderLookups) {
    FieldSchemaListBuilder fieldSchemaListBuilder = new FieldSchemaListBuilder();

    fieldSchemaListBuilder
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
        .intField("artist_edits_pending")
        .timestampField("artist_last_updated")
        .stringField("artist_comment")
        .boolField("artist_ended")
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
        .boolField("recording_video");

    if (usingAreaGenderLookups) {
      fieldSchemaListBuilder
          .stringField("artist_area")
          .stringField("artist_gender")
          .stringField("artist_begin_area");
    } else {
      fieldSchemaListBuilder
          .intField("artist_area")
          .intField("artist_gender")
          .intField("artist_begin_area");
    }

    return fieldSchemaListBuilder.schema();
  }
}
