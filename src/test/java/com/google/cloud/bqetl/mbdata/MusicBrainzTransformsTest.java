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

package com.google.cloud.bqetl.mbdata;

import com.google.cloud.bqetl.mbdata.MusicBrainzTransforms;
import com.google.cloud.bqetl.mbdata.MusicBrainzDataObject;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Created by johnlabarge on 9/30/16.
 */
public class MusicBrainzTransformsTest {

  private List<String> artistCreditLinesOfJson;
  private List<String> recordingLinesOfJson;
  private List<String> artistLinesOfJson;
  private List<String> areaLinesOfJson;

  @org.junit.Test
  public void loadArtistCreditsByKey() {
    DirectPipeline p = DirectPipeline.createForTest();
    Long artistCreditIds[] = {634509L, 846332L};
    PCollection<String> text = p.apply(Create.of(artistCreditLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollection<KV<Long, MusicBrainzDataObject>> artistCredits = MusicBrainzTransforms.loadTableFromText(text, "artist_credit_name", "artist_credit");
    PCollection<Long> artistCreditIdPCollection =
        artistCredits.apply(MapElements.via((KV<Long, MusicBrainzDataObject> kv) -> {
              Long k = kv.getKey();
              return k;
            })
                .withOutputType(new TypeDescriptor<Long>() {
                })
        );
    DataflowAssert.that(artistCreditIdPCollection).containsInAnyOrder(634509L, 846332L);
  }

  @org.junit.Test
  public void joinArtistCreditsWithRecordings() {

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<String> artistCreditText = p.apply("artistCredits", Create.of(artistCreditLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollection<KV<Long, MusicBrainzDataObject>> artistCredits = MusicBrainzTransforms.loadTableFromText(artistCreditText, "artist_credit_name", "artist_credit");

    PCollection<String> recordingText = p.apply("recordings", Create.of(recordingLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollection<KV<Long, MusicBrainzDataObject>> recordings = MusicBrainzTransforms.loadTableFromText(recordingText, "recording", "artist_credit");

    PCollection<MusicBrainzDataObject> joinedRecordings = MusicBrainzTransforms.innerJoin("artist credits with recordings", artistCredits, recordings);

    PCollection<Long> recordingIds = joinedRecordings.apply(MapElements.via((MusicBrainzDataObject mbo) -> (Long) mbo.getColumnValue("recording_id")).
        withOutputType(new TypeDescriptor<Long>() {
        }));

    Long bieberRecording = 17069165L;
    Long bieberRecording2 = 15508507L;


    DataflowAssert.that(recordingIds).satisfies((longs) -> {
      List<Long> theList = new ArrayList<Long>();
      longs.forEach(theList::add);
      assert (theList.contains(bieberRecording));
      assert (theList.contains(bieberRecording2));
      return null;
    });

    PCollection<Long> numberJoined = joinedRecordings.apply("count joined recrodings", Count.globally());
    PCollection<Long> numberOfArtistCredits = artistCredits.apply("count artist credits", Count.globally());

    DirectPipelineRunner.EvaluationResults results = p.run();

    long joinedRecordingsCount = results.getPCollection(numberJoined).get(0);
    assert (448 == joinedRecordingsCount);
  }

  @org.junit.Test
  public void loadArtistsWithMapping() {

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<String> artistText = p.apply("artist", Create.of(artistLinesOfJson)).setCoder(StringUtf8Coder.of());
    Map<String, PCollectionView<Map<Long, String>>> maps = new HashMap<>();
    PCollection<String> areaMapText = p.apply("area", Create.of(areaLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollectionView<Map<Long, String>> areamap = MusicBrainzTransforms.loadMapFromText(areaMapText, "id", "area");
    maps.put("area", areamap);
    PCollection<KV<Long, MusicBrainzDataObject>> loadedArtists = MusicBrainzTransforms.loadTableFromText(artistText, "artist", "id", maps);

    PCollection<String> areas = loadedArtists.apply("areaLabels", MapElements.via((KV<Long, MusicBrainzDataObject> row) -> {
      return (String) row.getValue().getColumnValue("area");
    }).withOutputType(new TypeDescriptor<String>() {
    }));

    DataflowAssert.that(areas).satisfies((areaLabels) -> {
      List<String> theList = new ArrayList<>();
      areaLabels.forEach(theList::add);
      assert (theList.contains("Canada"));
      return null;
    });


  }

  @org.junit.Test
  public void testNest() {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> artistText = p.apply("artist", Create.of(artistLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollection<String> artistCreditNameText = p.apply("artist_credit_name", Create.of(artistCreditLinesOfJson));
    PCollection<String> recordingText = p.apply("recording", Create.of(recordingLinesOfJson)).setCoder(StringUtf8Coder.of());

    PCollection<KV<Long, MusicBrainzDataObject>> artistsById = MusicBrainzTransforms.loadTableFromText(artistText, "artist", "id");

    PCollection<KV<Long, MusicBrainzDataObject>> recordingsByArtistCredit =
        MusicBrainzTransforms.loadTableFromText(recordingText, "recording", "artist_credit");
    PCollection<KV<Long, MusicBrainzDataObject>> artistCreditByArtistCredit =
        MusicBrainzTransforms.loadTableFromText(artistCreditNameText, "artist_credit_name", "artist_credit");

    PCollection<MusicBrainzDataObject> recordingsWithCredits = MusicBrainzTransforms.innerJoin("credited recordings", artistCreditByArtistCredit, recordingsByArtistCredit);
    PCollection<KV<Long, MusicBrainzDataObject>> recordingsJoinedWithCredits =
        MusicBrainzTransforms.by("artist_credit_name_artist", recordingsWithCredits);
    PCollection<MusicBrainzDataObject> artistsWithNestedRecordings = MusicBrainzTransforms.nest(artistsById, recordingsJoinedWithCredits, "recordings");


    DirectPipelineRunner.EvaluationResults results = p.run();

    List<MusicBrainzDataObject> resultObjects = results.getPCollection(artistsWithNestedRecordings);
    assert (resultObjects.size() == 1);
    assert (((List<MusicBrainzDataObject>) resultObjects.get(0).getColumnValue("artist_recordings")).size() == 448);


  }

  @org.junit.Before
  public void setUp() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    InputStream stream = classLoader.getResourceAsStream("artist_credit_name.json");
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    artistCreditLinesOfJson = reader.lines().collect(toList());

    stream = classLoader.getResourceAsStream("recording.json");
    reader = new BufferedReader(new InputStreamReader(stream));
    recordingLinesOfJson = reader.lines().collect(toList());

    stream = classLoader.getResourceAsStream("artist.json");
    reader = new BufferedReader(new InputStreamReader(stream));
    artistLinesOfJson = reader.lines().collect(toList());

    stream = classLoader.getResourceAsStream("area.json");
    reader = new BufferedReader(new InputStreamReader(stream));
    areaLinesOfJson = reader.lines().collect(toList());
  }
}
