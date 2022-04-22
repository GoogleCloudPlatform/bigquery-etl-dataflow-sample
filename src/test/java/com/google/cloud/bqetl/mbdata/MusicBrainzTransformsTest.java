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

package com.google.cloud.bqetl.mbdata;

import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class MusicBrainzTransformsTest {

  private List<String> artistCreditLinesOfJson;
  private List<String> recordingLinesOfJson;
  private List<String> artistLinesOfJson;
  private List<String> areaLinesOfJson;

  static final Logger LOG = LoggerFactory.getLogger(MusicBrainzTransformsTest.class);

  @org.junit.Test
  public void loadArtistCreditsByKey() {

    TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    Long[] artistCreditIds = {634509L, 846332L};
    PCollection<String> text =
        p.apply(Create.of(artistCreditLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollection<KV<Long, MusicBrainzDataObject>> artistCredits =
        MusicBrainzTransforms.loadTableFromText(text, "artist_credit_name", "artist_credit");

    PCollection<Long> artistCreditIdPCollection = artistCredits.apply(Keys.create());
    PAssert.that(artistCreditIdPCollection).containsInAnyOrder(634509L, 846332L);
  }

  @org.junit.Test
  public void joinArtistCreditsWithRecordings() {

    TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    PCollection<String> artistCreditText =
        p.apply("artistCredits", Create.of(artistCreditLinesOfJson)).setCoder(StringUtf8Coder.of());

    PCollection<KV<Long, MusicBrainzDataObject>> artistCredits =
        MusicBrainzTransforms.loadTableFromText(
            artistCreditText, "artist_credit_name", "artist_credit");

    PCollection<String> recordingText =
        p.apply("recordings", Create.of(recordingLinesOfJson)).setCoder(StringUtf8Coder.of());

    PCollection<KV<Long, MusicBrainzDataObject>> recordings =
        MusicBrainzTransforms.loadTableFromText(recordingText, "recording", "artist_credit");

    PCollection<MusicBrainzDataObject> joinedRecordings =
        MusicBrainzTransforms.innerJoin(
            "artist credits with recordings", artistCredits, recordings);

    PCollection<Long> recordingIds =
        joinedRecordings.apply(
            MapElements.into(new TypeDescriptor<Long>() {})
                .via((MusicBrainzDataObject mbo) -> (Long) mbo.getColumnValue("recording_id")));

    Long bieberRecording = 17069165L;
    Long bieberRecording2 = 15508507L;

    PAssert.that(recordingIds)
        .satisfies(
            (longs) -> {
              List<Long> theList = new ArrayList<>();
              longs.forEach(theList::add);
              assert (theList.contains(bieberRecording));
              assert (theList.contains(bieberRecording2));
              return null;
            });

    PCollection<Long> numberJoined =
        joinedRecordings.apply("count joined recordings", Count.globally());
    PCollection<Long> numberOfArtistCredits =
        artistCredits.apply("count artist credits", Count.globally());

    PAssert.thatSingleton(numberJoined).isEqualTo(448L);

    p.run();
  }

  @org.junit.Test
  public void loadArtistsWithMapping() {

    TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    PCollection<String> artistText =
        p.apply("artist", Create.of(artistLinesOfJson)).setCoder(StringUtf8Coder.of());

    List<SimpleEntry<List<String>, PCollectionView<Map<Long, String>>>> maps = new ArrayList<>();

    PCollection<String> areaMapText =
        p.apply("area", Create.of(areaLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollectionView<Map<Long, String>> areamap =
        MusicBrainzTransforms.loadMapFromText(areaMapText, "id", "area", "area");

    maps.add(new SimpleEntry<>(Collections.singletonList("area"), areamap));

    PCollection<KV<Long, MusicBrainzDataObject>> loadedArtists =
        MusicBrainzTransforms.loadTableFromText(artistText, "artist", "id", maps);

    PCollection<String> areas =
        loadedArtists.apply(
            "areaLabels",
            MapElements.into(new TypeDescriptor<String>() {})
                .via(
                    (KV<Long, MusicBrainzDataObject> row) ->
                        (String) row.getValue().getColumnValue("area")));

    PAssert.that(areas)
        .satisfies(
            (areaLabels) -> {
              List<String> theList = new ArrayList<>();
              areaLabels.forEach(theList::add);
              assert (theList.contains("Canada"));
              return null;
            });
  }

  @org.junit.Test
  public void testNest() {
    TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    PCollection<String> artistText =
        p.apply("artist", Create.of(artistLinesOfJson)).setCoder(StringUtf8Coder.of());
    PCollection<String> artistCreditNameText =
        p.apply("artist_credit_name", Create.of(artistCreditLinesOfJson));
    PCollection<String> recordingText =
        p.apply("recording", Create.of(recordingLinesOfJson)).setCoder(StringUtf8Coder.of());

    PCollection<KV<Long, MusicBrainzDataObject>> artistsById =
        MusicBrainzTransforms.loadTableFromText(artistText, "artist", "id");

    PCollection<KV<Long, MusicBrainzDataObject>> recordingsByArtistCredit =
        MusicBrainzTransforms.loadTableFromText(recordingText, "recording", "artist_credit");
    PCollection<KV<Long, MusicBrainzDataObject>> artistCreditByArtistCredit =
        MusicBrainzTransforms.loadTableFromText(
            artistCreditNameText, "artist_credit_name", "artist_credit");

    PCollection<MusicBrainzDataObject> recordingsWithCredits =
        MusicBrainzTransforms.innerJoin(
            "credited recordings", artistCreditByArtistCredit, recordingsByArtistCredit);
    PCollection<KV<Long, MusicBrainzDataObject>> recordingsJoinedWithCredits =
        MusicBrainzTransforms.by("artist_credit_name_artist", recordingsWithCredits);
    PCollection<MusicBrainzDataObject> artistsWithNestedRecordings =
        MusicBrainzTransforms.nest(artistsById, recordingsJoinedWithCredits, "recordings");

    PAssert.that(artistsWithNestedRecordings)
        .satisfies(
            (artistCollection) -> {
              List<MusicBrainzDataObject> theList = new ArrayList<>();
              artistCollection.forEach(theList::add);

              assert (theList.size() == 1);
              @SuppressWarnings("unchecked")
              List<MusicBrainzDataObject> artist_recordings =
                  (List<MusicBrainzDataObject>) theList.get(0).getColumnValue("artist_recordings");
              assert (artist_recordings.size() == 448);

              return null;
            });

    p.run();
  }

  @org.junit.Before
  public void setUp() {
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
