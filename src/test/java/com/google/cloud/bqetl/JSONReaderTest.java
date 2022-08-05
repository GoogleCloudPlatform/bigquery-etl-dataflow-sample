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

import com.google.cloud.bqetl.json.JSONReader;
import com.google.cloud.bqetl.mbdata.MusicBrainzDataObject;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JSONReaderTest {

  private String testArtistJSON;

  @org.junit.Test
  public void readMap() {
    MusicBrainzDataObject musicBrainzDataObject = JSONReader.readObject("artist", testArtistJSON);
    List<Map.Entry<String, Object>> entries = new ArrayList<>();
    musicBrainzDataObject.getColumns().forEachRemaining(entries::add);
    System.out.printf(
        "Columns set for artist %s : %d ",
        musicBrainzDataObject.getColumnValue("artist_name"), entries.size());
    assert (entries.size() == 15);
  }

  @org.junit.Before
  public void setUp() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    InputStream stream = classLoader.getResourceAsStream("artist.json");
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    testArtistJSON = reader.readLine();
  }
}
