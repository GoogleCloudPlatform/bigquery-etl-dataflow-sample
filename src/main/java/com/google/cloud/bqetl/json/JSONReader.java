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

package com.google.cloud.bqetl.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bqetl.mbdata.MusicBrainzDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses builtin jackson parser to parse a line of JSON and turn it into an MusicBrainzDataObject.
 */
public class JSONReader {

  private static final Logger logger = LoggerFactory.getLogger(JSONReader.class);
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  /**
   * This method attempts to transform the json node into an object with a known type.
   *
   * @return an Object with the apparent type from JSON (number types are given their wide
   *     equivalent (Long for ints, Double for float)
   */
  private static Object nodeValueToObject(
      JsonNode node) { // No child objects or arrays in this flat data just text/number
    switch (node.getNodeType()) {
      case NUMBER:
        if (node.isFloat() || node.isDouble()) {
          return node.doubleValue();
        } else {
          // For simplicity let all integers be Long.
          return node.asLong();
        }
      case STRING:
        return node.asText();
      case BOOLEAN:
        return node.asBoolean();
      case NULL:
        return null;
      default:
        logger.warn("Unknown node type:" + node.getNodeType());
        return null;
    }
  }

  /**
   * Reads an MusicBrainzDataObject from a json string.
   *
   * @param objectName - the namespace for the object
   * @param json the json string
   * @return the parsed object
   */
  public static MusicBrainzDataObject readObject(String objectName, String json) {
    MusicBrainzDataObject datum = new MusicBrainzDataObject(objectName);
    try {
      JsonParser parser = JSON_FACTORY.createParser(json);
      parser.setCodec(new ObjectMapper());
      while (!parser.isClosed()) {
        JsonToken token = parser.nextToken();

        if (token != null && token.equals(JsonToken.START_OBJECT)) {

          JsonNode jsonTree = parser.readValueAsTree();
          jsonTree
              .fields()
              .forEachRemaining(
                  entry -> {
                    if (entry.getValue() != null) {
                      Object value = nodeValueToObject(entry.getValue());
                      if (value != null) {
                        datum.addColumnValue(entry.getKey(), nodeValueToObject(entry.getValue()));
                      }
                    } else {
                      logger.warn("null value for entry : " + entry.getKey());
                    }
                  });
        }
      }
    } catch (Exception e) {
      logger.error("parse exception", e);
    }
    return datum;
  }
}
