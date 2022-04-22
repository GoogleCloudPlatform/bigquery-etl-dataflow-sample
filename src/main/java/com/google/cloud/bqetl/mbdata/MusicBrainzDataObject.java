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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that represents a row of musicbrainz data from any table using wrapped
 * HashMap&lt;String,Object&gt; It represents each column by adding the tablename as a namespace for
 * the column in the format tablename_ to distinguish keys
 */
public class MusicBrainzDataObject implements Serializable {

  // the namespace field for this object, typically the name of the table from musicbrainz
  private final String namespace;

  // the column names and values.
  private Map<String, Object> columns = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(MusicBrainzDataObject.class);

  /**
   * Constructs a new MusicBrainzDataObject with the namespace (tablename in the RDBMS) set to
   * supplied argument.
   *
   * @param namespace namespace for this MusicBrainzDataObject
   */
  public MusicBrainzDataObject(String namespace) {
    this.namespace = namespace;
  }

  /**
   * Get the namespace for this MusicBrainzDataObject
   *
   * @return name of the table for this MusicBrainzDataObject
   */
  public String getNamespace() {
    return namespace;
  }

  /** Get an iterator to iterate through all the columns on this MusicBrainzDataObject. */
  public Iterator<Map.Entry<String, Object>> getColumns() {
    return columns.entrySet().iterator();
  }

  /**
   * Adds a column value to this object prepending the namespace to the beginning. resulting key
   * will be tablename_columnname
   *
   * @param name the name of the column
   * @param value the value for the column
   */
  public void addColumnValue(String name, Object value) {
    String namespaced_name = getNamespace() + "_" + name;
    columns.put(namespaced_name, value);
  }

  /**
   * Removes a column value from this object.
   *
   * @param name - name of the column to delete (including namespace)
   */
  public Object removeColumnValue(String name) {
    return columns.remove(name);
  }

  /**
   * Get's a column value by its namespaced name.
   *
   * @param name - a string of the format tablename_columnname
   */
  public Object getColumnValue(String name) {
    return columns.get(name);
  }

  /**
   * Merges a another MusicBrainzDataObject's entries with this object's. When doing so it leaves
   * the other MusicBrainzDataObject's field namespaces intact. If the other MusicBrainzDataObject
   * has "othertablename_othercolumnname", the entry in this MusicBrainzDataObject will be
   * "othertablename_othercolumnname" and the namespace will not be changed.
   *
   * @param other - the Row to merge with this one
   */
  public MusicBrainzDataObject merge(MusicBrainzDataObject other) {

    if (other != null) {
      other.columns.forEach(
          (String key, Object value) -> {
            if (columns.containsKey(key)) {
              LOG.warn(
                  "Duplicate key:"
                      + key
                      + "found merging MusicBrainzDataObject "
                      + namespace
                      + " with "
                      + other.getNamespace());
            }
            columns.put(key, value);
          });
    }
    return this;
  }

  public void replace(String key, Object value) {
    columns.replace(key, value);
  }

  /** Makes a shallow clone of this object. */
  @SuppressWarnings("unchecked")
  public MusicBrainzDataObject duplicate() {
    MusicBrainzDataObject duplicate = new MusicBrainzDataObject(namespace);
    duplicate.columns = (HashMap<String, Object>) ((HashMap<String, Object>) columns).clone();
    return duplicate;
  }

  /** Deep equals */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MusicBrainzDataObject)) {
      return false;
    }
    MusicBrainzDataObject that = (MusicBrainzDataObject) o;
    return namespace.equals(that.namespace) && columns.equals(that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, columns);
  }

  @Override
  public String toString() {
    return "MusicBrainzDataObject{"
        + "namespace='"
        + namespace
        + '\''
        + ", columns="
        + columns
        + '}';
  }
}
