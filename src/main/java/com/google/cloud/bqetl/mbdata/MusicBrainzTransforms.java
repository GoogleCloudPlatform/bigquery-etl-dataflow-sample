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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bqetl.json.JSONReader;
import com.google.cloud.bqetl.mbschema.FieldSchemaListBuilder;
import com.google.cloud.bqetl.options.BQETLOptions;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MusicBrainzTransforms {

  /** This is a library of reusable transforms for the musicbrainz dataset. */
  private static final int BIGQUERY_NESTING_LIMIT = 1000;

  private static final Logger logger = LoggerFactory.getLogger(MusicBrainzTransforms.class);

  /**
   * Given a PCollection of MusicBrainzDataObject's turn into to a keyed collection keyed by the
   * given key.
   *
   * @param name - name of the column value to use as the key
   * @param input - the PCollection of MusicBrainzDataObject's note that the column value is assumed
   *     to be a Long
   */
  public static PCollection<KV<Long, MusicBrainzDataObject>> by(
      String name, PCollection<MusicBrainzDataObject> input) {
    return input.apply(
        "by " + name,
        MapElements.into(new TypeDescriptor<KV<Long, MusicBrainzDataObject>>() {})
            .via(
                (MusicBrainzDataObject inputObject) -> {
                  try {
                    return KV.of((Long) inputObject.getColumnValue(name), inputObject);
                  } catch (Exception e) {
                    logger.error(" exception in by " + name, e);
                    return null;
                  }
                }));
  }

  private static PCollection<KV<Long, CoGbkResult>> group(
      String name,
      PCollection<KV<Long, MusicBrainzDataObject>> first,
      PCollection<KV<Long, MusicBrainzDataObject>> second,
      TupleTag<MusicBrainzDataObject> firstTag,
      TupleTag<MusicBrainzDataObject> secondTag) {
    PCollection<KV<Long, CoGbkResult>> joinedResult;
    try {
      joinedResult =
          KeyedPCollectionTuple.of(firstTag, first)
              .and(secondTag, second)
              .apply("joinResult_" + name, CoGroupByKey.create());
    } catch (Exception e) {
      logger.error("exception grouping.", e);
      return null;
    }
    return joinedResult;
  }

  /**
   * Perform an inner join of two keyed PCollections of MusicBrainzDataObjects and merge the results
   * into a list of MusicBrainzDataObjects.
   *
   * @param table1 - Keyed PCollection of MusicBrainzDataObject's
   * @param table2 - Keyed PCollection of MusicBrainzDataObject's
   */
  // [START innerJoin]
  public static PCollection<MusicBrainzDataObject> innerJoin(
      String name,
      PCollection<KV<Long, MusicBrainzDataObject>> table1,
      PCollection<KV<Long, MusicBrainzDataObject>> table2) {
    final TupleTag<MusicBrainzDataObject> t1 = new TupleTag<MusicBrainzDataObject>() {};
    final TupleTag<MusicBrainzDataObject> t2 = new TupleTag<MusicBrainzDataObject>() {};
    PCollection<KV<Long, CoGbkResult>> joinedResult = group(name, table1, table2, t1, t2);
    // [END innerJoin]
    // [START mergeJoinResults]
    PCollection<List<MusicBrainzDataObject>> mergedResult =
        joinedResult.apply(
            "merge join results",
            MapElements.into(new TypeDescriptor<List<MusicBrainzDataObject>>() {})
                .via(
                    (KV<Long, CoGbkResult> group) -> {
                      List<MusicBrainzDataObject> result = new ArrayList<>();
                      Iterable<MusicBrainzDataObject> leftObjects = group.getValue().getAll(t1);
                      Iterable<MusicBrainzDataObject> rightObjects = group.getValue().getAll(t2);
                      leftObjects.forEach(
                          (MusicBrainzDataObject l) ->
                              rightObjects.forEach(
                                  (MusicBrainzDataObject r) -> result.add(l.duplicate().merge(r))));
                      return result;
                    }));
    // [END mergeJoinResults]
    // [START flattenMergedResults]
    return mergedResult.apply("Flatten List to Objects", Flatten.iterables());
    // [END flattenMergedResults]
  }

  /**
   * Given a parent PCollection with a known value for a key, nest a given child collection based on
   * it's key value within elements of the first collection.
   *
   * @param parent - Keyed PCollection of Parent MusicBrainzDataObject's
   * @param child - Keyed PCollection of Child MusicBrainzDataObject's
   */
  // [START nestTransform]
  public static PCollection<MusicBrainzDataObject> nest(
      PCollection<KV<Long, MusicBrainzDataObject>> parent,
      PCollection<KV<Long, MusicBrainzDataObject>> child,
      String nestingKey) {
    final TupleTag<MusicBrainzDataObject> parentTag = new TupleTag<MusicBrainzDataObject>() {};
    final TupleTag<MusicBrainzDataObject> childTag = new TupleTag<MusicBrainzDataObject>() {};

    PCollection<KV<Long, CoGbkResult>> joinedResult =
        group("nest " + nestingKey, parent, child, parentTag, childTag);
    return joinedResult.apply(
        "merge join results " + nestingKey,
        MapElements.into(new TypeDescriptor<MusicBrainzDataObject>() {})
            .via(
                (KV<Long, CoGbkResult> group) -> {
                  MusicBrainzDataObject parentObject = group.getValue().getOnly(parentTag);
                  Iterable<MusicBrainzDataObject> children = group.getValue().getAll(childTag);
                  List<MusicBrainzDataObject> childList = new ArrayList<>();
                  children.forEach(childList::add);
                  parentObject = parentObject.duplicate();
                  parentObject.addColumnValue("recordings", childList);
                  return parentObject;
                }));
  }
  // [END nestTransform]

  /*
   * Create a simple serializable version of the TableSchema usable across worker nodes.
   */
  private static Map<String, Object> serializeableTableSchema(TableSchema schema) {
    return serializeableTableSchema(schema.getFields());
  }

  /*
   * Recursable method for serializable schema
   */
  private static Map<String, Object> serializeableTableSchema(List<TableFieldSchema> fields) {
    HashMap<String, Object> current = new HashMap<>();
    for (TableFieldSchema field : fields) {
      if (field.getType().equals(FieldSchemaListBuilder.RECORD)) {
        current.put(field.getName(), serializeableTableSchema(field.getFields()));
      } else {
        current.put(field.getName(), field.getType());
      }
    }
    return current;
  }

  /**
   * Given a set of MusicBrainzDataObject's representing table rows and a TableSchema that has
   * keynames that match with the PCollection of MusicBrainzDataObjects, execute a transform to
   * transform those objects in to BigQuery table rows. Only use the fields of the data objects
   * found in the Table schema.
   *
   * @param objects - the PCollection of data objects to transform into TableRows
   * @param schema - the table schema to use
   */
  public static PCollection<TableRow> transformToTableRows(
      PCollection<MusicBrainzDataObject> objects, TableSchema schema) {
    Map<String, Object> serializableSchema = serializeableTableSchema(schema);
    return objects
        .apply(
            "Big Query TableRow Transform",
            MapElements.into(new TypeDescriptor<List<TableRow>>() {})
                .via(
                    (MusicBrainzDataObject inputObject) ->
                        toTableRows(inputObject, serializableSchema)))
        .apply("Flatten TableRow List to TableRows", Flatten.iterables());
  }

  /**
   * This converts a single MusicBrainzDataObject into a list of one or more TableRows, nesting as
   * necessary. It uses the BIGQUERY_NESTING_LIMIT to duplicate rows and continue adding nested
   * records to their duplicate. For example if a MusicBrainzDataObject has a child list of
   * BIGQUERY_NESTING_LIMIT + 1 nested objects the result will be a list containing two table rows 1
   * with BIGQUERY_NESTINGLIMIT children and a duplicate with one child.
   */
  // [START toTableRows]
  private static List<TableRow> toTableRows(
      MusicBrainzDataObject mbdo, Map<String, Object> serializableSchema) {
    TableRow row = new TableRow();
    List<TableRow> result = new ArrayList<>();
    Map<String, List<MusicBrainzDataObject>> nestedLists = new HashMap<>();
    Set<String> keySet = serializableSchema.keySet();
    /*
     *  construct a row object without the nested objects
     */
    int maxListSize = 0;
    for (String key : keySet) {
      Object value = serializableSchema.get(key);
      Object fieldValue = mbdo.getColumnValue(key);
      if (fieldValue != null) {
        if (value instanceof Map) {
          @SuppressWarnings("unchecked")
          List<MusicBrainzDataObject> list = (List<MusicBrainzDataObject>) fieldValue;
          if (list.size() > maxListSize) {
            maxListSize = list.size();
          }
          nestedLists.put(key, list);
        } else {
          row.set(key, fieldValue);
        }
      }
    }
    /*
     * add the nested objects but break up the nested objects across duplicate rows if nesting
     * limit exceeded
     */
    TableRow parent = row.clone();
    Set<String> listFields = nestedLists.keySet();
    for (int i = 0; i < maxListSize; i++) {
      parent = (parent == null ? row.clone() : parent);
      final TableRow parentRow = parent;
      nestedLists.forEach(
          (String key, List<MusicBrainzDataObject> nestedList) -> {
            if (nestedList.size() > 0) {
              if (parentRow.get(key) == null) {
                parentRow.set(key, new ArrayList<TableRow>());
              }
              @SuppressWarnings("unchecked")
              List<TableRow> childRows = (List<TableRow>) parentRow.get(key);
              @SuppressWarnings("unchecked")
              Map<String, Object> map = (Map<String, Object>) serializableSchema.get(key);
              childRows.add(toChildRow(nestedList.remove(0), map));
            }
          });
      if ((i > 0) && (i % BIGQUERY_NESTING_LIMIT == 0)) {
        result.add(parent);
        parent = null;
      }
    }
    if (parent != null) {
      result.add(parent);
    }
    return result;
  }
  // [END toTableRows]

  /**
   * A child row cannot have any nested repeated records. This turns a MusicBrainzDataObject into a
   * child row that is presumably nested inside a parent.
   */
  private static TableRow toChildRow(
      MusicBrainzDataObject object, Map<String, Object> childSchema) {
    TableRow row = new TableRow();
    childSchema.forEach((String key, Object value) -> row.set(key, object.getColumnValue(key)));
    return row;
  }

  /**
   * Given the cloud storage object containing a line delimited json file, a pipeline and a keyname
   * load MusicBrainzDataObject's into a keyed PCollection with the key being the column value for
   * keyName and applies the supplied mappings.
   *
   * @param p Pipeline object to use to load the data objects
   * @param name the name of the google cloud storage object
   * @param keyName the name of the column to use as the key for this PCollection. Note that this
   *     key assumed to be a Long.
   * @param mappers variable sized list of lookup descriptions. mappers map a Long integer to a
   *     String
   */
  public static PCollection<KV<Long, MusicBrainzDataObject>> loadTable(
      Pipeline p, String name, String keyName, LookupDescription... mappers) {
    PCollection<String> text = loadText(p, name);
    return loadTableFromText(text, name, keyName, mappers);
  }

  /**
   * Given the cloud storage object containing a line delimited json file, a pipeline and a keyname
   * load MusicBrainzDataObjects into a keyed PCollection with the key being the column value for
   * keyName
   *
   * @param p Pipeline object to use to load the data objects
   * @param name the name of the google cloud storage object
   * @param keyName the name of the column to use as the key for this PCollection Note that this key
   *     assumed to be a Long.
   */
  public static PCollection<KV<Long, MusicBrainzDataObject>> loadTable(
      Pipeline p, String name, String keyName) {
    PCollection<String> text = loadText(p, name);
    return loadTableFromText(text, name, keyName);
  }

  /**
   * Given the cloud storage object containing a line delimited json file, and a pipeline that has
   * the BQETLOptions set, load the MusicBrainzDataObjects into a PCollection.
   *
   * @param p Pipeline object to use for the load
   * @param name name of the google cloud storage object
   */
  public static PCollection<MusicBrainzDataObject> loadTable(Pipeline p, String name) {
    return loadTableFromText(loadText(p, name), name);
  }

  /**
   * Given a PCollection of String's each representing an MusicBrainzDataObject transform those
   * strings into KV<Long,MusicBrainzDataObject> where the name is the namespace of the data object
   * key is the value of the object's keyName property.
   *
   * @param text - the PCollection of strings
   * @param name - the namespace for the data objects (or row name)
   * @param keyName - the key to use as the key in the KV object.
   */
  // [START loadTableByValue]
  public static PCollection<KV<Long, MusicBrainzDataObject>> loadTableFromText(
      PCollection<String> text, String name, String keyName) {
    final String namespacedKeyname = name + "_" + keyName;
    return text.apply(
        "load " + name,
        MapElements.into(new TypeDescriptor<KV<Long, MusicBrainzDataObject>>() {})
            .via(
                (String input) -> {
                  MusicBrainzDataObject datum = JSONReader.readObject(name, input);
                  Long key = (Long) datum.getColumnValue(namespacedKeyname);
                  return KV.of(key, datum);
                }));
  }
  // [END loadTableByValue]

  /**
   * Given a PCollection of String's each representing an MusicBrainzDataObject transform those
   * strings into MusicBrainzDataObject's where the namespace for the MusicBrainzDataObject is
   * 'name'
   *
   * @param text the json string representing the MusicBrainzDataObject
   * @param name the namespace for hte MusicBrainzDataObject
   * @return PCollection of MusicBrainzDataObjects
   */
  public static PCollection<MusicBrainzDataObject> loadTableFromText(
      PCollection<String> text, String name) {
    return text.apply(
        "load : " + name,
        MapElements.into(new TypeDescriptor<MusicBrainzDataObject>() {})
            .via((String input) -> JSONReader.readObject(name, input)));
  }

  /**
   * Given a PCollection of Strings each with json containing a mapping from a Long to a String
   * create a Singleton PCollection with the mapping. Example mapping:
   *
   * <pre>
   * {
   * ...
   * "id" : 38,
   * .... "name": "Canada
   * ....
   * }
   * </pre>
   *
   * In this case keyKey is id and valueKey is name.
   *
   * @param text - json string containing the mapping
   * @param keyKey - the json key for the value that will serve as the key for the mapping
   * @param valueKey - the json key for the value that will serve as the value for the mapping
   */
  // [START lookupTableWithSideInputs1]
  public static PCollectionView<Map<Long, String>> loadMapFromText(
      PCollection<String> text, String name, String keyKey, String valueKey) {
    // column/Key names are namespaced in MusicBrainzDataObject
    String keyKeyName = name + "_" + keyKey;
    String valueKeyName = name + "_" + valueKey;

    PCollection<KV<Long, String>> entries =
        text.apply(
            "sideInput_" + name,
            MapElements.into(new TypeDescriptor<KV<Long, String>>() {})
                .via(
                    (String input) -> {
                      MusicBrainzDataObject object = JSONReader.readObject(name, input);
                      Long key = (Long) object.getColumnValue(keyKeyName);

                      String value = (String) object.getColumnValue(valueKeyName);
                      return KV.of(key, value);
                    }));

    return entries.apply(View.asMap());
  }
  // [END lookupTableWithSideInputs1]

  /**
   * Given a PCollection of String's each representing an MusicBrainzDataObject transform those
   * strings into MusicBrainzDataObject's where the name space for the MusicBrainzDataObject is
   * 'name'
   *
   * @param text the json string representing the MusicBrainzDataObject
   * @param name the namespace for hte MusicBrainzDataObject
   * @param mappers variable number of lookup descriptions - lookup descriptions can be created
   *     using the factory method lookup();
   * @return PCollection of MusicBrainzDataObjects
   */
  public static PCollection<KV<Long, MusicBrainzDataObject>> loadTableFromText(
      PCollection<String> text, String name, String keyName, LookupDescription... mappers) {
    // [START lookupTableWithSideInputs2]
    List<SimpleEntry<List<String>, PCollectionView<Map<Long, String>>>> mapSideInputs =
        new ArrayList<>();

    for (LookupDescription mapper : mappers) {
      PCollectionView<Map<Long, String>> mapView =
          loadMap(text.getPipeline(), mapper.objectName, mapper.keyKey, mapper.valueKey);
      List<String> destKeyList =
          mapper.destinationKeys.stream()
              .map(destinationKey -> name + "_" + destinationKey)
              .collect(Collectors.toList());

      mapSideInputs.add(new SimpleEntry<>(destKeyList, mapView));
    }
    // [END lookupTableWithSideInputs2]
    return loadTableFromText(text, name, keyName, mapSideInputs);
  }

  static PCollection<KV<Long, MusicBrainzDataObject>> loadTableFromText(
      PCollection<String> text,
      String name,
      String keyName,
      List<SimpleEntry<List<String>, PCollectionView<Map<Long, String>>>> sideMappings) {
    // List<PCollectionView<Map<Long, String>>> sideMappings) {

    final String namespacedKeyname = name + "_" + keyName;

    return text.apply(
        "load with SideMappings",
        ParDo.of(
                new DoFn<String, KV<Long, MusicBrainzDataObject>>() {

                  @ProcessElement
                  // public void processElement(ProcessContext processContext) throws Exception {
                  public void processElement(
                      @Element String input,
                      OutputReceiver<KV<Long, MusicBrainzDataObject>> out,
                      ProcessContext c) {

                    MusicBrainzDataObject result = JSONReader.readObject(name, input);

                    sideMappings.forEach(
                        (SimpleEntry<List<String>, PCollectionView<Map<Long, String>>> mapping) -> {
                          // [START lookupTableWithSideInputs3]
                          Map<Long, String> sideInputMap = c.sideInput(mapping.getValue());

                          List<String> keyList = mapping.getKey();

                          keyList.forEach(
                              (String key) -> {
                                Long id = (Long) result.getColumnValue(key);
                                if (id != null) {
                                  String label = sideInputMap.get(id);
                                  if (label == null) {
                                    label = "" + id;
                                  }
                                  result.replace(key, label);
                                  // [END lookupTableWithSideInputs3]
                                }
                              });
                        });

                    Long key = (Long) result.getColumnValue(namespacedKeyname);

                    out.output(KV.of(key, result));
                  }
                })
            .withSideInputs(
                sideMappings.stream().map(SimpleEntry::getValue).collect(Collectors.toList())));
  }

  /**
   * Given the cloud storage object containing a line delimited json file from which a map should be
   * defined by: value of json key (keyKey) -> value of other json key (valueKey) (maps should be
   * relatively small as they are generally used as side inputs for adding a mapping from a
   * non-user-friendly ID to a user-friendly name or description.
   *
   * @param name - name
   * @param keyKey - the name of the json key to use as the key in the resulting map.
   * @param valueKey - the name of hte json key to use as the value in the resulting map.
   */
  private static PCollectionView<Map<Long, String>> loadMap(
      Pipeline p, String name, String keyKey, String valueKey) {
    PCollection<String> text = loadText(p, name);
    return loadMapFromText(text, name, keyKey, valueKey);
  }

  /**
   * Load a json line delimited file into a PCollection of strings each representing a line of JSON.
   *
   * @param name name of objects to load.
   */
  // [START loadStrings]
  public static PCollection<String> loadText(Pipeline p, String name) {
    BQETLOptions options = (BQETLOptions) p.getOptions();
    String loadingBucket = options.getLoadingBucketURL();
    String objectToLoad = storedObjectName(loadingBucket, name);
    return p.apply(name, TextIO.read().from(objectToLoad));
  }
  // [END loadStrings]

  /**
   * Derive a storedObject name from the loading bucket and the name of the object.
   *
   * @param loadingBucket name of bucket
   * @param name - name of the object
   */
  private static String storedObjectName(String loadingBucket, String name) {
    return loadingBucket + "/" + name + ".json";
  }

  /**
   * Factory method for creating a lookup table description a lookup usually maps a primary key to a
   * label. Note that in this demonstration maps are assumed to be of the form Long -> String.
   *
   * @param objectName - the storage object name
   * @param keyKey - the keyname in the mapping table that will match the value in the target object
   *     to be replaced (e.g. id)
   * @param valueKey - the keyname in the mapping table that contains the value
   * @param destinationKeys - the key to replace in the target object
   */
  // [START lookupMethod]
  public static LookupDescription lookup(
      String objectName, String keyKey, String valueKey, String... destinationKeys) {
    return new LookupDescription(objectName, keyKey, valueKey, destinationKeys);
  }
  // [END lookupMethod]

  /**
   * Simple class for encapsulating the description of a lookup. Created through factory method
   * lookup().
   */
  public static class LookupDescription {

    final String objectName;
    final List<String> destinationKeys;
    final String keyKey;
    final String valueKey;

    LookupDescription(
        String objectName, String keyKey, String valueKey, String... destinationKeys) {
      this.objectName = objectName;
      this.destinationKeys = Arrays.asList(destinationKeys);
      this.keyKey = keyKey;
      this.valueKey = valueKey;
    }
  }
}
