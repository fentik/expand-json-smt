/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fentik.dataflo.expandjsonsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

/**
 * Main project class implementing JSON string transformation.
 */
abstract class ExpandJSON<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpandJSON.class);

    interface ConfigName {
        String TARGET_JSON_ARRAY = "targetField";
        String SPLICE_FIELD = "spliceField";
        String OUTPUT_FIELDS = "outputFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TARGET_JSON_ARRAY, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to a json array object.")
            .define(ConfigName.SPLICE_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "The name of the child field that should be used to flatten the array object.")
            .define(ConfigName.OUTPUT_FIELDS, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                    "List of path for variables in the json object that should be stored in the output");

    private static final String PURPOSE = "expand json";

    private String sourceFields;
    private String childFieldName;
    // The list of fields in the JSON object that we should emit.
    private List<String> childOutputFields;
    // For nested JSON objects (we only support 1-level), the fields that should be emitted as a field of the new JSON object.
    private Map<String, List<String> > grandChildOutputFields;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceFields = config.getString(ConfigName.TARGET_JSON_ARRAY);
        childFieldName = config.getString(ConfigName.SPLICE_FIELD);
        List<String> outputFields = config.getList(ConfigName.OUTPUT_FIELDS);

        childOutputFields = new ArrayList<String>();
        grandChildOutputFields = new HashMap<String, List<String> >();

        // Now, find the fields that we are supposed to emit.
        for (int i = 0; i < outputFields.size(); i++) {
            String s = outputFields.get(i);
            String[] result = s.split("[.]");
            if (result.length > 2) {
                LOGGER.info("We only supported 1-level nested fields for output.");
                continue;
            }
            if (result.length == 1) {
                LOGGER.info("We need to emit " + result[0]);
                childOutputFields.add(result[0]);
            }
            if (result.length == 2) {
                LOGGER.info("We need to emit " + result[0] + " . " + result[1]);
                List<String> grandChildList = grandChildOutputFields.get(result[0]);
                if (grandChildList == null) {
                    grandChildList = new ArrayList<String>();
                    grandChildOutputFields.put(result[0], grandChildList);
                }
                grandChildList.add(result[1]);
            }
        }
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applyWithoutSchema(record);
        } else {
            LOGGER.info("Records with schemas are not supported");
            return null;
        }
    }


    private R applyWithoutSchema(R record) {
        try {
            Object recordValue = operatingValue(record);
            if (recordValue == null) {
                LOGGER.info("Expandjson record is null");
                LOGGER.info(record.toString());
                return record;
            }
            // Walk through the record.
            // 1. If the field name matches the sourceField
            // 2. Read the value of the field into JSON
            // 3. Expect it to be an array
            // 4. For each JSON, use "SPLICE_FIELD" to create the name of new field
            // 5. Generate a new JSONObject that only has fields mentioned in OUTPUT_FIELDS.
            // 6. Convert it into the map and emit it
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final Map<String, Object> newValue = new HashMap<>(value.size());

            for (Map.Entry<String, Object> e : value.entrySet()) {
                final String fieldName = e.getKey();
                if (sourceFields.equals(fieldName)) {
                    // We assume the field is a string that encodes a JSON array.
                    JSONArray array = new JSONArray(e.getValue().toString());
                    for (int i = 0; i < array.length(); i++) {
                        JSONObject childObj = array.getJSONObject(i);
                        Object childField = childObj.get(childFieldName);
                        if (childField != null) {
                            String newFieldName = fieldName + "_" + childField.toString();
                            // Now, get the child (and granchild) fields from the object to
                            // childObj.remove(childFieldName);
                            JSONObject newChildObject = new JSONObject();
                            for (int j = 0; j < childOutputFields.size(); j++) {
                                Object obj = childObj.get(childOutputFields.get(j));
                                if (obj == null) {
                                    LOGGER.error("Failed to find " + childOutputFields.get(j) + " in " + childObj.toString());
                                } else {
                                    newChildObject.put(childOutputFields.get(j),obj);
                                }
                            }
                            for (Map.Entry<String, List<String>> grandChildEntry : grandChildOutputFields.entrySet()) {
                                String nestedChildFieldName = grandChildEntry.getKey();
                                JSONObject nestedChildObj = childObj.getJSONObject(nestedChildFieldName);
                                if (nestedChildObj == null) {
                                    LOGGER.error("Failed to find " + nestedChildFieldName + " in " + childObj.toString());
                                } else {
                                    List<String> grandChildFieldNames = grandChildEntry.getValue();
                                    for (int k = 0; k < grandChildFieldNames.size(); k++) {
                                        Object obj = nestedChildObj.get(grandChildFieldNames.get(i));
                                        if (obj == null) {
                                            LOGGER.error("Failed to find " + grandChildFieldNames.get(k) + " in nested child " + nestedChildObj.toString());
                                        } else {
                                            newChildObject.put(grandChildFieldNames.get(k), obj);
                                        }
                                    }
                                }
                            }
                            // Convert the JSON object to a map before storing it.
                            Map<String, Object> output = childObj.toMap();
                            newValue.put(newFieldName, output);
                        }
                    }
                } else {
                    newValue.put(e.getKey(), e.getValue());
                }
            }
            return newRecord(record, null, newValue);
        } catch (DataException e) {
            LOGGER.warn("ExpandJSON fields missing from record: " + record.toString(), e);
            return record;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() { }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends ExpandJSON<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
