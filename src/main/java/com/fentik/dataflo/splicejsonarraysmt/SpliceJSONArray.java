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
package com.fentik.dataflo.splicejsonarraysmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

/**
 * Main project class implementing JSON string transformation.
 */
abstract class SpliceJSONArray<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpliceJSONArray.class);

    interface ConfigName {
        String TARGET_FIELD = "targetField";
        String SPLICE_FIELD = "spliceField";
        String OUTPUT_FIELD = "outputField";
        String OUTPUT_FIELD_TYPE = "outputFieldType";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TARGET_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Target field name. The value should be a string representing JSON Array.")
            .define(ConfigName.SPLICE_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "The name of the field in targetField that is used to splice new entries into the record. The value of the field is used as a prefix for the new field name.")
            .define(ConfigName.OUTPUT_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "(Optional) - The field in the json object that should be emitted. If not provided, the entire object is returned as value of the new spliced key.")
            .define(ConfigName.OUTPUT_FIELD_TYPE, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "If the output field is JSON object or JSON array, set it to 'json' or 'json_array'");

    private static final String PURPOSE = "expand json";

    private String targetFieldName;
    private String spliceKeyFieldName;
    private String outputFieldName;
    private String outputFieldType;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        targetFieldName = config.getString(ConfigName.TARGET_FIELD);
        spliceKeyFieldName = config.getString(ConfigName.SPLICE_FIELD);
        outputFieldName = config.getString(ConfigName.OUTPUT_FIELD);
        outputFieldType = config.getString(ConfigName.OUTPUT_FIELD_TYPE);
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


    /* We need to handle any nested type that is not supported by kafka connect JSONConvertor
       (like BigDecimal and BigInteger (or nested JSONObjects/JSONArrays)). We convert them
       into string (since it works for Gem's current use case), but this is obviously not as
        generic as we would like - The right answer lies in defining an explicit schema.
    */
    private Map<String, Object> jsonMap(JSONObject jsonObject) {
        Map<String, Object> map = jsonObject.toMap();
        Map<String, Object> retVal = new HashMap<String, Object>(map.size());
        for (Map.Entry<String, Object> e : map.entrySet()) {
            Object val = e.getValue();
            if (val == null) {
                retVal.put(e.getKey(), null);
            } else {
                if (val instanceof BigDecimal || val instanceof BigInteger ||
                    val instanceof JSONArray || val instanceof JSONObject) {
                    retVal.put(e.getKey(), val.toString());
                } else {
                    retVal.put(e.getKey(), val);
                }
            }
        }
        return retVal;
    }

    private R applyWithoutSchema(R record) {
        try {
            Object recordValue = operatingValue(record);
            if (recordValue == null) {
                LOGGER.info("SpliceJSONArray record is null");
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
                if (targetFieldName.equals(fieldName)) {
                    // We assume the field is a string that encodes a JSON array.
                    JSONArray array = new JSONArray(e.getValue().toString());
                    for (int i = 0; i < array.length(); i++) {
                        JSONObject childObj = array.getJSONObject(i);
                        Object keyFieldValue = childObj.get(spliceKeyFieldName);
                        if (keyFieldValue == null) {
                            LOGGER.error("Failed to find splice key " + spliceKeyFieldName + " in " + childObj.toString());
                            // We did not find the spliceKeyFieldName in the obj. So, we skip it.
                            continue;
                        }
                        String newFieldName = targetFieldName + "_" + keyFieldValue.toString();
                        if (outputFieldName == null) {
                            // There was no output field provided. So, we output the entire obj.
                            newValue.put(newFieldName, jsonMap(childObj));
                        } else{
                            Object obj = childObj.get(outputFieldName);
                            if (obj == null) {
                                LOGGER.error("Failed to find " + outputFieldName + " in " + childObj.toString());
                                // If there is no field matching outputFieldName, skip it.
                                continue;
                            }
                            // Handle JSON and JSON_ARRAY types separately since we have to deserialize them in
                            // a manner that's compatible with JSONConvertor.
                            if (outputFieldType.equals("json")) {
                                JSONObject outputObj = new JSONObject(obj.toString());
                                newValue.put(newFieldName, jsonMap(outputObj));
                            } else if (outputFieldType.equals("json_array")) {
                                JSONArray outputObj = new JSONArray(obj.toString());
                                ArrayList<Map<String, Object>> outputArray = new ArrayList<Map<String, Object>>();
                                for (int j = 0; j < outputObj.length(); j++) {
                                    if (j < 10) {
                                        JSONObject o = outputObj.getJSONObject(j);
                                        outputArray.add(jsonMap(o));
                                    }
                                }
                                newValue.put(newFieldName, outputArray);
                            } else {
                                newValue.put(newFieldName, obj);
                            }
                        }
                    }
                } else {
                    newValue.put(e.getKey(), e.getValue());
                }
            }
            return newRecord(record, null, newValue);
        } catch (DataException e) {
            LOGGER.warn("SpliceJSONArray fields missing from record: " + record.toString(), e);
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

    public static class Value<R extends ConnectRecord<R>> extends SpliceJSONArray<R> {

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
