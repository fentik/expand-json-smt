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

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class SpliceJSONArrayTest {
    private SpliceJSONArray<SinkRecord> xform = new SpliceJSONArray.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

/*     @Test
    public void schemaless() {
        xform.configure(new HashMap<>());

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);
        assertNull(transformedRecord);
    }
 */
/*     @Test
    public void basicCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("targetField", "agg_person_custom_fields");
        props.put("spliceField", "team_category_ordinal_number");
        props.put("outputField", "custom_field_values");

        xform.configure(props);


        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);
        value.put("agg_person_custom_fields", "[{\"custom_field_values\":\"[{\\\"date_ts\\\":null,\\\"option_id\\\":23561,\\\"option_value\\\":\\\"URT\\\"}]\",\"team_category_ordinal_number\":8},{\"custom_field_values\":\"[{\\\"date_ts\\\":null,\\\"option_id\\\":16327,\\\"option_value\\\":\\\"Female\\\"}]\",\"team_category_ordinal_number\":5}]");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord);
    }
 */
    @Test
    public void basicCase2() {
        final Map<String, String> props = new HashMap<>();
        props.put("targetField", "agg_person_custom_fields");
        props.put("spliceField", "team_category_ordinal_number");
        props.put("outputField", "custom_field_values");
        props.put("outputFieldType", "json_array");
        props.put("newTargetFieldPrefix", "agg_person_custom_field");

        xform.configure(props);


        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);
//        value.put("agg__person_custom_fields", value: "[{\"custom_field_values\":\"[{\\\"date_ts\\\":null,\\\"option_id\\":20439,\\"option_value\\":\\"\\"}]","team_category_ordinal_number":7},{"custom_field_values":"[{\\"date_ts\\":null,\\"option_id\\":20440,\\"option_value\\":\\"\\"}]\",\"team_category_ordinal_number\":8}]");

        value.put("agg_person_custom_fields", "[{\"custom_field_values\":\"[{\\\"date_ts\\\":null,\\\"option_id\\\":22702,\\\"option_value\\\":\\\"AI\\\"},{\\\"date_ts\\\":null,\\\"option_id\\\":22694,\\\"option_value\\\":\\\"MI\\\"}]\",\"team_category_ordinal_number\":1}]");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals("Josef", updatedValue.get("name"));
        assertEquals(42, updatedValue.get("age"));
        ArrayList<Map<String, Object>> output = (ArrayList<Map<String, Object>>) updatedValue.get("agg_person_custom_field__1");
        assertEquals(2, output.size());
        assertEquals(22702, output.get(0).get("option_id"));
        assertEquals("AI", output.get(0).get("option_value"));
    }

    /*
    @Test
    public void missingField() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "address");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("address","{}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("Struct{}", updatedValue.getStruct("address").toString());
    }

    @Test
    public void complex() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "location");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("location", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("name", "Josef");
        value.put("age", 42);
        value.put("location",
                "{\"country\":\"Czech Republic\",\"address\":{\"city\":\"Studenec\",\"code\":123},\"add\":0}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("age"));
        assertEquals("Josef", updatedValue.getString("name"));
        assertEquals("Studenec", updatedValue.getStruct("location").getStruct("address")
                .getString("city"));
        assertEquals(new Integer(123), updatedValue.getStruct("location").getStruct("address")
                .getInt32("code"));
        assertEquals("Czech Republic", updatedValue.getStruct("location").getString("country"));
    }

    @Test
    public void arrayCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct().field("obj", Schema.STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("obj","{\"ar1\":[1,2,3,4]}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals(4, updatedValue.getStruct("obj").getArray("ar1").size());
    }

    @Test
    public void testEmptyArray() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct().field("obj", Schema.STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("obj","{\"null\": {\"uIczQ\": []}}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertNotNull(updatedValue.getStruct("obj").getStruct("null"));
        assertEquals(0, updatedValue.getStruct("obj").getStruct("null").getArray("uIczQ").size());
    }

    @Test
    public void nullValue() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "location,metadata");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("age", Schema.INT32_SCHEMA)
                .field("location", Schema.OPTIONAL_STRING_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("age", 42);
        value.put("location", "{\"country\":\"USA\"}");
        value.put("metadata", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("age"));
        assertEquals("USA", updatedValue.getStruct("location").getString("country"));
        assertNull(updatedValue.get("metadata"));
    }

    @Test
    public void arrayInRoot() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "arr1,arr2,arr3");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("arr1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr3", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("arr1","[1,2,3,4]");
        value.put("arr2", "[]");
        value.put("arr3", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(4, updatedValue.getStruct("arr1").getArray("array").size());
        assertEquals(0, updatedValue.getStruct("arr2").getArray("array").size());
        assertNull(updatedValue.getStruct("arr3"));
    }

    @Test
    public void arrayNullMember() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "arr");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("arr", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("arr","[null, 3, 5]");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        final String expected = "Struct{arr=Struct{array=[null, 3, 5]}}";
        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void malformated() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj1,obj2,arr");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("obj1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("obj2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("obj1","{\"a\":\"msg\"}");
        value.put("obj2", "{malf");
        value.put("arr", "[]");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals("msg", updatedValue.getStruct("obj1").getString("a"));
        assertEquals(0, updatedValue.getStruct("arr").getArray("array").size());
        assertEquals("{malf",updatedValue.getStruct("obj2").getString("value"));
        assertTrue(updatedValue.getStruct("obj2").getString("error").startsWith("JSON reader"));
    }

    @Test
    public void deepField() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj1.obj2.obj3");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("obj1", SchemaBuilder.struct()
                        .field("obj2", SchemaBuilder.struct()
                                .field("obj3", Schema.OPTIONAL_STRING_SCHEMA))).build();
        final Struct obj2 = new Struct(schema.field("obj1").schema().field("obj2").schema());
        obj2.put("obj3", "{\"country\": \"USA\"}");
        final Struct obj1 = new Struct(schema.field("obj1").schema());
        obj1.put("obj2", obj2);
        final Struct struct = new Struct(schema);
        struct.put("obj1", obj1);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, struct, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("USA", updatedValue.getStruct("obj1").getStruct("obj2").getStruct("obj3")
                .getString("country"));
    } */
}
