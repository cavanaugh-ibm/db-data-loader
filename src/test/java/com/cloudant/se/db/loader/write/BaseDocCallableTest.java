package com.cloudant.se.db.loader.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.loader.AppConstants.JsonType;
import com.cloudant.se.db.loader.AppOptions;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.read.BaseDataTableReader;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseDocCallableTest {
    private BaseDocCallable     callable = null;
    private AppConfig           config   = null;
    private DataTable           table    = null;
    private BaseDataTableReader reader   = null;

    public void setupPlumbing(String configpath) throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        File configFile = new File(configpath);
        config = mapper.readValue(configFile, AppConfig.class);
        config.setDefaultDirectory(configFile.getParentFile() != null ? configFile.getParentFile() : new File("."));
        config.mergeOptions(new AppOptions());
        config.validate();

        table = config.getTables().iterator().next();

        reader = new BaseDataTableReader(config, table, null) {
            @Override
            public Integer call() throws Exception {
                return 1;
            }
        };

        callable = new BaseDocCallable(config, table) {
            @Override
            protected WriteCode handle() throws Exception {
                return WriteCode.EXCEPTION;
            }

            @Override
            protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
                return failed;
            }
        };
    }

    @Test
    public void testAddObjectToArray() throws JsonParseException, JsonMappingException, IOException {
        setupPlumbing("src/test/resources/address.config.json");

        //
        // Load up the maps we have stored off to files in JSON
        Map<String, Object> parentActual = new ObjectMapper().reader(Map.class).readValue(new File("src/test/resources/maps/parent_array_map_before.json"));
        Map<String, Object> toAdd = new ObjectMapper().reader(Map.class).readValue(new File("src/test/resources/maps/parent_array_map_toadd.json"));
        Map<String, Object> parentExpected = new ObjectMapper().reader(Map.class).readValue(new File("src/test/resources/maps/parent_array_map_after.json"));

        //
        // Call the add
        callable.addObjectToArray(parentActual, table.getJsonNestField(), table.getJsonUniqueIdField(), "2369", toAdd);

        //
        // Make sure it matches are expected
        Assert.assertEquals(parentExpected, parentActual);

        //
        // Call the add to make sure it doesn't double add
        callable.addObjectToArray(parentActual, table.getJsonNestField(), table.getJsonUniqueIdField(), "2369", toAdd);

        //
        // Make sure it matches are expected
        Assert.assertEquals(parentExpected, parentActual);
    }

    @Test
    public void testProcessFields() throws JsonParseException, JsonMappingException, IOException {
        setupPlumbing("src/test/resources/generic.config.json");

        buildMap();
        callable.addFields(reader.getCurrentRow());

        // Logger.getLogger(DataTableField.class.getPackage().getName()).setLevel(Level.TRACE);

        callable.processFields();

        validateField("Field1", 10, true, JsonType.NUMBER);
        validateField("Field2", "20140721204145", true, JsonType.DATE_FORMATTED_STRING);
        validateField("Field3", new Date(1405975305000l), true, JsonType.DATE);
        validateField("Field4", 1405975305000l, true, JsonType.DATE_EPOCH);
        validateField("Field5", "I should be ignored", false, JsonType.STRING);
        validateField("Field6", "(555) 212-8379", true, JsonType.STRING);
        validateField("Field7", "Residential", true, JsonType.STRING);
        validateField("Field_Underscore", "Field with an underscore", true, JsonType.STRING);
        validateField("field_underscore_lowercase", "Field with an underscore that is lowercase", true, JsonType.STRING);
    }

    private void addField(String fileFieldname, String fileValue, boolean included, JsonType jsonType, String expectedDbFieldname, String expectedJsonFieldName) {
        reader.addField(fileFieldname, fileValue);

        if (included) {
            assertTrue(fileFieldname + " should exist in the current row", reader.getCurrentRow().containsKey(fileFieldname.toLowerCase()));

            FieldInstance instance = reader.getCurrentRow().get(fileFieldname.toLowerCase());
            assertEquals(jsonType, instance.getField().getJsonType());
            assertEquals(expectedDbFieldname, instance.getField().getDbFieldName());
            assertEquals(expectedJsonFieldName, instance.getField().getJsonFieldName());
        } else {
            assertFalse(fileFieldname + " should NOT exist in the current row", reader.getCurrentRow().containsKey(fileFieldname.toLowerCase()));
        }
    }

    @Test
    @Ignore
    public void testAddStringToArray() {
    }

    @Test
    @Ignore
    public void testBuildEmptyParent() {
    }

    @Test
    @Ignore
    public void testBuildIdFrom() {
    }

    @Test
    @Ignore
    public void testToMap() {
    }

    private void buildMap() {
        addField("Field1", "10", true, JsonType.NUMBER, "Field1", "Field1");
        addField("Field2", "2014-07-21T20:41:45+00:00", true, JsonType.DATE_FORMATTED_STRING, "Field2", "Field2");
        addField("Field3", "2014-07-21T20:41:45+00:00", true, JsonType.DATE, "Field3", "Field3");
        addField("Field4", "2014-07-21T20:41:45+00:00", true, JsonType.DATE_EPOCH, "Field4", "Field4");
        addField("Field5", "I should be ignored", false, JsonType.STRING, "Field5", "Field5");
        addField("Field6", "5552128379", true, JsonType.STRING, "Field6", "Field6");
        addField("Field7", "RES", true, JsonType.STRING, "Field7", "Field7");
        addField("Field_Underscore", "Field with an underscore", true, JsonType.STRING, "Field_Underscore", "FieldUnderscore");
        addField("field_underscore_lowercase", "Field with an underscore that is lowercase", true, JsonType.STRING, "field_underscore_lowercase", "FieldUnderscoreLowercase");
    }

    private void validateField(String fileFieldname, Object expected, boolean included, JsonType jsonType) {
        if (included) {
            assertTrue(fileFieldname + " should exist in the current row", callable.getData().containsKey(fileFieldname.toLowerCase()));

            FieldInstance instance = callable.getData().get(fileFieldname.toLowerCase());
            assertEquals(jsonType, instance.getField().getJsonType());
            assertEquals(expected, instance.getValue());
        } else {
            assertFalse(fileFieldname + " should NOT exist in the current row", callable.getData().containsKey(fileFieldname.toLowerCase()));
        }
    }
}
