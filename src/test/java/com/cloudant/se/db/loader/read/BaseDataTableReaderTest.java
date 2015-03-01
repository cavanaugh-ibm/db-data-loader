package com.cloudant.se.db.loader.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import com.cloudant.se.db.loader.AppConstants.JsonType;
import com.cloudant.se.db.loader.AppOptions;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.write.FieldInstance;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseDataTableReaderTest {
    private BaseDataTableReader reader = null;
    private AppConfig           config = null;

    @Before
    public void before() throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        File configFile = new File("src/test/resources/generic.config.json");
        config = mapper.readValue(configFile, AppConfig.class);
        config.setDefaultDirectory(configFile.getParentFile() != null ? configFile.getParentFile() : new File("."));
        config.mergeOptions(new AppOptions());
        config.validate();

        reader = new BaseDataTableReader(config, config.getTables().iterator().next(), null) {
            @Override
            public Integer call() throws Exception {
                return 1;
            }
        };
    }

    @Test
    public void testAddField() {
        addField("Field1", "10", true, JsonType.NUMBER, "Field1", "Field1");
        addField("Field2", new Date().toString(), true, JsonType.DATE_FORMATTED_STRING, "Field2", "Field2");
        addField("Field3", new Date().toString(), true, JsonType.DATE, "Field3", "Field3");
        addField("Field4", new Date().toString(), true, JsonType.DATE_EPOCH, "Field4", "Field4");
        addField("Field5", "I should be ignored", false, JsonType.STRING, "Field5", "Field5");
        addField("Field6", "5552128379", true, JsonType.STRING, "Field6", "Field6");
        addField("Field7", "RES", true, JsonType.STRING, "Field7", "Field7");
        addField("Field_Underscore", "Field with an underscore", true, JsonType.STRING, "Field_Underscore", "FieldUnderscore");
        addField("field_underscore_lowercase", "Field with an underscore that is lowercase", true, JsonType.STRING, "field_underscore_lowercase", "FieldUnderscoreLowercase");
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
}
