package com.cloudant.se.db.loader.config;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Date;

import org.junit.Test;

import com.cloudant.se.db.loader.AppConstants.JsonType;
import com.cloudant.se.db.loader.AppConstants.TransformLanguage;
import com.cloudant.se.db.loader.write.FieldInstance;

public class DataTableFieldTest {
    @Test
    public void testApplyConversionsDate() {
        DataTable table = new DataTable();
        DataTableField field = getDataTableField(JsonType.DATE);

        Date date = null;
        FieldInstance instance = null;

        //
        // Date toString()
        date = new Date();
        instance = new FieldInstance(date.toString(), field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("Date conversion failed", instance.getValue() instanceof Date);
        assertTrue("Date conversion failed", (date.getTime() - ((Date) instance.getValue()).getTime()) < 1000);

        //
        // Date 2014-07-21T20:41:45+00:00
        date = new Date(1405975305000l);
        instance = new FieldInstance("2014-07-21T20:41:45+00:00", field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("Date conversion failed", instance.getValue() instanceof Date);
        assertTrue("Date conversion failed", (date.getTime() - ((Date) instance.getValue()).getTime()) < 1000);
    }

    @Test
    public void testApplyConversionsDateEpoch() {
        DataTable table = new DataTable();
        DataTableField field = getDataTableField(JsonType.DATE_EPOCH);

        Date date = null;
        FieldInstance instance = null;

        //
        // Date toString()
        date = new Date();
        instance = new FieldInstance(date.toString(), field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("DateEpoch conversion failed", instance.getValue() instanceof Long);
        assertTrue("DateEpoch conversion failed", (date.getTime() - (Long) instance.getValue()) < 1000);

        //
        // Date 2014-07-21T20:41:45+00:00
        date = new Date(1405975305000l);
        instance = new FieldInstance("2014-07-21T20:41:45+00:00", field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("DateEpoch conversion failed", instance.getValue() instanceof Long);
        assertEquals("DateEpoch conversion failed", 1405975305000l, instance.getValue());
    }

    @Test
    public void testApplyConversionsFormattedDateString() {
        DataTable table = new DataTable();
        DataTableField field = getDataTableField(JsonType.DATE_FORMATTED_STRING);

        FieldInstance instance = null;

        //
        // Date 2014-07-21T20:41:45+00:00 -- Timezone NOT in play
        instance = new FieldInstance("2014-07-21T20:41:45+00:00", field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("FormattedDateString conversion failed", instance.getValue() instanceof String);
        assertEquals("FormattedDateString conversion failed", "20140721204145", instance.getValue());

        //
        // Date 2014-07-21T20:41:45+01:00 -- Timezone IN play
        instance = new FieldInstance("2014-07-21T20:41:45+01:00", field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("FormattedDateString conversion failed", instance.getValue() instanceof String);
        assertEquals("FormattedDateString conversion failed", "20140721194145", instance.getValue());
    }

    @Test
    public void testApplyConversionsFormattedDateNumber() {
        DataTable table = new DataTable();
        DataTableField field = getDataTableField(JsonType.DATE_FORMATTED_NUMBER);

        FieldInstance instance = null;

        //
        // Date 2014-07-21T20:41:45+00:00 -- Timezone NOT in play
        instance = new FieldInstance("2014-07-21T20:41:45+00:00", field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("FormattedDateNumber conversion failed", instance.getValue() instanceof Long);
        assertEquals("FormattedDateNumber conversion failed", 20140721204145l, instance.getValue());

        //
        // Date 2014-07-21T20:41:45+01:00 -- Timezone IN play
        instance = new FieldInstance("2014-07-21T20:41:45+01:00", field, table);
        instance.getField().applyConversions("testid", instance);
        assertTrue("FormattedDateNumber conversion failed", instance.getValue() instanceof Long);
        assertEquals("FormattedDateNumber conversion failed", 20140721194145l, instance.getValue());
    }

    @Test
    public void testApplyConversionsNumber() {
        DataTable table = new DataTable();
        DataTableField field = getDataTableField(JsonType.NUMBER);

        FieldInstance instance = null;

        //
        // Integer
        instance = new FieldInstance("10", field, table);
        instance.getField().applyConversions("testid", instance);
        assertEquals("Integer conversion failed", 10, instance.getValue());

        //
        // Long
        instance = new FieldInstance("10000000000000", field, table);
        instance.getField().applyConversions("testid", instance);
        assertEquals("Long conversion failed", 10000000000000l, instance.getValue());

        //
        // Float
        instance = new FieldInstance("10.0", field, table);
        instance.getField().applyConversions("testid", instance);
        assertEquals("Float conversion failed", 10.0f, instance.getValue());

        //
        // String
        instance = new FieldInstance("ten", field, table);
        instance.getField().applyConversions("testid", instance);
        assertEquals("String conversion failed", "ten", instance.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateEmpty() {
        DataTableField f = new DataTableField();
        f.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateExcludeNoDb() {
        DataTableField f = new DataTableField();
        f.setInclude(false);
        f.setJsonFieldName("testing");
        f.validate();
    }

    @Test
    public void testValidateExcludeNoJson() {
        DataTableField f = new DataTableField();
        f.setInclude(false);
        f.setDbFieldName("testing");
        f.validate();

        assertEquals("Testing", f.getJsonFieldName());
    }

    @Test
    public void testValidateGood() {
        DataTableField f = new DataTableField();
        f.setDbFieldName("sTesting");
        f.setJsonFieldName("testing");
        f.validate();

        assertEquals("sTesting", f.getDbFieldName());
        assertEquals("testing", f.getJsonFieldName());
        assertNull(f.getTransformScript());
        assertEquals(TransformLanguage.GROOVY, f.getTransformScriptLanguage());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateIncludeNoDb() {
        DataTableField f = new DataTableField();
        f.setJsonFieldName("testing");
        f.validate();
    }

    @Test
    public void testValidateIncludeNoJson() {
        DataTableField f = new DataTableField();
        f.setDbFieldName("testing");
        f.validate();

        assertEquals("Testing", f.getJsonFieldName());
    }

    private DataTableField getDataTableField(JsonType jsonType) {
        DataTableField field = new DataTableField("testing");
        field.setJsonType(jsonType);

        switch (jsonType) {
            case DATE:
                break;
            case DATE_EPOCH:
                break;
            case DATE_FORMATTED_STRING:
                field.setJsonDateStringFormat("yyyyMMddHHmmss");
                field.setJsonDateStringTimezone("UTC");
                break;
            case DATE_FORMATTED_NUMBER:
                field.setJsonDateStringFormat("yyyyMMddHHmmss");
                field.setJsonDateStringTimezone("UTC");
                break;
            case NUMBER:
                break;
            case STRING:
            default:
                break;
        }

        return field;
    }
}
