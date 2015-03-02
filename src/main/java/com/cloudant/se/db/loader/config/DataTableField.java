package com.cloudant.se.db.loader.config;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.springframework.util.Assert.hasText;
import static org.springframework.util.Assert.isTrue;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.springframework.util.Assert;

import com.cloudant.se.db.loader.AppConstants.JsonType;
import com.cloudant.se.db.loader.AppConstants.TransformLanguage;
import com.cloudant.se.db.loader.write.FieldInstance;
import com.cloudant.se.util.UJson;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.reinert.jjschema.Attributes;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

@Attributes(title = "DataTableField", description = "A field within the source dataset")
public class DataTableField {
    @JsonIgnore
    protected static final Logger log                               = Logger.getLogger(DataTableField.class);

    @Attributes(required = true, description = "Name of the field within the source data")
    private String                dbFieldName                       = null;

    @Attributes(required = false, description = "Include in the resulting JSON?")
    private boolean               include                           = true;

    @Attributes(required = false, description = "The date format for date-->string conversions")
    private String                jsonDateStringFormat              = null;

    @Attributes(required = false, description = "The timezone for date-->string conversions")
    private String                jsonDateStringTimezone            = null;

    @Attributes(required = false, description = "Name of the field in the resuting JSON")
    private String                jsonFieldName                     = null;

    @Attributes(required = false, description = "The type of the field in the resulting JSON")
    private JsonType              jsonType                          = JsonType.STRING;

    @Attributes(required = false, description = "Transformation script")
    private String                transformScript                   = null;

    @Attributes(required = false, description = "Transformation scrirpt language")
    private TransformLanguage     transformScriptLanguage           = TransformLanguage.GROOVY;

    @Attributes(required = false, description = "Transformation script test input (used during validation)")
    private String                transformScriptTestInput          = null;

    @Attributes(required = false, description = "Transformation script test output (used during validation)")
    private String                transformScriptTestOutputExpected = null;

    public DataTableField() {
    }

    public DataTableField(String fieldName) {
        this(fieldName, UJson.toCamelCase(fieldName));
    }

    public DataTableField(String dbFieldName, String jsonFieldName) {
        this.dbFieldName = dbFieldName;
        this.jsonFieldName = jsonFieldName;
    }

    public void applyConversions(String documentId, FieldInstance instance) {
        if (instance.getValue() == null) {
            return;
        }

        Object newValue = null;
        switch (jsonType) {
            case DATE:
                newValue = convertToDate(instance.getValue());
                break;
            case DATE_EPOCH:
                newValue = convertToDateEpoch(instance.getValue());
                break;
            case DATE_FORMATTED_STRING:
                newValue = convertToDateString(instance.getValue());
                break;
            case DATE_FORMATTED_NUMBER:
                newValue = convertToDateNumber(instance.getValue());
                break;
            case NUMBER:
                newValue = convertToNumber(instance.getValue());
                break;
            case STRING:
            default:
                newValue = instance.getValue().toString();
        }

        log.trace("[id=" + documentId + "] - " + dbFieldName + " - Conversion - " + jsonType + " - " + instance.getValue() + " --> " + newValue);
        instance.setValue(newValue);
    }

    public void applyScripting(String id, FieldInstance instance) {
        if (StringUtils.isBlank(transformScript)) {
            return;
        }

        Object newValue = instance.getValue();
        log.trace("[id=" + id + "] - " + dbFieldName + " - Transformation script not blank");
        log.trace("[id=" + id + "] - " + dbFieldName + " - Transformation - type - " + transformScriptLanguage);
        log.trace("[id=" + id + "] - " + dbFieldName + " - Transformation - script - " + transformScript);

        try {
            newValue = runScript(instance.getValue());
        } catch (MissingPropertyException e) {
            log.warn("[id=" + id + "] - " + dbFieldName + " - Transformation error - script references an unknown property - " + e.getProperty());
        } catch (Exception e) {
            System.out.println(e.getClass());
            log.warn("[id=" + id + "] - " + dbFieldName + " - Transformation error - " + e.getMessage());
        }

        log.trace("[id=" + id + "] - " + dbFieldName + " - Transformation - output - " + instance.getValue() + " --> " + newValue);
        instance.setValue(newValue);
    }

    public String getDbFieldName() {
        return dbFieldName;
    }

    public String getJsonDateStringFormat() {
        return jsonDateStringFormat;
    }

    public String getJsonDateStringTimezone() {
        return jsonDateStringTimezone;
    }

    public String getJsonFieldName() {
        return jsonFieldName;
    }

    public JsonType getJsonType() {
        return jsonType;
    }

    public String getTransformScript() {
        return transformScript;
    }

    public TransformLanguage getTransformScriptLanguage() {
        return transformScriptLanguage;
    }

    public String getTransformScriptTestInput() {
        return transformScriptTestInput;
    }

    public String getTransformScriptTestOutputExpected() {
        return transformScriptTestOutputExpected;
    }

    public boolean isInclude() {
        return include;
    }

    public void setDbFieldName(String dbFieldName) {
        this.dbFieldName = dbFieldName;
    }

    public void setInclude(boolean include) {
        this.include = include;
    }

    public void setJsonDateStringFormat(String jsonDateStringFormat) {
        this.jsonDateStringFormat = jsonDateStringFormat;
    }

    public void setJsonDateStringTimezone(String jsonDateStringTimezone) {
        this.jsonDateStringTimezone = jsonDateStringTimezone;
    }

    public void setJsonFieldName(String jsonFieldName) {
        this.jsonFieldName = jsonFieldName;
    }

    public void setJsonType(JsonType jsonType) {
        this.jsonType = jsonType;
    }

    public void setTransformScript(String transformScript) {
        this.transformScript = transformScript;
    }

    public void setTransformScriptLanguage(TransformLanguage transformScriptLanguage) {
        this.transformScriptLanguage = transformScriptLanguage;
    }

    public void setTransformScriptTestInput(String transformScriptTestInput) {
        this.transformScriptTestInput = transformScriptTestInput;
    }

    public void setTransformScriptTestOutputExpected(String transformScriptTestOutputExpected) {
        this.transformScriptTestOutputExpected = transformScriptTestOutputExpected;
    }

    public void validate() {
        //
        // Make sure we know what type of field this is
        Assert.notNull(jsonType, "Must provide a type for this field");

        //
        // Make sure we know what field in the original source this came from
        hasText(dbFieldName);
        printSetting("dbFieldName", dbFieldName);

        //
        // Where its being written to
        if (StringUtils.isBlank(jsonFieldName)) {
            jsonFieldName = UJson.toCamelCase(dbFieldName);
        }
        printSetting("jsonFieldName", jsonFieldName);

        //
        // Make sure we have our formats for formatted strings
        if (jsonType == JsonType.DATE_FORMATTED_STRING) {
            hasText(jsonDateStringFormat, "Must provde a date-->string format");
            hasText(jsonDateStringTimezone, "Must provde a date-->string timezone");
            printSetting("jsonDateStringFormat", jsonDateStringFormat);
            printSetting("jsonDateStringTimezone", jsonDateStringTimezone);
        } else if (jsonType == JsonType.DATE_FORMATTED_NUMBER) {
            hasText(jsonDateStringFormat, "Must provde a date-->number format");
            hasText(jsonDateStringTimezone, "Must provde a date-->number timezone");
            printSetting("jsonDateStringFormat", jsonDateStringFormat);
            printSetting("jsonDateStringTimezone", jsonDateStringTimezone);
        }

        //
        // Do a test run of the transform script
        if (StringUtils.isNotBlank(transformScript)) {
            try {
                if (transformScriptLanguage == TransformLanguage.JAVASCRIPT) {
                    //
                    // Wrap the javascript in a function call so the return works as expected
                    transformScript = "function runcode(input) { " + transformScript + " }\n runcode(input);";
                }

                //
                // Do a test run of the script to make sure it works
                if (isNotBlank(transformScriptTestInput) && isNotBlank(transformScriptTestOutputExpected)) {
                    Object output = runScript(transformScriptTestInput);
                    isTrue(transformScriptTestOutputExpected.equals(output), "Transformation script did not provide expected test output - " + transformScriptTestOutputExpected + " - " + output);
                }
            } catch (MissingPropertyException e) {
                isTrue(false, "Script for " + dbFieldName + " must not reference invalid properties - " + e.getProperty());
            } catch (MultipleCompilationErrorsException e) {
                isTrue(false, "Script for " + dbFieldName + " must compile - " + e.getMessage());
                // } catch (EvaluatorException e) {
                // e.printStackTrace();
                // isTrue(false, "Script for " + dbFieldName + " must compile - " + e.getMessage());
            } catch (Exception e) {
                isTrue(false, "Script for " + dbFieldName + " must be exception free - " + e.getMessage());
            }

            printSetting("transformScriptLanguage", transformScriptLanguage);
            printSetting("transformScript", transformScript);
        }
    }

    private Object convertToDate(Object currentValue) {
        if (currentValue instanceof Date) {
            return currentValue;
        }

        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(currentValue.toString());
        if (groups.size() == 1) {
            List<Date> dates = groups.get(0).getDates();
            if (dates.size() == 1) {
                //
                // We were able to parse the date down to a single date and a single group, use its
                return dates.get(0);
            }
        }

        //
        // We were not able to parse the date with enough confidence, keep the original
        return currentValue;
    }

    private Object convertToDateEpoch(Object currentValue) {
        Object date = convertToDate(currentValue);
        if (date instanceof Date) {
            //
            // We were able to convert it to a date
            return ((Date) date).getTime();
        }

        //
        // We were not able to parse the date with enough confidence, keep the original
        return currentValue;
    }

    private Object convertToDateNumber(Object currentValue) {
        Object parsedDate = convertToDateString(currentValue);
        if (NumberUtils.isNumber(parsedDate.toString())) {
            //
            // Attempt conversion and fail silently
            try {
                return NumberUtils.createNumber(parsedDate.toString());
            } catch (NumberFormatException e) {
            }
        }

        //
        // We were not able to parse the date with enough confidence, keep the original
        return currentValue;
    }

    private Object convertToDateString(Object currentValue) {
        Date date = null;
        Object parsedDate = convertToDate(currentValue);
        if (parsedDate instanceof Date) {
            //
            // We were able to convert it to a date
            date = ((Date) parsedDate);
        }

        if (date != null) {
            //
            // Requested it as a string
            try {
                DateFormat dateFormat = new SimpleDateFormat(jsonDateStringFormat);
                dateFormat.setTimeZone(TimeZone.getTimeZone(jsonDateStringTimezone));
                return dateFormat.format(date);
            } catch (Exception e) {
                //
                // If we can't format the date for some reason, just give the toString() date
                log.warn("Date formatting failed, using toString()");
                return date.toString();
            }

        }

        //
        // We were not able to parse the date with enough confidence, keep the original
        return currentValue;
    }

    private Object convertToNumber(Object currentValue) {
        if (currentValue instanceof Number) {
            return currentValue;
        } else if (NumberUtils.isNumber(currentValue.toString())) {
            //
            // Attempt conversion and fail silently
            try {
                return NumberUtils.createNumber(currentValue.toString());
            } catch (NumberFormatException e) {
            }
        }

        //
        // Unable to convert to a number, return original
        return currentValue;
    }

    private void printSetting(String setting, Object value) {
        log.debug("        " + setting + " --> " + value);
    }

    private Object runScript(Object origValue) throws ScriptException {
        Object newValue = origValue;
        switch (transformScriptLanguage) {
            case GROOVY:
                Binding binding = new Binding();
                binding.setVariable("input", origValue);
                GroovyShell shell = new GroovyShell(binding);

                newValue = shell.evaluate(transformScript);
                break;
            case JAVASCRIPT:
                // //
                // // Wrap the javascript in a function call so the return works as expected
                // transformScript = "function runcode(input) { " + transformScript + " }\n runcode(input);";
                //
                // ScriptEngineManager factory = new ScriptEngineManager();
                // ScriptEngine engine = factory.getEngineByName("JavaScript");
                // engine.put("input", "bogus_value");
                //
                // engine.eval(transformScript);
                ScriptEngineManager factory = new ScriptEngineManager();
                ScriptEngine engine = factory.getEngineByName("JavaScript");
                engine.put("input", origValue);

                newValue = engine.eval(transformScript);
                break;
            default:
                break;
        }

        return newValue;
    }
}
