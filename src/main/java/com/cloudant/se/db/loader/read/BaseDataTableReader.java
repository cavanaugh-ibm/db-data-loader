package com.cloudant.se.db.loader.read;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.AppConstants.JsonType;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.config.DataTableField;
import com.cloudant.se.db.loader.write.BaseDocCallable;
import com.cloudant.se.db.loader.write.FieldInstance;
import com.cloudant.se.db.loader.write.NestedDocArrayCallable;
import com.cloudant.se.db.loader.write.NestedDocCallable;
import com.cloudant.se.db.loader.write.ParentDocCallable;
import com.cloudant.se.db.loader.write.ReferenceDocArrayCallable;
import com.cloudant.se.db.loader.write.ReferenceDocCallable;
import com.cloudant.se.util.UJson;
import com.google.common.collect.Maps;

import edu.emory.mathcs.backport.java.util.Collections;

/**
 * This class is NOT thread safe
 *
 * @author Cloudant
 */
public abstract class BaseDataTableReader implements Callable<Integer> {
    protected static final Logger      log            = Logger.getLogger(BaseDataTableReader.class);
    private Map<String, FieldInstance> currentRow     = Maps.newTreeMap();
    protected AppConfig                config         = null;

    protected ExecutorService          executor       = null;
    protected BaseDocCallable          outputCallable = null;
    protected int                      processed      = 0;
    protected DataTable                table          = null;

    public BaseDataTableReader(AppConfig config, DataTable table, ExecutorService executor) {
        this.config = config;
        this.table = table;
        this.executor = executor;

    }

    public void addField(String fieldName, String fieldValue) {
        fieldName = fieldName.trim();
        fieldValue = fieldValue.trim();

        log.trace(fieldName + " --> " + fieldValue);

        boolean foundFromUser = false;
        for (DataTableField field : table.getDataFields()) {
            if (StringUtils.equalsIgnoreCase(field.getDbFieldName(), fieldName)) {
                //
                // This is a defined field, handle it the way the user asked us to
                foundFromUser = true;
                log.trace(fieldName + " - found in configuration");

                if (field.isInclude()) {
                    log.trace(fieldName + " - include = true");
                    currentRow.put(fieldName.toLowerCase(), new FieldInstance(fieldValue, field, table));
                } else {
                    log.trace(fieldName + " - include = false");
                }
            }
        }

        if (!foundFromUser) {
            log.trace(fieldName + " - NOT found in configuration");

            //
            // Extra field, nothing to do to it, just keep it
            DataTableField field = new DataTableField(fieldName, UJson.toCamelCase(fieldName));

            if (table.isCastNumerics()) {
                field.setJsonType(JsonType.NUMBER);
            }

            log.trace(fieldName + " - setting jsonFieldName to " + field.getJsonFieldName());
            currentRow.put(fieldName.toLowerCase(), new FieldInstance(fieldValue, field, table));
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, FieldInstance> getCurrentRow() {
        return Collections.unmodifiableMap(currentRow);
    }

    protected void recordComplete() throws InterruptedException {
        processed++;

        //
        // We have all the data from the source in our internal state, go ahead with processing
        BaseDocCallable callable = null;
        switch (table.getJsonNestType()) {
            case ARRAY:
                callable = new NestedDocArrayCallable(config, table);
                break;
            case OBJECT:
                callable = new NestedDocCallable(config, table);
                break;
            case PARENT:
                callable = new ParentDocCallable(config, table);
                break;
            case REFERENCE:
                callable = new ReferenceDocCallable(config, table);
                break;
            case REFERENCE_ARRAY:
                callable = new ReferenceDocArrayCallable(config, table);
                break;
            default:
                break;
        }

        //
        // Add all our data into the callable
        callable.addFields(currentRow);
        executor.submit(callable);

        //
        // Reset our state
        currentRow.clear();
    }
}