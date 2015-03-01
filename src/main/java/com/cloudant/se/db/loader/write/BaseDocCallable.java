package com.cloudant.se.db.loader.write;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.lightcouch.internal.CouchDbUtil;

import com.cloudant.se.Constants;
import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.config.DataTableField;
import com.cloudant.se.db.writer.CloudantWriter;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.emory.mathcs.backport.java.util.Collections;

public abstract class BaseDocCallable extends CloudantWriter {
    protected static final String         DOC_TYPE       = "DocumentType";
    protected static final DataTableField DOC_TYPE_FIELD = new DataTableField(DOC_TYPE);
    protected static final Logger         log            = Logger.getLogger(BaseDocCallable.class);
    protected static final String         REF_PREFIX     = "@";

    protected AppConfig                   config         = null;

    protected Map<String, FieldInstance>  data           = Maps.newLinkedHashMap();
    protected String                      id             = null;
    protected Joiner                      keyJoiner      = null;
    protected String                      parentId       = null;
    protected DataTable                   table          = null;

    public BaseDocCallable(AppConfig config, DataTable table) {
        super(config.database);

        this.config = config;
        this.table = table;

        this.keyJoiner = Joiner.on(config.getConcatinationChar()).skipNulls();
    }

    public void addFields(Map<String, FieldInstance> currentRow) {
        data.putAll(currentRow);
    }

    @Override
    public final WriteCode call() throws Exception {
        log.debug(" *** call starting *** ");

        WriteCode wc = null;
        try {
            //
            // Add in a document type discriminator
            data.put(DOC_TYPE, new FieldInstance(table.getJsonDocumentType(), DOC_TYPE_FIELD, table));

            //
            // Figure out what our IDs are {id/parentid}
            buildAndSetId();
            buildAndSetParentId();

            //
            // Process the individual fields (numbers, dates, scripts)
            processFields();

            //
            // Give to the implementer to handle
            wc = handle();
        } catch (NullPointerException e) {
            e.printStackTrace();
            wc = WriteCode.EXCEPTION;
        } catch (Exception e) {
            log.error("Error while saving record");
            wc = WriteCode.EXCEPTION;
        }

        switch (wc) {
            case UPDATE:
            case INSERT:
                log.debug(" *** call finished with code \"" + wc + "\"*** ");
                break;
            default:
                log.error(" *** call finished with code \"" + wc + "\"*** ");
        }
        return wc;
    }

    @SuppressWarnings("unchecked")
    public Map<String, FieldInstance> getData() {
        return Collections.unmodifiableMap(data);
    }

    protected void addObjectToArray(Map<String, Object> source, Map<String, Object> newData) throws StructureException {
        //
        // These two methods exist like this to facilitate testing
        addObjectToArray(source, table.getJsonNestField(), table.getJsonUniqueIdField(), id, newData);
    }

    @SuppressWarnings("unchecked")
    protected void addObjectToArray(Map<String, Object> source, String nestField, String uniqueIdField, String newId, Map<String, Object> newData) throws StructureException {
        List<Map<String, Object>> items = null;

        if (source.containsKey(nestField)) {
            Object nestFieldObject = source.get(nestField);

            if (nestFieldObject instanceof List) {
                items = (List<Map<String, Object>>) source.get(nestField);
            } else {
                throw new StructureException("Structure from the database is not what we expected");
            }
        } else {
            items = Lists.newArrayList();
        }

        //
        // Make sure the array does not have it already
        for (Iterator<Map<String, Object>> iter = items.iterator(); iter.hasNext();) {
            Map<String, Object> item = iter.next();

            if (item.containsKey(uniqueIdField)) {
                if (item.get(uniqueIdField).equals(newId)) {
                    iter.remove();
                }
            }
        }

        items.add(newData);
        source.put(nestField, items);
    }

    @SuppressWarnings("unchecked")
    protected void addStringToArray(Map<String, Object> source, String field, String newData) throws StructureException {
        List<String> items = null;

        if (source.containsKey(table.getJsonNestField())) {
            Object nestField = source.get(table.getJsonNestField());

            if (nestField instanceof List) {
                items = (List<String>) source.get(table.getJsonNestField());
            } else {
                throw new StructureException("Structure from the database is not what we expected");
            }
        } else {
            items = Lists.newArrayList();
        }

        //
        // Make sure the array does not have it already
        for (Iterator<String> iter = items.iterator(); iter.hasNext();) {
            String item = iter.next();

            if (StringUtils.equals(item, newData)) {
                iter.remove();
            }
        }

        items.add(newData);
        source.put(field, items);
    }

    protected void buildAndSetId() {
        this.id = buildIdFrom(table.getDbIdFields());
    }

    protected void buildAndSetParentId() {
        this.parentId = buildIdFrom(table.getDbParentIdFields());
    }

    protected Map<String, Object> buildEmptyParent(Object nestedObject) {
        Map<String, Object> newMap = Maps.newHashMap();
        newMap.put("_id", parentId);
        newMap.put(table.getJsonNestField(), nestedObject);

        return newMap;
    }

    protected String buildIdFrom(Set<String> fields) {
        Set<Object> idValues = Sets.newLinkedHashSet();
        boolean deleteFromCurrentRow = false;

        if (table.getDbIdFields().size() == 1) {
            deleteFromCurrentRow = true;
        }

        for (String fieldName : fields) {
            if (data.containsKey(fieldName.toLowerCase())) {
                idValues.add(data.get(fieldName.toLowerCase()).getValue());
            } else if (StringUtils.equalsIgnoreCase(fieldName, Constants.GENERATED)) {
                idValues.add(CouchDbUtil.generateUUID());
            }

            if (deleteFromCurrentRow) {
                data.remove(fieldName.toLowerCase());
            }
        }

        return keyJoiner.join(idValues);
    }

    protected abstract WriteCode handle() throws Exception;

    protected void processFields() {
        for (FieldInstance f : data.values()) {
            f.getField().applyScripting(id, f);
            f.getField().applyConversions(id, f);

            //
            // Check for empty fields and null them out - JSON will drop them for us
            if (!f.getTable().isJsonIncludeEmpty()) {
                if (f.getValue() != null && StringUtils.isBlank(f.getValue().toString())) {
                    f.setValue(null);
                }
            }
        }
    }

    protected Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newLinkedHashMap();
        for (FieldInstance f : data.values()) {
            map.put(f.getField().getDbFieldName(), f.getValue());
        }

        map.put(table.getJsonUniqueIdField(), id);

        return map;
    }
}
