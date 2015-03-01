package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ReferenceDocCallable extends BaseDocCallable {
    protected static final Logger log = Logger.getLogger(NestedDocArrayCallable.class);

    public ReferenceDocCallable(AppConfig config, DataTable table) {
        super(config, table);
    }

    @Override
    public WriteCode handle() throws Exception {
        //
        // This case is a little more complex -
        // - We need to insert the base object (upsert)
        // - We need to add a reference to the parent object (upsert)
        //

        upsert(id, toMap());
        return upsert(parentId, buildEmptyParent(REF_PREFIX + id));
    }

    @Override
    protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
        Map<String, Object> fromCloudant = get(parentId);
        fromCloudant.put(table.getJsonNestField(), REF_PREFIX + id);

        return fromCloudant;
    }
}
