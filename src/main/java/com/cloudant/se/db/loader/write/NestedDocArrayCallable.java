package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

public class NestedDocArrayCallable extends BaseDocCallable {
    protected static final Logger log = Logger.getLogger(NestedDocArrayCallable.class);

    public NestedDocArrayCallable(AppConfig config, DataTable table) {
        super(config, table);
    }

    @SuppressWarnings("unchecked")
    @Override
    public WriteCode handle() {
        //
        // This case is a little more complex -
        // - Try inserting into an empty top level document
        // - if it fails, get the parent from the database and add us into it with array logic
        //

        return upsert(parentId, buildEmptyParent(Lists.newArrayList(toMap())));
    }

    @Override
    protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
        Map<String, Object> fromCloudant = get(parentId);
        addObjectToArray(fromCloudant, toMap());

        return fromCloudant;
    }
}
