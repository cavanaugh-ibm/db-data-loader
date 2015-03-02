package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.writer.CloudantWriteResult;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ParentDocCallable extends BaseDocCallable {
    protected static final Logger log = Logger.getLogger(NestedDocArrayCallable.class);

    public ParentDocCallable(AppConfig config, DataTable table) {
        super(config, table);
    }

    @Override
    public CloudantWriteResult handle() throws Exception {
        //
        // This is the simplest case of all - we try to insert it, if it fails, we update from the database
        //

        return upsert(id, toMap());
    }

    @Override
    protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
        Map<String, Object> fromCloudant = get(id);
        fromCloudant.putAll(toMap());

        return fromCloudant;
    }
}
