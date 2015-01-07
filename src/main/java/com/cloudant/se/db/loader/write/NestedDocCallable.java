package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.exception.StructureException;
import com.fasterxml.jackson.core.JsonProcessingException;

public class NestedDocCallable extends BaseDocCallable {
	protected static final Logger	log	= Logger.getLogger(NestedDocArrayCallable.class);

	public NestedDocCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@Override
	public Integer handle() {
		//
		// This case is a little more complex -
		// - Try inserting into an empty top level document
		// - if it fails, get the parent from the database and add us into it
		//

		upsert(parentId, buildEmptyParent(toMap()));

		return 0;
	}

	@Override
	protected Map<String, Object> handleConflict() throws StructureException, JsonProcessingException, IOException {
		Map<String, Object> fromCloudant = getFromCloudant(parentId);
		fromCloudant.put(table.nestField, toMap());

		return fromCloudant;
	}
}
