package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.exception.StructureException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

public class NestedDocArrayCallable extends BaseDocCallable {
	protected static final Logger	log	= Logger.getLogger(NestedDocArrayCallable.class);

	public NestedDocArrayCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Integer handle() {
		//
		// This case is a little more complex -
		// - Try inserting into an empty top level document
		// - if it fails, get the parent from the database and add us into it with array logic
		//

		upsert(parentId, buildEmptyParent(Lists.newArrayList(toMap())));

		return 0;
	}

	@Override
	protected Map<String, Object> handleConflict() throws StructureException, JsonProcessingException, IOException {
		Map<String, Object> fromCloudant = getFromCloudant(parentId);
		addToArrayAt(fromCloudant, table.nestField, toMap());

		return fromCloudant;
	}
}
