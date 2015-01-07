package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.exception.StructureException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

public class ReferenceDocArrayCallable extends BaseDocCallable {
	protected static final Logger	log	= Logger.getLogger(NestedDocArrayCallable.class);

	public ReferenceDocArrayCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@Override
	public Integer handle() throws Exception {
		//
		// This case is a little more complex -
		// - We need to insert the base object (upsert)
		// - We need to add a reference to the parent object (upsert)
		//

		upsert(id, toMap());
		upsert(parentId, buildEmptyParent(Lists.newArrayList(REF_PREFIX + id)));

		return 0;
	}

	@Override
	protected Map<String, Object> handleConflict() throws StructureException, JsonProcessingException, IOException {
		Map<String, Object> fromCloudant = getFromCloudant(parentId);
		addToArrayAt(fromCloudant, table.nestField, REF_PREFIX + id);

		return fromCloudant;
	}
}
