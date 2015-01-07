package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.exception.StructureException;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ParentDocCallable extends BaseDocCallable {
	protected static final Logger	log	= Logger.getLogger(NestedDocArrayCallable.class);

	public ParentDocCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@Override
	public Integer handle() throws Exception {
		//
		// This is the simplest case of all - we try to insert it, if it fails, we update from the database
		//

		upsert(id, toMap());

		return 0;
	}

	@Override
	protected Map<String, Object> handleConflict() throws StructureException, JsonProcessingException, IOException {
		Map<String, Object> fromCloudant = getFromCloudant(id);
		fromCloudant.putAll(toMap());

		return fromCloudant;
	}
}
