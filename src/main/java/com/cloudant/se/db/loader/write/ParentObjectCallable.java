package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.util.Map;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ParentObjectCallable extends ABaseObjectCallable {
	public ParentObjectCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@Override
	public Integer handle() throws Exception {
		try {

			//
			// This is the simplest case of all - we try to insert it, if it fails, we update from the database
			//

			if (!upsert(id, toMap())) {
				throw new Exception("Unable to upsert a document after seceral attempts.  I should handle this error better - id = " + id);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	protected Map<String, Object> handleConflict() throws JsonProcessingException, IOException {
		Map<String, Object> fromCloudant = getFromCloudant(id);
		fromCloudant.putAll(toMap());

		return fromCloudant;
	}
}
