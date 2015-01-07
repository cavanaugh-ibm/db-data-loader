package com.cloudant.se.db.loader.write;

import java.util.Map;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.google.common.collect.Lists;

public class NestedObjectArrayCallable extends ABaseObjectCallable {
	public NestedObjectArrayCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Integer handle() throws Exception {
		try {

			//
			// This case is a little more complex -
			// - Try inserting into an empty top level document
			// - if it fails, get the parent from the database and add us into it
			//

			if (!upsert(parentId, buildEmptyParent(Lists.newArrayList(toMap())))) {
				throw new Exception("Unable to upsert a document after seceral attempts.  I should handle this error better - [parentId=" + parentId + "][id=" + id + "]");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	protected Map<String, Object> handleConflict() throws Exception {
		Map<String, Object> fromCloudant = getFromCloudant(parentId);
		addToArrayAt(fromCloudant, table.nestField, toMap());

		return fromCloudant;
	}
}
