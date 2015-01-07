package com.cloudant.se.db.loader.write;

import java.util.Map;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.google.common.collect.Lists;

public class ReferenceObjectArrayCallable extends ABaseObjectCallable {
	public ReferenceObjectArrayCallable(AppConfig config, DataTable table) {
		super(config, table);
	}

	@Override
	public Integer handle() throws Exception {
		try {

			//
			// This case is a little more complex -
			// - We need to insert the base object (upsert)
			// - We need to add a reference to the parent object (upsert)
			//

			if (!upsert(id, toMap())) {
				throw new Exception("Unable to upsert a document after seceral attempts.  I should handle this error better - [parentId=" + parentId + "][id=" + id + "]");
			}

			if (!upsert(parentId, buildEmptyParent(Lists.newArrayList(REF_PREFIX + id)))) {
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
		addToArrayAt(fromCloudant, table.nestField, REF_PREFIX + id);

		return fromCloudant;
	}
}
