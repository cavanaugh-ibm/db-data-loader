package com.cloudant.se.db.loader.write;

import java.util.Map;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;

public class ReferenceObjectCallable extends ABaseObjectCallable {
	public ReferenceObjectCallable(AppConfig config, DataTable table) {
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

			Map<String, Object> parent = buildEmptyParent(REF_PREFIX + id);
			Map<String, Object> map = toMap();

			if (!upsert(id, map)) {
				throw new Exception("Unable to upsert a document after seceral attempts.  I should handle this error better - [parentId=" + parentId + "][id=" + id + "]");
			}

			if (!upsert(parentId, parent)) {
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
		fromCloudant.put(table.nestField, REF_PREFIX + id);

		return fromCloudant;
	}
}
