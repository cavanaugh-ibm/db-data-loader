package com.cloudant.se.db.loader.write;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.lightcouch.DocumentConflictException;

import com.cloudant.se.db.loader.LockManager;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

public abstract class ABaseObjectCallable implements Callable<Integer> {
	protected static final Logger			log			= Logger.getLogger(ABaseObjectCallable.class);
	protected static final String			REF_PREFIX	= "@";

	protected AppConfig						config		= null;
	protected Map<String, FieldInstance>	data		= new LinkedHashMap<>();
	protected String						id			= null;

	protected Joiner						keyJoiner	= null;

	protected String						parentId	= null;
	protected DataTable						table		= null;

	public ABaseObjectCallable(AppConfig config, DataTable table) {
		this.config = config;
		this.table = table;

		this.keyJoiner = Joiner.on(config.concatinationChar).skipNulls();
	}

	public void addFields(Map<String, FieldInstance> currentRow) {
		data.putAll(currentRow);
	}

	@Override
	public final Integer call() throws Exception {
		data.put("DocumentType", new FieldInstance("DocumentType", table.jsonDocumentType, null));

		this.id = buildIdFrom(table.idFields);
		this.parentId = buildIdFrom(table.parentIdFields);

		int rc = 0;
		try {
			rc = handle();
		} catch (Exception e) {
			log.error("Error while saving record");
		}

		return rc;
	}

	private boolean insert(Map<String, Object> map) {
		try {
			log.info("Calling remote save " + map.get("_id"));
			config.database.save(map);
			return true;
		} catch (DocumentConflictException e) {
			return false;
		}
	}

	private boolean update(Map<String, Object> map) {
		try {
			log.info("Calling remote update " + map.get("_id"));
			config.database.update(map);
			return true;
		} catch (DocumentConflictException e) {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	protected void addToArrayAt(Map<String, Object> source, String field, Map<String, Object> newData) throws Exception {
		List<Map<String, Object>> items = null;

		if (source.containsKey(table.nestField)) {
			Object nestField = source.get(table.nestField);

			if (nestField instanceof List) {
				items = (List<Map<String, Object>>) source.get(table.nestField);
			} else {
				throw new Exception("Structure from the database is not what we expected");
			}
		} else {
			items = new ArrayList<>();
		}

		//
		// Make sure the array does not have it already
		for (Iterator<Map<String, Object>> iter = items.iterator(); iter.hasNext();) {
			Map<String, Object> item = iter.next();

			if (item.containsKey(table.idField)) {
				if (item.get(table.idField).equals(id)) {
					iter.remove();
				}
			}
		}

		items.add(newData);
		source.put(field, items);
	}

	@SuppressWarnings("unchecked")
	protected void addToArrayAt(Map<String, Object> source, String field, String newData) throws Exception {
		List<String> items = null;

		if (source.containsKey(table.nestField)) {
			Object nestField = source.get(table.nestField);

			if (nestField instanceof List) {
				items = (List<String>) source.get(table.nestField);
			} else {
				throw new Exception("Structure from the database is not what we expected");
			}
		} else {
			items = new ArrayList<>();
		}

		//
		// Make sure the array does not have it already
		for (Iterator<String> iter = items.iterator(); iter.hasNext();) {
			String item = iter.next();

			if (StringUtils.equals(item, newData)) {
				iter.remove();
			}
		}

		items.add(newData);
		source.put(field, items);
	}

	protected Map<String, Object> buildEmptyParent(Object nestedObject) {
		Map<String, Object> newMap = Maps.newHashMap();
		newMap.put("_id", parentId);
		newMap.put(table.nestField, nestedObject);

		return newMap;
	}

	protected String buildIdFrom(Set<String> fields) {
		Set<Object> idValues = new LinkedHashSet<>();
		boolean deleteFromCurrentRow = false;

		if (table.idFields.size() == 1) {
			deleteFromCurrentRow = true;
		}

		for (String fieldName : fields) {
			if (data.containsKey(fieldName.toLowerCase())) {
				idValues.add(data.get(fieldName.toLowerCase()).value);
			}

			if (deleteFromCurrentRow) {
				data.remove(fieldName.toLowerCase());
			}
		}

		return keyJoiner.join(idValues);
	}

	protected Map<String, Object> getFromCloudant(String id) throws JsonProcessingException, IOException {
		InputStream is = config.database.find(id);
		return new ObjectMapper().reader(Map.class).readValue(is);
	}

	protected abstract Integer handle() throws Exception;

	protected abstract Map<String, Object> handleConflict() throws Exception;

	protected Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		for (FieldInstance f : data.values()) {
			if (f.field != null) {
				Object value = f.value;

				//
				// Logic to attempt number vs. string
				if (f.field.isNumericHint && NumberUtils.isNumber(value.toString())) {
					try {
						value = NumberUtils.createNumber(value.toString());
					} catch (NumberFormatException e) {
					}
				}
				map.put(f.field.jsonFieldName, f.field.isReference ? REF_PREFIX + value : value);
			} else {
				map.put(f.name, f.value);
			}
		}

		map.put(table.idField, id);

		return map;
	}

	protected boolean upsert(String id, Map<String, Object> map) throws Exception {
		LockManager.acquire(id);

		try {
			if (insert(map)) {
				//
				// Insert worked, nothing else to do in this scenario
				return true;
			} else {
				//
				// Conflict, get the old version, merge in our changes (adding)
				// TODO add this to the configuration (max iterations)
				int i = 0;
				while (i < config.maxRetries) {
					i++;
					Map<String, Object> merged = handleConflict();

					if (update(merged)) {
						return true;
					} else {
						continue;
					}
				}
			}
		} finally {
			LockManager.release(id);
		}

		return false;
	}
}
