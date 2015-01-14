package com.cloudant.se.db.loader.write;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.lightcouch.CouchDbException;
import org.lightcouch.DocumentConflictException;

import com.cloudant.se.db.loader.AppConstants.WriteCode;
import com.cloudant.se.db.loader.LockManager;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.config.DataTableField;
import com.cloudant.se.db.loader.exception.StructureException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

public abstract class BaseDocCallable implements Callable<Integer> {
	protected static final Logger			log			= Logger.getLogger(BaseDocCallable.class);
	protected static final String			REF_PREFIX	= "@";
	protected static final String			DOC_TYPE	= "DocumentType";

	protected AppConfig						config		= null;

	protected Map<String, FieldInstance>	data		= new LinkedHashMap<>();
	protected Gson							gson		= null;
	protected String						id			= null;
	protected Joiner						keyJoiner	= null;
	protected String						parentId	= null;
	protected DataTable						table		= null;

	public BaseDocCallable(AppConfig config, DataTable table) {
		this.config = config;
		this.table = table;

		this.gson = new Gson();
		this.keyJoiner = Joiner.on(config.concatinationChar).skipNulls();
	}

	public void addFields(Map<String, FieldInstance> currentRow) {
		data.putAll(currentRow);
	}

	@Override
	public final Integer call() throws Exception {
		log.debug(" *** call starting *** ");

		int rc = 0;
		try {
			addDocumentType();

			this.id = buildIdFrom(table.idFields);
			this.parentId = buildIdFrom(table.parentIdFields);

			//
			// Process the individual fields (numbers, dates, scripts)
			processFields();

			//
			// Give to the implementer to handle
			rc = handle();
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (Exception e) {
			log.error("Error while saving record");
		}

		log.debug(" *** call finished *** ");
		return rc;
	}

	private void addDocumentType() {
		DataTableField docTypeField = new DataTableField();
		docTypeField.dbFieldName = DOC_TYPE;
		docTypeField.jsonFieldName = DOC_TYPE;

		data.put(DOC_TYPE, new FieldInstance(DOC_TYPE, table.jsonDocumentType, docTypeField, table));
	}

	private void checkForDateProcessing(FieldInstance f) {
		if (f.field.isDate) {
			//
			// Logic to attempt date conversion (explicitly told to try date processing for this field)
			Object newValue = convertDate(f.field.outputNumber, f.field.outputString, f.field.outputDateStringFormat, f.field.outputDateStringTimezone, f.value.toString());
			log.trace("[id=" + id + "] - " + f.field.dbFieldName + " - Casting - date - " + f.value + " --> " + newValue);
			f.value = newValue;
		} else if (!f.field.isNotDate
				&& (config.autoCastDatesToNumbers || config.autoCastDatesToStrings)
				&& (f.field.dbFieldName.toLowerCase().endsWith("timestamp") || f.field.dbFieldName.toLowerCase().endsWith("date"))) {
			//
			// Logic to attempt date conversion based on naming
			Object newValue = convertDate(config.autoCastDatesToNumbers, config.autoCastDatesToStrings, config.autoCastDatesFormat, config.autoCastDatesTimezone, f.value.toString());
			log.trace("[id=" + id + "] - " + f.field.dbFieldName + " - Casting - date - " + f.value + " --> " + newValue);
			f.value = newValue;
		}
	}

	private void checkForNumberProcessing(FieldInstance f) {
		if (f.field.isNumericHint) {
			//
			// Logic to attempt number vs. string
			if (NumberUtils.isNumber(f.value.toString())) {
				try {
					f.value = NumberUtils.createNumber(f.value.toString());
				} catch (NumberFormatException e) {
				}
			}
		}
	}

	private void checkForScriptProcessing(FieldInstance f) {
		if (StringUtils.isNotBlank(f.field.transformScript)) {
			log.trace("[id=" + id + "] - " + f.name + " - Transformation script not blank");
			try {
				Object output = null;
				log.trace("[id=" + id + "] - " + f.name + " - Transformation - type - " + f.field.transformScriptLanguage);
				log.trace("[id=" + id + "] - " + f.name + " - Transformation - script - " + f.field.transformScript);

				switch (f.field.transformScriptLanguage) {
					case GROOVY:
						Binding binding = new Binding();
						binding.setVariable("input", f.value);
						GroovyShell shell = new GroovyShell(binding);

						output = shell.evaluate(f.field.transformScript);
						log.trace("[id=" + id + "] - " + f.name + " - Transformation - output - " + f.value + " --> " + output);
						f.value = output == null ? null : output.toString();
						break;
					case JAVASCRIPT:
						ScriptEngineManager factory = new ScriptEngineManager();
						ScriptEngine engine = factory.getEngineByName("JavaScript");
						engine.put("input", f.value);

						output = engine.eval(f.field.transformScript);
						log.trace("[id=" + id + "] - " + f.name + " - Transformation - output - " + f.value + " --> " + output);
						f.value = output == null ? null : output.toString();
						break;
					default:
						break;
				}
			} catch (MissingPropertyException e) {
				log.warn(f.name + " - Transformation error - script references an unknown property - " + e.getProperty());
			} catch (Exception e) {
				System.out.println(e.getClass());
				log.warn(f.name + " - Transformation error - " + e.getMessage());
			}
		}
	}

	private Object convertDate(boolean outputNumber, boolean outputDate, String outDateFormat, String outDateTimezone, String value) {
		Parser parser = new Parser();
		List<DateGroup> groups = parser.parse(value);
		if (groups.size() == 1) {
			List<Date> dates = groups.get(0).getDates();
			if (dates.size() == 1) {
				//
				// We were able to parse the date down to a single date and a single group, use its
				if (outputNumber) {
					//
					// Requested it as a number
					return dates.get(0).getTime();
				} else if (outputDate) {
					//
					// Requested it as a string
					DateFormat dateFormat = new SimpleDateFormat(outDateFormat);
					dateFormat.setTimeZone(TimeZone.getTimeZone(outDateTimezone));
					return dateFormat.format(dates.get(0));
				}
			}
		}

		//
		// We were not able to pare the date with enough confidence, keep the original
		return value;
	}

	private boolean insert(Map<String, Object> map) {
		Object id = map.get("_id");
		try {
			log.debug("[id=" + id + "] - save - remote call");
			config.database.save(map);
			return true;
		} catch (DocumentConflictException e) {
			log.debug("[id=" + id + "] - insert - DocumentConflictException - returning false");
			return false;
		} catch (CouchDbException e) {
			if (e.getCause() != null) {
				log.debug("[id=" + id + "] - insert - CouchDbException - " + e.getCause().getMessage());
				if (StringUtils.contains(e.getCause().getMessage(), "Connection timed out: connect")) {
					log.debug("[id=" + id + "] - insert - CouchDbException - timeout - returning false");
					return false;
				}
			}

			if (e.getMessage().contains("\"error\":\"forbidden\",\"reason\":\"_writer access is required for this request\"")) {
				log.fatal("Cloudant account does not have writer permissions - exiting");
				System.exit(-1);
			}

			throw e;
		}
	}

	private void processFields() {
		for (FieldInstance f : data.values()) {
			checkForNumberProcessing(f);
			checkForDateProcessing(f);
			checkForScriptProcessing(f);

			//
			// Check for empty fields
			if (!f.table.includeEmpty) {
				if (f.value != null && StringUtils.isBlank(f.value.toString())) {
					f.value = null;
				}
			}
		}
	}

	private boolean update(Map<String, Object> map) {
		Object id = map.get("_id");
		try {
			log.debug("[id=" + id + "] - update - remote call");
			config.database.update(map);
			return true;
		} catch (DocumentConflictException e) {
			log.debug("[id=" + id + "] - update - DocumentConflictException - returning false");
			return false;
		} catch (CouchDbException e) {
			if (e.getCause() != null) {
				log.debug("[id=" + id + "] - update - CouchDbException - " + e.getCause().getMessage());
				if (StringUtils.contains(e.getCause().getMessage(), "Connection timed out: connect")) {
					log.debug("[id=" + id + "] - update - CouchDbException - timeout - returning false");
					return false;
				}
			}

			throw e;
		}
	}

	@SuppressWarnings("unchecked")
	protected void addToArrayAt(Map<String, Object> source, String field, Map<String, Object> newData) throws StructureException {
		List<Map<String, Object>> items = null;

		if (source.containsKey(table.nestField)) {
			Object nestField = source.get(table.nestField);

			if (nestField instanceof List) {
				items = (List<Map<String, Object>>) source.get(table.nestField);
			} else {
				throw new StructureException("Structure from the database is not what we expected");
			}
		} else {
			items = new ArrayList<>();
		}

		//
		// Make sure the array does not have it already
		for (Iterator<Map<String, Object>> iter = items.iterator(); iter.hasNext();) {
			Map<String, Object> item = iter.next();

			if (item.containsKey(table.uniqueIdField)) {
				if (item.get(table.uniqueIdField).equals(id)) {
					iter.remove();
				}
			}
		}

		items.add(newData);
		source.put(field, items);
	}

	@SuppressWarnings("unchecked")
	protected void addToArrayAt(Map<String, Object> source, String field, String newData) throws StructureException {
		List<String> items = null;

		if (source.containsKey(table.nestField)) {
			Object nestField = source.get(table.nestField);

			if (nestField instanceof List) {
				items = (List<String>) source.get(table.nestField);
			} else {
				throw new StructureException("Structure from the database is not what we expected");
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
		log.debug("[id=" + id + "] - read - call");
		InputStream is = config.database.find(id);
		Map<String, Object> map = new ObjectMapper().reader(Map.class).readValue(is);
		log.debug("[id=" + id + "] - read - success");

		return map;
	}

	protected abstract Integer handle() throws Exception;

	protected abstract Map<String, Object> handleConflict() throws StructureException, JsonProcessingException, IOException;

	protected Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		for (FieldInstance f : data.values()) {
			map.put(f.name, f.value);
		}

		map.put(table.uniqueIdField, id);

		return map;
	}

	protected WriteCode upsert(String id, Map<String, Object> map) {
		try {
			LockManager.acquire(id);

			Map<String, Object> toUpsert = map;
			try {
				if (insert(toUpsert)) {
					//
					// Insert worked, nothing else to do in this scenario
					log.debug("[id=" + id + "] - insert - succeeded");
					return WriteCode.INSERT;
				} else {
					//
					// Conflict, get the old version, merge in our changes (adding)
					int i = 0;
					while (i < config.maxRetries) {
						i++;
						toUpsert = handleConflict();

						if (update(toUpsert)) {
							log.debug("[id=" + id + "] - update - succeeded");
							return WriteCode.UPDATE;
						} else {
							continue;
						}
					}

					//
					// If we get to here it means we passed the max attempts - log that we did not write the message
					log.warn("[id=" + id + "] - Unable to upsert a document after " + config.maxRetries + " attempts - [" + gson.toJson(toUpsert) + "]");
					return WriteCode.MAX_ATTEMPTS;
				}
			} catch (Exception e) {
				log.warn("[id=" + id + "] - Unable to upsert a document due to exception - [" + gson.toJson(toUpsert) + "]", e);
				return WriteCode.EXCEPTION;
			} finally {
				LockManager.release(id);
			}
		} catch (InterruptedException e) {
			log.warn("[id=" + id + "] - Unable to upsert a document due to lock exception - [" + gson.toJson(map) + "]");
			return WriteCode.EXCEPTION;
		}
	}
}
