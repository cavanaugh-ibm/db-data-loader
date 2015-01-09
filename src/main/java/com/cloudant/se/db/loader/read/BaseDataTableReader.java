package com.cloudant.se.db.loader.read;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.config.DataTableField;
import com.cloudant.se.db.loader.write.BaseDocCallable;
import com.cloudant.se.db.loader.write.FieldInstance;
import com.cloudant.se.db.loader.write.NestedDocArrayCallable;
import com.cloudant.se.db.loader.write.NestedDocCallable;
import com.cloudant.se.db.loader.write.ParentDocCallable;
import com.cloudant.se.db.loader.write.ReferenceDocArrayCallable;
import com.cloudant.se.db.loader.write.ReferenceDocCallable;

/**
 * This class is NOT thread safe
 *
 * @author Cloudant
 */
public abstract class BaseDataTableReader implements Callable<Integer> {
	protected static final Logger		log				= Logger.getLogger(BaseDataTableReader.class);
	private Map<String, FieldInstance>	currentRow		= new TreeMap<>();
	protected AppConfig					config			= null;

	protected ExecutorService			executor		= null;
	protected BaseDocCallable			outputCallable	= null;
	protected int						processed		= 0;
	protected DataTable					table			= null;

	public BaseDataTableReader(AppConfig config, DataTable table, ExecutorService executor) {
		this.config = config;
		this.table = table;
		this.executor = executor;

	}

	protected void addField(String fieldName, String fieldValue) {
		fieldName = fieldName.trim();
		fieldValue = fieldValue.trim();

		boolean foundFromUser = false;
		for (DataTableField field : table.dataFields) {
			if (StringUtils.equalsIgnoreCase(field.dbFieldName, fieldName)) {
				//
				// This is a defined field, handle it the way the user asked us to
				foundFromUser = true;

				if (field.include) {
					if (StringUtils.isNotBlank(field.transformScript)) {
						try {
							switch (field.transformScriptLanguage) {
								case GROOVY:
									Binding binding = new Binding();
									binding.setVariable("input", fieldValue);
									GroovyShell shell = new GroovyShell(binding);

									Object output = shell.evaluate(field.transformScript);
									fieldValue = output == null ? null : output.toString();
									break;
								case JAVASCRIPT:
									break;
								default:
									break;
							}
						} catch (MissingPropertyException e) {
							log.warn(field.dbFieldName + " - Transformation error - script references an unknown property - " + e.getProperty());
						} catch (Exception e) {
							System.out.println(e.getClass());
							log.warn(field.dbFieldName + " - Transformation error - " + e.getMessage());
						}
					}

					//
					// Add it into our row map
					if (StringUtils.isNotBlank(fieldValue)) {
						// Not blank
						currentRow.put(fieldName.toLowerCase(), new FieldInstance(fieldName, fieldValue, field));
					} else {
						// Blank, should we store it?
						if (table.includeEmpty) {
							currentRow.put(fieldName.toLowerCase(), new FieldInstance(fieldName, fieldValue, field));
						}
					}
				} else {
					//
					// Do nothing
				}
			}
		}

		if (!foundFromUser) {
			//
			// Extra field, nothing to do to it, just keep it
			DataTableField field = new DataTableField();
			field.dbFieldName = fieldName;
			field.jsonFieldName = WordUtils.capitalize(fieldName, new char[] { '_' }).replaceAll("_", "");

			if (StringUtils.isNotBlank(fieldValue)) {
				// Not blank
				currentRow.put(fieldName.toLowerCase(), new FieldInstance(fieldName, fieldValue, field));
			} else {
				// Blank, should we store it?
				if (table.includeEmpty) {
					currentRow.put(fieldName.toLowerCase(), new FieldInstance(fieldName, fieldValue, field));
				}
			}
		}
	}

	protected void recordComplete() throws InterruptedException {
		processed++;

		//
		// We have all the data from the source in our internal state, go ahead with processing
		BaseDocCallable callable = null;
		switch (table.nestType) {
			case ARRAY:
				callable = new NestedDocArrayCallable(config, table);
				break;
			case OBJECT:
				callable = new NestedDocCallable(config, table);
				break;
			case PARENT:
				callable = new ParentDocCallable(config, table);
				break;
			case REFERENCE:
				callable = new ReferenceDocCallable(config, table);
				break;
			case REFERENCE_ARRAY:
				callable = new ReferenceDocArrayCallable(config, table);
				break;
			default:
				break;
		}

		//
		// Add all our data into the callable
		callable.addFields(currentRow);
		executor.submit(callable);

		//
		// Reset our state
		currentRow.clear();
	}
}