package com.cloudant.se.db.loader.config;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.log4j.Logger;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.springframework.util.Assert;

import com.cloudant.se.db.loader.AppConstants.TransformLanguage;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class DataTableField {
	@JsonIgnore
	protected static final Logger	log							= Logger.getLogger(DataTableField.class);
	public String					dbFieldName					= null;

	public boolean					include						= true;
	public boolean					isNumericHint				= false;
	public boolean					isReference					= false;
	public String					jsonFieldName				= null;
	public String					transformScript				= null;
	public TransformLanguage		transformScriptLanguage		= TransformLanguage.GROOVY;

	public boolean					isDate						= false;
	public boolean					isNotDate					= false;
	public boolean					outputNumber				= false;
	public boolean					outputString				= false;
	public String					outputDateStringFormat		= null;
	public String					outputDateStringTimezone	= null;

	@Override
	public String toString() {
		return "DataTableField [include=" + include + ", dbFieldName=" + dbFieldName + ", jsonFieldName=" + jsonFieldName + ", transformScript=" + transformScript + ", transformScriptLanguage="
				+ transformScriptLanguage + "]";
	}

	public void validate() {
		Assert.hasText(dbFieldName);
		printSetting("dbFieldName", dbFieldName);

		if (StringUtils.isBlank(jsonFieldName)) {
			jsonFieldName = WordUtils.capitalize(dbFieldName, new char[] { '_' }).replaceAll("_", "");
		}
		printSetting("jsonFieldName", jsonFieldName);

		printSetting("isDate", isDate);
		printSetting("isNotDate", isNotDate);
		if (isDate) {
			Assert.isTrue(outputNumber != outputString, dbFieldName + " - Must chose either outputing as a date or a number");
			Assert.isTrue(!(outputNumber && outputString), dbFieldName + " - Must chose either outputing as a date or a number");

			printSetting("outputNumber", outputNumber);
			printSetting("outputString", outputString);

			if (outputString) {
				Assert.hasText(outputDateStringFormat, "Must provde an output format for casted date strings");
				Assert.hasText(outputDateStringTimezone, "Must provde a timezone for casted date strings");
				printSetting("outputDateStringFormat", outputDateStringFormat);
				printSetting("outputDateStringTimezone", outputDateStringTimezone);
			}
		}

		if (StringUtils.isNotBlank(transformScript)) {
			try {
				switch (transformScriptLanguage) {
					case GROOVY:
						Binding binding = new Binding();
						binding.setVariable("input", "bogus_value");
						GroovyShell shell = new GroovyShell(binding);

						shell.evaluate(transformScript);
						break;
					case JAVASCRIPT:
						//
						// Wrap the javascript in a function call so the return works as expected
						transformScript = "function runcode(input) { " + transformScript + " }\n runcode(input);";

						ScriptEngineManager factory = new ScriptEngineManager();
						ScriptEngine engine = factory.getEngineByName("JavaScript");
						engine.put("input", "bogus_value");

						engine.eval(transformScript);
						break;
					default:
						break;
				}
			} catch (MissingPropertyException e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must not reference invalid properties - " + e.getProperty());
			} catch (MultipleCompilationErrorsException e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must compile - " + e.getMessage());
				// } catch (EvaluatorException e) {
				// e.printStackTrace();
				// Assert.isTrue(false, "Script for " + dbFieldName + " must compile - " + e.getMessage());
			} catch (Exception e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must be exception free - " + e.getMessage());
			}

			printSetting("transformScriptLanguage", transformScriptLanguage);
			printSetting("transformScript", transformScript);
		}
	}

	private void printSetting(String setting, Object value) {
		log.info("        " + setting + " --> " + value);
	}
}
