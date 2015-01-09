package com.cloudant.se.db.loader.config;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.ektorp.util.Assert;

import sun.org.mozilla.javascript.internal.EvaluatorException;

import com.cloudant.se.db.loader.AppConstants.TransformLanguage;

@SuppressWarnings("restriction")
public class DataTableField {
	public String				dbFieldName				= null;

	public boolean				include					= true;
	public boolean				isNumericHint			= false;
	public boolean				isReference				= false;
	public String				jsonFieldName			= null;
	public String				transformScript			= null;
	public TransformLanguage	transformScriptLanguage	= TransformLanguage.GROOVY;

	@Override
	public String toString() {
		return "DataTableField [include=" + include + ", dbFieldName=" + dbFieldName + ", jsonFieldName=" + jsonFieldName + ", transformScript=" + transformScript + ", transformScriptLanguage="
				+ transformScriptLanguage + "]";
	}

	public void validate() {
		Assert.hasText(dbFieldName);

		if (StringUtils.isBlank(jsonFieldName)) {
			jsonFieldName = WordUtils.capitalize(dbFieldName, new char[] { '_' }).replaceAll("_", "");
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
			} catch (EvaluatorException e) {
				e.printStackTrace();
				Assert.isTrue(false, "Script for " + dbFieldName + " must compile - " + e.getMessage());
			} catch (Exception e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must be exception free - " + e.getMessage());
			}
		}
	}
}
