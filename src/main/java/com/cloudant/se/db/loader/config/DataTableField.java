package com.cloudant.se.db.loader.config;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.ektorp.util.Assert;

import com.cloudant.se.db.loader.AppConstants.TransformLanguage;

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
						break;
					default:
						break;
				}
			} catch (MissingPropertyException e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must not reference invalid properties - " + e.getProperty());
			} catch (MultipleCompilationErrorsException e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must compile - " + e.getMessage());
			} catch (Exception e) {
				Assert.isTrue(false, "Script for " + dbFieldName + " must be exception free - " + e.getMessage());
			}
		}
	}
}
