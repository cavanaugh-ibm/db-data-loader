package com.cloudant.se.db.loader.config;

import org.apache.commons.lang.StringUtils;
import org.ektorp.util.Assert;

import com.cloudant.se.db.loader.App.TransformLanguage;

public class DataTableField {
	public String				dbFieldName				= null;

	public boolean				include					= true;
	public boolean				isReference				= false;
	public boolean				isNumericHint			= false;
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
			jsonFieldName = dbFieldName;
		}
	}
}
