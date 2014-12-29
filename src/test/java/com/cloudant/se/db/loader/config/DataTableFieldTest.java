package com.cloudant.se.db.loader.config;

import org.junit.Assert;
import org.junit.Test;

import com.cloudant.se.db.loader.App.TransformLanguage;

public class DataTableFieldTest {
	@Test(expected = IllegalArgumentException.class)
	public void testValidateEmpty() {
		DataTableField f = new DataTableField();
		f.validate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateExcludeNoDb() {
		DataTableField f = new DataTableField();
		f.include = false;
		f.jsonFieldName = "testing";
		f.validate();
	}

	@Test
	public void testValidateExcludeNoJson() {
		DataTableField f = new DataTableField();
		f.include = false;
		f.dbFieldName = "testing";
		f.validate();

		Assert.assertEquals("Testing", f.jsonFieldName);
	}

	@Test
	public void testValidateGood() {
		DataTableField f = new DataTableField();
		f.dbFieldName = "sTesting";
		f.jsonFieldName = "testing";
		f.validate();

		Assert.assertEquals("sTesting", f.dbFieldName);
		Assert.assertEquals("testing", f.jsonFieldName);
		Assert.assertNull(f.transformScript);
		Assert.assertEquals(TransformLanguage.GROOVY, f.transformScriptLanguage);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateIncludeNoDb() {
		DataTableField f = new DataTableField();
		f.jsonFieldName = "testing";
		f.validate();
	}

	@Test
	public void testValidateIncludeNoJson() {
		DataTableField f = new DataTableField();
		f.dbFieldName = "testing";
		f.validate();

		Assert.assertEquals("Testing", f.jsonFieldName);
	}
}
