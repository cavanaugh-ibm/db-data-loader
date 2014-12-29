package com.cloudant.se.db.loader.config;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudant.se.db.loader.config.DataTable.NestType;

public class DataTableTest {
	@Test
	public void testValidateArray() {
		DataTable d = new DataTable();
		d.jsonDocumentType = "JunitTesting";
		d.nestType = NestType.ARRAY;
		d.fileNames.add("bogus_file_name");
		d.idFields.add("x");

		// Missing all
		try {
			d.validate();
			fail("Array documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing nestField
		d.parentIdFields.add("nParentId");
		try {
			d.validate();
			fail("Array documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		d.nestField = "Children";
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("Array documents requirements were met but complained about - " + e.getMessage());
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateEmpty() {
		DataTableField f = new DataTableField();
		f.validate();
	}

	@Test
	public void testValidateFileTypeCSV() {
		DataTable d = new DataTable();
		d.jsonDocumentType = "JunitTesting";
		d.nestType = NestType.PARENT;
		d.idFields.add("nId");

		try {
			d.validate();
			fail("File based source files should require fileNames");
		} catch (IllegalArgumentException e) {
		}

		d.fileNames.add("bogus");
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("File based documents requirements were met");
		}
	}

	@Test
	public void testValidateFileTypeDB() {
		DataTable d = new DataTable();
		d.jsonDocumentType = "JunitTesting";
		d.nestType = NestType.PARENT;
		d.useDatabase = true;
		d.idFields.add("nId");

		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.sqlDriver = "abc1";
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.sqlPass = "abc2";
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.sqlQuery = "abc3";
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.sqlUrl = "abc4";
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.sqlUser = "abc5";
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			fail("DB based documents requirements were met");
		}
	}

	@Test
	public void testValidateObject() {
		DataTable d = new DataTable();
		d.jsonDocumentType = "JunitTesting";
		d.nestType = NestType.OBJECT;
		d.fileNames.add("bogus_file_name");

		// Missing all
		try {
			d.validate();
			fail("Object documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing nestField
		d.parentIdFields.add("nParentId");
		try {
			d.validate();
			fail("Object documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		d.nestField = "Children";
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("Object documents requirements were met");
		}
	}

	@Test
	public void testValidateParent() {
		DataTable d = new DataTable();
		d.jsonDocumentType = "JunitTesting";
		d.nestType = NestType.PARENT;
		d.fileNames.add("bogus_file_name");

		try {
			d.validate();
			fail("Parent documents should require idFields");
		} catch (IllegalArgumentException e) {
		}

		d.idFields.add("nId");
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("Parent documents requirements were met");
		}
	}

	@Test
	public void testValidateReference() {
		DataTable d = new DataTable();
		d.jsonDocumentType = "JunitTesting";
		d.nestType = NestType.REFERENCE;
		d.fileNames.add("bogus_file_name");

		// Missing all three
		try {
			d.validate();
			fail("Reference documents should require idFields, parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing parentIdFields, nestField
		d.idFields.add("nId");
		try {
			d.validate();
			fail("Reference documents should require idFields, parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing nestField
		d.parentIdFields.add("nParentId");
		try {
			d.validate();
			fail("Reference documents should require idFields, parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		d.nestField = "Children";
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			fail("Reference documents requirements were met");
		}
	}
}
