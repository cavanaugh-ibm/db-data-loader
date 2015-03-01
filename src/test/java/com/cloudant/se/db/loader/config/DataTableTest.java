package com.cloudant.se.db.loader.config;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudant.se.db.loader.AppConstants.NestType;

public class DataTableTest {
	@Test
	public void testValidateArray() {
		DataTable d = new DataTable();
		d.setName("JunitTesting");
		d.setJsonDocumentType("JunitTesting");
		d.setJsonNestType(NestType.ARRAY);
		d.getFileNames().add("bogus_file_name");
		d.getDbIdFields().add("x");

		// Missing all
		try {
			d.validate();
			fail("Array documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing nestField
		d.getDbParentIdFields().add("nParentId");
		try {
			d.validate();
			fail("Array documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		d.setJsonNestField("Children");
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
		d.setName("JunitTesting");
		d.setJsonDocumentType("JunitTesting");
		d.setJsonNestType(NestType.PARENT);
		d.getDbIdFields().add("nId");

		try {
			d.validate();
			fail("File based source files should require fileNames");
		} catch (IllegalArgumentException e) {
		}

		d.getFileNames().add("bogus");
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("File based documents requirements were met");
		}
	}

	@Test
	public void testValidateFileTypeDB() {
		DataTable d = new DataTable();
		d.setName("JunitTesting");
		d.setJsonDocumentType("JunitTesting");
		d.setJsonNestType(NestType.PARENT);
		d.setUseDatabase(true);
		d.getDbIdFields().add("nId");

		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.setSqlDriver("abc1");
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.setSqlPass("abc2");
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.setSqlQuery("abc3");
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.setSqlUrl("abc4");
		try {
			d.validate();
			fail("DB based source files should require sql driver, url, user, pass, and query");
		} catch (IllegalArgumentException e) {
		}

		d.setSqlUser("abc5");
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
		d.setName("JunitTesting");
		d.setJsonDocumentType("JunitTesting");
		d.setJsonNestType(NestType.OBJECT);
		d.getFileNames().add("bogus_file_name");

		// Missing all
		try {
			d.validate();
			fail("Object documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing nestField
		d.getDbParentIdFields().add("nParentId");
		try {
			d.validate();
			fail("Object documents should require parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		d.setJsonNestField("Children");
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("Object documents requirements were met");
		}
	}

	@Test
	public void testValidateParent() {
		DataTable d = new DataTable();
		d.setName("JunitTesting");
		d.setJsonDocumentType("JunitTesting");
		d.setJsonNestType(NestType.PARENT);
		d.getFileNames().add("bogus_file_name");

		// try {
		// d.validate();
		// fail("Parent documents should require idFields");
		// } catch (IllegalArgumentException e) {
		// }

		d.getDbIdFields().add("nId");
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			fail("Parent documents requirements were met");
		}
	}

	@Test
	public void testValidateReference() {
		DataTable d = new DataTable();
		d.setName("JunitTesting");
		d.setJsonDocumentType("JunitTesting");
		d.setJsonNestType(NestType.REFERENCE);
		d.getFileNames().add("bogus_file_name");

		// Missing all three
		try {
			d.validate();
			fail("Reference documents should require idFields, parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing parentIdFields, nestField
		d.getDbIdFields().add("nId");
		try {
			d.validate();
			fail("Reference documents should require idFields, parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		// Missing nestField
		d.getDbParentIdFields().add("nParentId");
		try {
			d.validate();
			fail("Reference documents should require idFields, parentIdFields, nestField");
		} catch (IllegalArgumentException e) {
		}

		d.setJsonNestField("Children");
		try {
			d.validate();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			fail("Reference documents requirements were met");
		}
	}
}
