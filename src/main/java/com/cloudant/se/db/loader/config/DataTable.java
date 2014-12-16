package com.cloudant.se.db.loader.config;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import com.google.common.collect.Sets;

public class DataTable {
	public Set<DataTableField>	dataFields			= Sets.newLinkedHashSet();

	public Set<String>			fileNames			= Sets.newLinkedHashSet();

	public FileType				fileType			= FileType.CSV;

	public String				idField				= "_id";
	public Set<String>			idFields			= Sets.newLinkedHashSet();
	public String				jsonDocumentType	= null;

	public String				nestField			= null;
	public NestType				nestType			= NestType.PARENT;
	public Set<String>			parentIdFields		= Sets.newLinkedHashSet();
	public String				sqlDriver			= null;
	public String				sqlPass				= null;
	public String				sqlQuery			= null;

	public String				sqlUrl				= null;
	public String				sqlUser				= null;

	public boolean				useDatabase			= false;

	@Override
	public String toString() {
		return "DataTable [jsonDocumentType=" + jsonDocumentType + ", idFields=" + idFields + ", useDatabase=" + useDatabase + ", sqlUrl=" + sqlUrl + ", sqlDriver=" + sqlDriver + ", sqlUser="
				+ sqlUser + ", sqlPass=" + sqlPass + ", sqlQuery=" + sqlQuery + ", fileNames=" + fileNames + ", fileType=" + fileType + ", dataFields=" + dataFields + ", parentIdFields="
				+ parentIdFields + ", nestField=" + nestField + ", nestType=" + nestType + "]";
	}

	public void validate() {
		if (useDatabase) {
			Assert.hasText(sqlUrl, "Must provide the JDBC URL of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlDriver, "Must provide the JDBC driver of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlUser, "Must provide the JDBC user of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlPass, "Must provide the JDBC password of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlQuery, "Must provide the SQL query that we will use to pull the data");
		} else {
			Assert.notEmpty(fileNames, "Must provide a data file for this table");
			Assert.notNull(fileType, "Must provide a type for the data files");
		}

		switch (nestType) {
			case PARENT:
				//
				// PARENT table records must provide at the very least the idFields
				Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
				Assert.hasText(jsonDocumentType, "Must provide the type name for the created document ");
				Assert.isTrue(StringUtils.equals("_id", idField), "For top level documents, the idField must be \"_id\"");
				break;
			case ARRAY:
				//
				// ARRAY table records must provide at the very least the parentFields and the nestField
				Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
				Assert.notEmpty(parentIdFields, "Must provide field(s) to create to find the parent _id for the record that each row will be inserted into - parentIdFields");
				Assert.hasText(nestField, "Must provide the field in the parent document that we will insert at");
				break;
			case OBJECT:
				//
				// OBJECT table records must provide at the very least the parentFields and the nestField
				Assert.notEmpty(parentIdFields, "Must provide field(s) to create an _id for the record that each row will be inserted into - parentIdFields");
				Assert.hasText(nestField, "Must provide the field in the parent document that we will insert at");
				break;
			case REFERENCE:
			case REFERENCE_ARRAY:
				//
				// REFERENCE table records must provide at the very least the parentFields and the nestField and the field(s) to create an _id for the records in this table
				Assert.notEmpty(parentIdFields, "Must provide field(s) to create an _id for the record that each row will be inserted into - parentIdFields");
				Assert.hasText(nestField, "Must provide the field in the parent document that we will insert at");
				Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
				Assert.hasText(jsonDocumentType, "Must provide the type name for the created document ");
				Assert.isTrue(StringUtils.equals("_id", idField), "For top level documents, the idField must be \"_id\"");
				break;
			default:
				break;
		}

		idField = StringUtils.defaultIfBlank(idField, "_id");
		//System.out.println("Unique key equals - [" + idField + "][" + StringUtils.defaultIfBlank(jsonDocumentType, nestField) + "]");

		if (dataFields != null) {
			for (DataTableField field : dataFields) {
				field.validate();
			}
		}
	}

	public enum FileType {
		CSV, JSON, XML
	}

	public enum NestType {
		ARRAY, OBJECT, PARENT, REFERENCE, REFERENCE_ARRAY
	}
}
