package com.cloudant.se.db.loader.config;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.cloudant.se.Constants;
import com.cloudant.se.db.loader.AppConstants.FileType;
import com.cloudant.se.db.loader.AppConstants.NestType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;

public class DataTable {
	@JsonIgnore
	protected static final Logger	log					= Logger.getLogger(DataTable.class);

	public Set<DataTableField>		dataFields			= Sets.newLinkedHashSet();

	public String					name				= null;

	public Set<String>				fileNames			= Sets.newLinkedHashSet();

	public FileType					fileType			= FileType.CSV;

	public String					uniqueIdField		= "_id";
	public Set<String>				idFields			= Sets.newLinkedHashSet();
	public String					jsonDocumentType	= null;

	public String					nestField			= null;
	public NestType					nestType			= NestType.PARENT;
	public Set<String>				parentIdFields		= Sets.newLinkedHashSet();
	public String					sqlDriver			= null;
	public String					sqlPass				= null;
	public String					sqlQuery			= null;

	public String					sqlUrl				= null;
	public String					sqlUser				= null;

	public boolean					useDatabase			= false;
	public boolean					includeEmpty		= false;

	public boolean					tryCaseNumeric		= false;

	@Override
	public String toString() {
		return "DataTable [jsonDocumentType=" + jsonDocumentType + ", idFields=" + idFields + ", useDatabase=" + useDatabase + ", sqlUrl=" + sqlUrl + ", sqlDriver=" + sqlDriver + ", sqlUser="
				+ sqlUser + ", sqlPass=" + sqlPass + ", sqlQuery=" + sqlQuery + ", fileNames=" + fileNames + ", fileType=" + fileType + ", dataFields=" + dataFields + ", parentIdFields="
				+ parentIdFields + ", nestField=" + nestField + ", nestType=" + nestType + "]";
	}

	public void validate() {
		Assert.hasText(name, "Must provide a name for this table (used during logging and debugging)");

		printSetting("Name", name);
		printSetting("Type", nestType);

		if (useDatabase) {
			Assert.hasText(sqlUrl, "Must provide the JDBC URL of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlDriver, "Must provide the JDBC driver of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlUser, "Must provide the JDBC user of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlPass, "Must provide the JDBC password of the database to pull this data from or set the default database configs");
			Assert.hasText(sqlQuery, "Must provide the SQL query that we will use to pull the data");
			printSetting("Source", sqlUrl);
		} else {
			Assert.notEmpty(fileNames, "Must provide a data file for this table");
			Assert.notNull(fileType, "Must provide a type for the data files");
			printSetting("Source", fileNames);
		}

		switch (nestType) {
			case PARENT:
				//
				// PARENT table records must provide at the very least the idFields
				// Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
				Assert.hasText(jsonDocumentType, "Must provide the type name for the created document ");
				Assert.isTrue(StringUtils.equals("_id", uniqueIdField), "For top level documents, the idField must be \"_id\"");
				if (idFields == null || idFields.size() == 0) {
					idFields = Sets.newLinkedHashSet();
					idFields.add(Constants.GENERATED);
				}
				printSetting("_id fields", idFields);
				break;
			case ARRAY:
				//
				// ARRAY table records must provide at the very least the parentFields and the nestField
				Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
				Assert.notEmpty(parentIdFields, "Must provide field(s) to create to find the parent _id for the record that each row will be inserted into - parentIdFields");
				Assert.hasText(nestField, "Must provide the field in the parent document that we will insert at");
				printSetting("_id fields", idFields);
				printSetting("parent _id fields", parentIdFields);
				printSetting("nestField", nestField);
				break;
			case OBJECT:
				//
				// OBJECT table records must provide at the very least the parentFields and the nestField
				Assert.notEmpty(parentIdFields, "Must provide field(s) to create an _id for the record that each row will be inserted into - parentIdFields");
				Assert.hasText(nestField, "Must provide the field in the parent document that we will insert at");
				printSetting("parent _id fields", parentIdFields);
				printSetting("nestField", nestField);
				break;
			case REFERENCE:
			case REFERENCE_ARRAY:
				//
				// REFERENCE table records must provide at the very least the parentFields and the nestField and the field(s) to create an _id for the records in this table
				Assert.notEmpty(parentIdFields, "Must provide field(s) to create an _id for the record that each row will be inserted into - parentIdFields");
				Assert.hasText(nestField, "Must provide the field in the parent document that we will insert at");
				Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
				Assert.hasText(jsonDocumentType, "Must provide the type name for the created document ");
				Assert.isTrue(StringUtils.equals("_id", uniqueIdField), "For top level documents, the idField must be \"_id\"");
				printSetting("_id fields", idFields);
				printSetting("parent _id fields", parentIdFields);
				printSetting("nestField", nestField);
				break;
			default:
				break;
		}

		uniqueIdField = StringUtils.defaultIfBlank(uniqueIdField, "_id");
		// System.out.println("Unique key equals - [" + idField + "][" + StringUtils.defaultIfBlank(jsonDocumentType, nestField) + "]");

		if (dataFields != null) {
			for (DataTableField field : dataFields) {
				log.info("    Validating field");
				field.validate();
			}
		}
	}

	private void printSetting(String setting, Object value) {
		log.info("    " + setting + " --> " + value);
	}
}
