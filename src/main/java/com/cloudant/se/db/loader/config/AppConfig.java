package com.cloudant.se.db.loader.config;

import java.io.File;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.se.db.loader.write.BaseDocCallable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;

public class AppConfig {
	@JsonIgnore
	protected static final Logger	log						= Logger.getLogger(BaseDocCallable.class);

	@JsonIgnore
	public CloudantClient			client					= null;
	public String					cloudantAccount			= null;
	public String					cloudantDatabase		= null;
	public String					cloudantPass			= null;
	public String					cloudantUser			= null;
	public char						concatinationChar		= '_';
	@JsonIgnore
	public Database					database				= null;

	public File						defaultDirectory		= new File(".");
	public String					defaultSqlDriver		= null;
	public String					defaultSqlPass			= null;
	public String					defaultSqlUrl			= null;

	public String					defaultSqlUser			= null;

	public int						maxRetries				= 20;
	public int						numThreads				= 0;
	public int						connectionTimeout		= 0;										// In ms
	public int						socketTimeout			= 0;										// In ms

	public Set<DataTable>			tables					= Sets.newLinkedHashSet();

	public boolean					autoCastDatesToNumbers	= false;
	public boolean					autoCastDatesToStrings	= false;
	public String					autoCastDatesFormat		= null;
	public String					autoCastDatesTimezone	= null;

	@Override
	public String toString() {
		return "AppConfig [tables=" + tables + ", concatinationChar=" + concatinationChar + ", defaultSqlUrl=" + defaultSqlUrl + ", defaultSqlDriver=" + defaultSqlDriver + ", defaultSqlUser="
				+ defaultSqlUser + ", defaultSqlPass=" + defaultSqlPass + ", defaultDirectory=" + defaultDirectory + "]";
	}

	public void validate() {
		log.info("Validating configuration");
		//
		// Validate our destination
		Assert.hasText(cloudantAccount, "Must provde a cloudant account to work with");
		Assert.hasText(cloudantDatabase, "Must provde a cloudant database to work with");
		Assert.hasText(cloudantUser, "Must provde a cloudant user name");
		Assert.hasText(cloudantPass, "Must provde a cloudant password");
		printSetting("Will output to  \"" + cloudantDatabase + "\" in " + cloudantAccount);

		//
		// Validate our date casting logic
		Assert.isTrue(!(autoCastDatesToNumbers && autoCastDatesToStrings), "Must chose either auto casting and outputing dates as strings or number");
		if (autoCastDatesToStrings) {
			Assert.hasText(autoCastDatesFormat, "Must provde an output format for casted date strings");
			Assert.hasText(autoCastDatesTimezone, "Must provde a timezone for casted date strings");
			printSetting("Will auto cast dates and output like \"" + autoCastDatesFormat + "\" in " + autoCastDatesTimezone + " timezone");
		}
		if (autoCastDatesToNumbers) {
			printSetting("Will auto cast dates and output as epoch");
		}

		//
		// Validate each of the tables
		Assert.notEmpty(tables, "Must define at least one table to load");
		for (DataTable table : tables) {
			if (table.useDatabase) {
				table.sqlDriver = StringUtils.isNotBlank(table.sqlDriver) ? table.sqlDriver : defaultSqlDriver;
				table.sqlPass = StringUtils.isNotBlank(table.sqlPass) ? table.sqlPass : defaultSqlPass;
				table.sqlUrl = StringUtils.isNotBlank(table.sqlUrl) ? table.sqlUrl : defaultSqlUrl;
				table.sqlUser = StringUtils.isNotBlank(table.sqlUser) ? table.sqlUser : defaultSqlUser;
			}

			log.info("Validating table");
			table.validate();
		}

		//
		// Do any calculations that are required
		if (numThreads < 1) {
			numThreads = tables.size() * 6;
		}
		printSetting("Number of writer threads - " + numThreads);
	}

	private void printSetting(String message) {
		log.info("Global: " + message);
	}
}
