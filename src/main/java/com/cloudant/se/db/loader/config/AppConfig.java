package com.cloudant.se.db.loader.config;

import java.io.File;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;

public class AppConfig {
	public int				maxRetries			= 20;
	public char				concatinationChar	= '_';
	public File				defaultDirectory	= new File(".");
	public String			defaultSqlDriver	= null;
	public String			defaultSqlPass		= null;
	public String			defaultSqlUrl		= null;
	public String			defaultSqlUser		= null;

	public String			cloudantAccount		= null;
	public String			cloudantDatabase	= null;
	public String			cloudantUser		= null;
	public String			cloudantPass		= null;

	public Set<DataTable>	tables				= Sets.newLinkedHashSet();

	@JsonIgnore
	public CloudantClient	client				= null;
	@JsonIgnore
	public Database			database			= null;

	@Override
	public String toString() {
		return "AppConfig [tables=" + tables + ", concatinationChar=" + concatinationChar + ", defaultSqlUrl=" + defaultSqlUrl + ", defaultSqlDriver=" + defaultSqlDriver + ", defaultSqlUser="
				+ defaultSqlUser + ", defaultSqlPass=" + defaultSqlPass + ", defaultDirectory=" + defaultDirectory + "]";
	}

	public void validate() {
		Assert.notEmpty(tables, "Must define at least one table to load");
		Assert.hasText(cloudantAccount, "Must provde a cloudant account to work with");
		Assert.hasText(cloudantDatabase, "Must provde a cloudant database to work with");
		Assert.hasText(cloudantUser, "Must provde a cloudant user name");
		Assert.hasText(cloudantPass, "Must provde a cloudant password");

		//
		// Give each table the defaults if it needs it and then let the table validate
		for (DataTable table : tables) {
			if (table.useDatabase) {
				table.sqlDriver = StringUtils.isNotBlank(table.sqlDriver) ? table.sqlDriver : defaultSqlDriver;
				table.sqlPass = StringUtils.isNotBlank(table.sqlPass) ? table.sqlPass : defaultSqlPass;
				table.sqlUrl = StringUtils.isNotBlank(table.sqlUrl) ? table.sqlUrl : defaultSqlUrl;
				table.sqlUser = StringUtils.isNotBlank(table.sqlUser) ? table.sqlUser : defaultSqlUser;
			}

			table.validate();
		}
	}
}
