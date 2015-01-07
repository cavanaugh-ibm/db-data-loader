package com.cloudant.se.db.loader;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class AppOptions {
	public static final Logger	log				= Logger.getLogger(App.class);

	@Parameter(names = { "-c", "--config" }, description = "The configuration file to load from", required = true)
	protected String			configFileName;

	@Parameter(names = "--debug_cloudant", description = "enables cloudant debugging")
	protected boolean			debug_cloudant	= false;

	@Parameter(names = "--debug_http", description = "enables http debugging")
	protected boolean			debug_http		= false;

	@Parameter(names = { "-?", "--help" }, help = true, description = "display this help")
	protected boolean			help			= false;

	@Override
	public String toString() {
		return "AppOptions [configFileName=" + configFileName + ", debug_http=" + debug_http + ", debug_cloudant=" + debug_cloudant + ", help=" + help + "]";
	}
}
