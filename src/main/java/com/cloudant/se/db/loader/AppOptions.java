package com.cloudant.se.db.loader;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class AppOptions {
	public static final Logger	log		= Logger.getLogger(App.class);

	@Parameter(names = { "-c", "-config" }, description = "The configuration file to load from", required = true)
	protected String			configFileName;

	@Parameter(names = { "-log", "-verbose" }, description = "Level of verbosity")
	protected Integer			verbose	= 0;

	@Parameter(names = { "-?", "--help" }, help = true, description = "Display this help")
	protected boolean			help;

	@Override
	public String toString() {
		return "AppOptions [configFileName=" + configFileName + ", verbose=" + verbose + ", help=" + help + "]";
	}
}
