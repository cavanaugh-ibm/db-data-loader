package com.cloudant.se.db.loader;

import java.util.List;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class AppOptions {
	public static final Logger	log		= Logger.getLogger(App.class);

	@Parameter(names = { "-?", "--help" }, help = true, description = "display this help")
	protected boolean			help	= false;

	@Parameter(names = "--debug", description = "enables client debugging")
	protected boolean			debug	= false;

	@Parameter(names = { "-c", "--config" }, description = "The configuration file to load from", required = true)
	protected String			configFileName;

	@Parameter(hidden = true)
	protected List<String>		unrecognizedOptions;

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [help=" + help + ", debug=" + debug + ", configFileName=" + configFileName + ", unrecognizedOptions=" + unrecognizedOptions + "]";
	}
}
