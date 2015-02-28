package com.cloudant.se.db.loader;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class AppOptions {
    public static final Logger log              = Logger.getLogger(App.class);

    @Parameter(names = { "-c", "-config" }, description = "The configuration file to load from", required = true)
    public String              configFileName;

    @Parameter(names = "-claccount", description = "Cloudant destination account")
    public String              cloudantAccount  = System.getProperty("cloudant_account");

    @Parameter(names = "-cldatabase", description = "Cloudant destination database")
    public String              cloudantDatabase = System.getProperty("cloudant_database");

    @Parameter(names = "-cluser", description = "Cloudant destination user")
    public String              cloudantUser     = System.getProperty("cloudant_user");

    @Parameter(names = "-clpassword", description = "Cloudant destination password", password = true)
    public String              cloudantPassword = System.getProperty("cloudant_password");

    @Parameter(names = { "-log", "-verbose" }, description = "Level of verbosity")
    public Integer             verbose          = 0;

    @Parameter(names = { "-traceread" }, description = "Trace reading code", hidden = true)
    public boolean             traceRead        = false;

    @Parameter(names = { "-tracewrite" }, description = "Trace writing code", hidden = true)
    public boolean             traceWrite       = false;

    @Parameter(names = { "-?", "--help" }, help = true, description = "Display this help")
    public boolean             help;
}
