package com.cloudant.se.db.loader.config;

import java.io.File;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.se.db.loader.AppOptions;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.reinert.jjschema.Attributes;

@Attributes(title = "AppConfig", description = "Main application configuration")
public class AppConfig {
    @JsonIgnore
    protected static final Logger log               = Logger.getLogger(AppConfig.class);

    @JsonIgnore
    public CloudantClient         client            = null;
    @JsonIgnore
    public Database               database          = null;

    @Attributes(required = false, description = "The cloudant account to store the data in")
    private String                cloudantAccount   = null;                             // see mergeOptions method below

    @Attributes(required = false, description = "The cloudant database to store the data in")
    private String                cloudantDatabase  = null;                             // see mergeOptions method below

    @Attributes(required = false, description = "The cloudant password to use")
    private String                cloudantPassword  = null;                             // see mergeOptions method below

    @Attributes(required = false, description = "The cloudant account to use")
    private String                cloudantUser      = null;                             // see mergeOptions method below

    @Attributes(required = false, description = "The concatenation character we will use for ids and parentids")
    private char                  concatinationChar = '_';

    @Attributes(required = false, description = "The http connection timeout")
    private int                   connectionTimeout = 0;                                // In ms

    private File                  defaultDirectory  = new File(".");

    @Attributes(required = false, description = "Default SQL Driver to use when pulling from sources")
    private String                defaultSqlDriver  = null;

    @Attributes(required = false, description = "Default SQL Password to use when pulling from sources")
    private String                defaultSqlPass    = null;

    @Attributes(required = false, description = "Default SQL Url to use when pulling from sources")
    private String                defaultSqlUrl     = null;

    @Attributes(required = false, description = "Default SQL User to use when pulling from sources")
    private String                defaultSqlUser    = null;

    @Attributes(required = false, description = "The number of writer threads to use when connecting to cloudant")
    private int                   numThreads        = 0;

    @Attributes(required = false, description = "The http socket timeout")
    private int                   socketTimeout     = 0;                                // In ms

    @Attributes(required = true, description = "The tables we are going to load")
    private Set<DataTable>        tables            = null;

    public String getCloudantAccount() {
        return cloudantAccount;
    }

    public String getCloudantDatabase() {
        return cloudantDatabase;
    }

    public String getCloudantPassword() {
        return cloudantPassword;
    }

    public String getCloudantUser() {
        return cloudantUser;
    }

    public char getConcatinationChar() {
        return concatinationChar;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public File getDefaultDirectory() {
        return defaultDirectory;
    }

    public String getDefaultSqlDriver() {
        return defaultSqlDriver;
    }

    public String getDefaultSqlPass() {
        return defaultSqlPass;
    }

    public String getDefaultSqlUrl() {
        return defaultSqlUrl;
    }

    public String getDefaultSqlUser() {
        return defaultSqlUser;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public Set<DataTable> getTables() {
        return tables;
    }

    /**
     * Merge options into the config. For the cloudant {account/database/user/pass} we will search in the following order
     * <ol>
     * <li>command line option</li>
     * <li>System property <b>cloudant_*</b></li>
     * <li>key in the configuration file</li>
     * </ol>
     *
     * @param cloudantPassword
     */
    public void mergeOptions(AppOptions options) {
        setCloudantAccount(StringUtils.defaultIfBlank(options.cloudantAccount, getCloudantAccount()));
        setCloudantDatabase(StringUtils.defaultIfBlank(options.cloudantDatabase, getCloudantDatabase()));
        setCloudantUser(StringUtils.defaultIfBlank(options.cloudantUser, getCloudantUser()));
        setCloudantPassword(StringUtils.defaultIfBlank(options.cloudantPassword, getCloudantPassword()));
    }

    public void setCloudantAccount(String cloudantAccount) {
        this.cloudantAccount = cloudantAccount;
    }

    public void setCloudantDatabase(String cloudantDatabase) {
        this.cloudantDatabase = cloudantDatabase;
    }

    public void setCloudantPassword(String cloudantPassword) {
        this.cloudantPassword = cloudantPassword;
    }

    public void setCloudantUser(String cloudantUser) {
        this.cloudantUser = cloudantUser;
    }

    public void setConcatinationChar(char concatinationChar) {
        this.concatinationChar = concatinationChar;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setDefaultDirectory(File defaultDirectory) {
        this.defaultDirectory = defaultDirectory;
    }

    public void setDefaultSqlDriver(String defaultSqlDriver) {
        this.defaultSqlDriver = defaultSqlDriver;
    }

    public void setDefaultSqlPass(String defaultSqlPass) {
        this.defaultSqlPass = defaultSqlPass;
    }

    public void setDefaultSqlUrl(String defaultSqlUrl) {
        this.defaultSqlUrl = defaultSqlUrl;
    }

    public void setDefaultSqlUser(String defaultSqlUser) {
        this.defaultSqlUser = defaultSqlUser;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public void setTables(Set<DataTable> tables) {
        this.tables = tables;
    }

    public void validate() {
        log.info("Validating application configuration");
        //
        // Validate our destination
        Assert.hasText(getCloudantAccount(), "Must provde a cloudant account to work with");
        Assert.hasText(getCloudantDatabase(), "Must provde a cloudant database to work with");
        Assert.hasText(getCloudantUser(), "Must provde a cloudant user name");
        Assert.hasText(getCloudantPassword(), "Must provde a cloudant password");
        printSetting("Will output to  \"" + getCloudantDatabase() + "\" in " + getCloudantAccount());

        //
        // Validate each of the tables
        Assert.notEmpty(getTables(), "Must define at least one table to load");
        for (DataTable table : getTables()) {
            if (table.isUseDatabase()) {
                table.setSqlDriver(StringUtils.isNotBlank(table.getSqlDriver()) ? table.getSqlDriver() : getDefaultSqlDriver());
                table.setSqlPass(StringUtils.isNotBlank(table.getSqlPass()) ? table.getSqlPass() : getDefaultSqlPass());
                table.setSqlUrl(StringUtils.isNotBlank(table.getSqlUrl()) ? table.getSqlUrl() : getDefaultSqlUrl());
                table.setSqlUser(StringUtils.isNotBlank(table.getSqlUser()) ? table.getSqlUser() : getDefaultSqlUser());
            }

            table.validate();
        }

        //
        // Do any calculations that are required
        if (getNumThreads() < 1) {
            setNumThreads(getTables().size() * 6);
        }
        printSetting("Number of writer threads - " + getNumThreads());
    }

    private void printSetting(String message) {
        log.debug("Global: " + message);
    }
}
