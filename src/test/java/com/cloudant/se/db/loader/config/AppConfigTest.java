package com.cloudant.se.db.loader.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.cloudant.se.db.loader.AppOptions;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AppConfigTest {
    @Test
    public void mergeOptions() {
        doTestVars("a", null, null, null, "a", "config_database", "config_user", "config_password");
        doTestVars(null, "b", null, null, "config_account", "b", "config_user", "config_password");
        doTestVars(null, null, "c", null, "config_account", "config_database", "c", "config_password");
        doTestVars(null, null, null, "d", "config_account", "config_database", "config_user", "d");
        doTestVars("a", "b", "c", "d", "a", "b", "c", "d");
    }

    @Test
    public void testValidate() {
        File folder = new File("src/test/resources/configs");
        File[] listOfFiles = folder.listFiles();
        ObjectMapper mapper = new ObjectMapper();

        for (File listOfFile : listOfFiles) {
            if (listOfFile.isFile()) {
                try {
                    AppConfig config = mapper.readValue(listOfFile, AppConfig.class);
                    if (listOfFile.getName().toUpperCase().endsWith("_PASS.JSON")) {
                        try {
                            config.validate();
                        } catch (IllegalArgumentException e) {
                            fail("Should have passed - \"" + listOfFile + "\" - " + e.getMessage());
                        }
                    } else if (listOfFile.getName().toUpperCase().endsWith("_FAIL.JSON")) {
                        try {
                            config.validate();
                            fail("Should have errored - \"" + listOfFile + "\"");
                        } catch (IllegalArgumentException e) {
                        }
                    } else {
                        try {
                            config.validate();
                        } catch (IllegalArgumentException e) {
                            fail("Could not determine desired pass/fail from name but it failed - \"" + listOfFile + "\" - " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Error reading/parsing - \"" + listOfFile + "\" - " + e.getMessage());
                }
            }
        }
    }

    private void checkVars(AppConfig config, String account, String db, String user, String password) {
        assertEquals(account, config.cloudantAccount);
        assertEquals(db, config.cloudantDatabase);
        assertEquals(user, config.cloudantUser);
        assertEquals(password, config.cloudantPassword);
    }

    private void doTestVars(String propAccount, String propDatabase, String propUser, String propPassword, String expectedAccount, String expectedDatabase, String expectedUser, String expectedPassword) {
        ObjectMapper mapper = new ObjectMapper();
        File configFile = new File("src/test/resources/configs/minimal_pass.json");
        AppConfig config = null;

        try {
            // Set the properties the way we want them
            setVars(propAccount, propDatabase, propUser, propPassword);

            // Read in and merge the options
            config = mapper.readValue(configFile, AppConfig.class);
            config.mergeOptions(new AppOptions());

            // Check the merge
            checkVars(config, expectedAccount, expectedDatabase, expectedUser, expectedPassword);
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error reading/parsing - \"" + configFile + "\" - " + e.getMessage());
        }

    }

    private void setVars(String account, String db, String user, String password) {
        System.setProperty("cloudant_account", StringUtils.defaultString(account));
        System.setProperty("cloudant_database", StringUtils.defaultString(db));
        System.setProperty("cloudant_user", StringUtils.defaultString(user));
        System.setProperty("cloudant_password", StringUtils.defaultString(password));
    }
}
