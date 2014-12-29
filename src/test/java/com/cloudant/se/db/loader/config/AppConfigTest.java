package com.cloudant.se.db.loader.config;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AppConfigTest {
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
}
