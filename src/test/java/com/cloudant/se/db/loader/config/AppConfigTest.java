package com.cloudant.se.db.loader.config;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AppConfigTest {
	@Test
	public void testValidate() {
		File folder = new File("src/test/resources/configs");
		File[] listOfFiles = folder.listFiles();
		ObjectMapper mapper = new ObjectMapper();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				try {
					AppConfig config = mapper.readValue(listOfFiles[i], AppConfig.class);
					if (listOfFiles[i].getName().toUpperCase().endsWith("_PASS.JSON")) {
						try {
							config.validate();
						} catch (IllegalArgumentException e) {
							fail("Should have passed - \"" + listOfFiles[i] + "\" - " + e.getMessage());
						}
					} else if (listOfFiles[i].getName().toUpperCase().endsWith("_FAIL.JSON")) {
						try {
							config.validate();
							fail("Should have errored - \"" + listOfFiles[i] + "\"");
						} catch (IllegalArgumentException e) {
						}
					} else {
						try {
							config.validate();
						} catch (IllegalArgumentException e) {
							fail("Could not determine desired pass/fail from name but it failed - \"" + listOfFiles[i] + "\" - " + e.getMessage());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					fail("Error reading/parsing - \"" + listOfFiles[i] + "\" - " + e.getMessage());
				}
			}
		}
	}
}
