package com.cloudant.se.db.loader.read;

import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;

public class CsvDataTableReader extends ADataTableReader {
	private static final Logger	log	= Logger.getLogger(CsvDataTableReader.class);

	public CsvDataTableReader(AppConfig config, DataTable table, ExecutorService writerExecutor) {
		super(config, table, writerExecutor);
	}

	@Override
	public Integer call() throws Exception {
		for (String fName : table.fileNames) {
			log.info("CSV reader starting file " + fName);
			try {
				File csvFile = null;
				if (fName.startsWith(File.pathSeparator)) {
					csvFile = new File(fName);
				} else {
					csvFile = new File(config.defaultDirectory.getAbsolutePath() + File.separator + fName);
				}

				if (csvFile.canRead()) {
					CSVReader reader = null;
					try {
						reader = new CSVReader(new FileReader(csvFile));
						String[] nextLine;

						// Assume the header is the first row
						// TODO - Need to handle errors better
						String[] header = reader.readNext();

						//
						// Loop through the remaining lines and give them to the record handler from the super class
						while ((nextLine = reader.readNext()) != null) {
							//
							// Reset our state for the current row
							reset();

							//
							// If the parsed record is of the correct length (successful parsing)
							if (header.length == nextLine.length) {
								for (int i = 0; i < header.length; i++) {
									addField(header[i], nextLine[i]);
								}
								recordComplete();
							} else {
								invalidCsvRecord(header, nextLine);
							}
						}
					} finally {
						reader.close();
					}
				} else {
					return -1;
				}
			} catch (Throwable e) {
				log.error("Error working with " + fName, e);
			}
		}

		log.info("CSV Reader completed for " + table.fileNames + " - " + processed);
		return processed;
	}

	private void invalidCsvRecord(String[] header, String[] nextLine) {
		log.error("Unable to process CSV record. Header [" + Arrays.toString(header) + "], data [" + Arrays.toString(nextLine) + "]");
	}
}
