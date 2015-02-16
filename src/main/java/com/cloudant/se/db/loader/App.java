package com.cloudant.se.db.loader;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.model.ConnectOptions;
import com.cloudant.se.concurrent.StatusingThreadPoolExecutor;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.read.BaseDataTableReader;
import com.cloudant.se.db.loader.read.CsvDataTableReader;
import com.cloudant.se.db.loader.read.SqlDataTableReader;
import com.cloudant.se.db.loader.write.BaseDocCallable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Things to consider in this - Should we use a pool for objects to speed up?
 *
 * @author Cavanaugh
 *
 */
public class App {
	private static final Logger	log				= Logger.getLogger(App.class);	;

	protected AppConfig			config			= null;
	protected AppOptions		options			= null;

	protected ExecutorService	readerExecutor	= null;
	protected ExecutorService	writerExecutor	= null;

	public App() {
	}

	public int config(String[] args) {
		options = new AppOptions();
		JCommander jCommander = new JCommander();
		jCommander.setProgramName("Cloudandt \"Relational Database\" Import Utility");
		jCommander.addObject(options);

		//
		// Try to parse the options we were given
		try {
			jCommander.parse(args);
		} catch (ParameterException e) {
			showUsage(jCommander);
			return 1;
		}

		//
		// Show the help if they asked for it
		if (options.help) {
			showUsage(jCommander);
			return 2;
		}

		//
		// Enable debugging if asked
		if (options.verbose > 0) {
			// Logger.getRootLogger().setLevel(Level.DEBUG);
			if (options.verbose >= 1) {
				Logger.getLogger("com.cloudant").setLevel(Level.DEBUG);
			}
			if (options.verbose >= 2) {
				Logger.getLogger("org.lightcouch").setLevel(Level.DEBUG);
			}
			if (options.verbose >= 3) {
				Logger.getLogger("org.apache.http").setLevel(Level.DEBUG);
			}
		}

		//
		// Enable tracing if asked
		if (options.traceRead) {
			Logger.getLogger(BaseDataTableReader.class.getPackage().getName()).setLevel(Level.TRACE);
		}
		if (options.traceWrite) {
			Logger.getLogger(BaseDocCallable.class.getPackage().getName()).setLevel(Level.TRACE);
		}

		//
		// Read the config they gave us
		try {
			//
			// Read the configuration from our file and let it validate itself
			ObjectMapper mapper = new ObjectMapper();
			File configFile = new File(options.configFileName);
			config = mapper.readValue(configFile, AppConfig.class);
			config.defaultDirectory = configFile.getParentFile() != null ? configFile.getParentFile() : new File(".");
			config.validate();

			//
			// Print out a sample of what the output JSON documents will look like will look like
			// TODO
			// config.printSample();
		} catch (IllegalArgumentException e) {
			System.err.println("Configuration error detected - " + e.getMessage());
			config = null;
			return -2;
		} catch (UnrecognizedPropertyException e) {
			System.err.println("Configuration error detected - unrecognized parameter - typo? - [" + e.getUnrecognizedPropertyName() + "][" + e.getLocation() + "]");
			config = null;
			return -3;
		} catch (Exception e) {
			System.err.println("Unexpected exception - see log for details - " + e.getMessage());
			log.error(e.getMessage(), e);
			return -1;
		}
		//
		// Setup our executor service
		readerExecutor = Executors.newFixedThreadPool(config.tables.size(), new ThreadFactoryBuilder().setNameFormat("ldr-r-%d").build());

		int threads = config.numThreads;
		BlockingQueue<Runnable> blockingQueue = new LinkedBlockingDeque<Runnable>(threads * 3);
		ThreadFactory writeThreadFactory = new ThreadFactoryBuilder().setNameFormat("ldr-w-%d").build();
		writerExecutor = new StatusingThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, blockingQueue, writeThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

		return 0;
	}

	private void showUsage(JCommander jCommander) {
		jCommander.usage();
	}

	private int start() {
		log.info("Configuration complete, starting up");
		try {
			ConnectOptions options = new ConnectOptions();
			options.setMaxConnections(config.numThreads);

			config.client = new CloudantClient(config.cloudantAccount, config.cloudantUser, config.cloudantPass, options);
			config.database = config.client.database(config.cloudantDatabase, false);
			log.info(" --- Connected to Cloudant --- ");
			// log.debug("Available databases - " + config.client.getAllDbs());
			// log.debug("Database shards - " + config.database.getShards().size());
		} catch (Exception e) {
			log.fatal("Unable to connect to the database", e);
			return -4;
		}

		try {
			for (DataTable table : config.tables) {
				if (table.useDatabase) {
					log.info("Submitting SQL DB reader for " + table.sqlQuery);
					readerExecutor.submit(new SqlDataTableReader(config, table, writerExecutor));
				} else {
					switch (table.fileType) {
						case CSV:
							log.info("Submitting CSV file reader for " + table.fileNames);
							readerExecutor.submit(new CsvDataTableReader(config, table, writerExecutor));
							break;
						case JSON:
						case XML:
						default:
							log.fatal("Files of type " + table.fileType + " are not supported yet");
							return 3;
					}
				}
			}
		} catch (Exception e) {
			log.fatal("Unexpected exception", e);
			return -1;
		}

		try {
			//
			// Be careful with the order you shutdown the pools because you may shutdown the writer pool before the reader has finished adding items in
			log.info("All readers have been scheduled, waiting for completion");
			readerExecutor.shutdown();
			readerExecutor.awaitTermination(1, TimeUnit.DAYS);
			log.info("All readers have completed");

			log.info("Waiting for writers to complete");
			writerExecutor.shutdown();
			writerExecutor.awaitTermination(1, TimeUnit.DAYS);
			log.info("All writers have completed");
		} catch (InterruptedException e) {
		}

		log.info("App complete, shutting down");
		return 0;
	}

	public static void main(String[] args)
	{
		initLoggers();
		App app = new App();
		int configReturnCode = app.config(args);
		switch (configReturnCode) {
			case 0:
				// config worked, user accepted design
				System.exit(app.start());
				break;
			default:
				// config did NOT work, error out
				System.exit(configReturnCode);
				break;
		}
	}

	private static void initLoggers() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

		Logger rootLogger = Logger.getRootLogger();
		Enumeration<?> appenders = rootLogger.getAllAppenders();
		FileAppender fa = null;
		while (appenders.hasMoreElements())
		{
			Appender currAppender = (Appender) appenders.nextElement();
			if (currAppender instanceof FileAppender)
			{
				fa = (FileAppender) currAppender;
			}
		}
		if (fa != null)
		{
			fa.setFile("load-" + dateFormat.format(new Date()) + ".log");
			fa.activateOptions();
		}
	}
}
