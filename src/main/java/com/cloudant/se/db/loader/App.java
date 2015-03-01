package com.cloudant.se.db.loader;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.model.ConnectOptions;
import com.cloudant.se.concurrent.StatusingNotifyingBlockingThreadPoolExecutor;
import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.read.CsvDataTableReader;
import com.cloudant.se.db.loader.read.SqlDataTableReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Things to consider in this - Should we use a pool for objects to speed up?
 *
 * @author Cavanaugh
 *
 */
public class App {
    private static final Logger log            = Logger.getLogger(App.class); ;

    protected AppConfig         config         = null;
    protected AppOptions        options        = null;

    protected ExecutorService   readerExecutor = null;
    protected ExecutorService   writerExecutor = null;

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
        // Enable messages as we were asked
        setScreenLogging();
        setFileLogging();

        //
        // Read the config they gave us
        try {
            File configFile = new File(options.configFileName);
            if (!configFile.exists()) {
                log.fatal("Unable to find configuration file - " + configFile.getAbsolutePath());
                return -5;
            } else if (!configFile.canRead()) {
                log.fatal("Unable to read configuration file - " + configFile.getAbsolutePath());
                return -6;
            }

            //
            // Validate the schema first
            final JsonNode appConfigSchema = JsonLoader.fromResource("/AppConfig.schema.json");
            final JsonNode configJson = JsonLoader.fromFile(configFile);
            final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
            final JsonSchema schema = factory.getJsonSchema(appConfigSchema);

            ProcessingReport report = schema.validate(configJson);
            if (report.isSuccess()) {
                //
                // Read the configuration from our file and let it validate itself
                ObjectMapper mapper = new ObjectMapper();
                config = mapper.readValue(configFile, AppConfig.class);
                config.setDefaultDirectory(configFile.getParentFile() != null ? configFile.getParentFile() : new File("."));
                config.mergeOptions(options);
                config.validate();
            } else {
                log.fatal(report);
                return -4;
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Configuration error detected - " + e.getMessage());
            config = null;
            return -2;
        } catch (UnrecognizedPropertyException e) {
            System.err.println("Configuration error detected - unrecognized parameter - typo? - [" + e.getPropertyName() + "][" + e.getLocation() + "]");
            config = null;
            return -3;
        } catch (Exception e) {
            System.err.println("Unexpected exception - see log for details - " + e.getMessage());
            log.error(e.getMessage(), e);
            return -1;
        }
        //
        // Setup our executor service
        readerExecutor = Executors.newFixedThreadPool(config.getTables().size(), new ThreadFactoryBuilder().setNameFormat("ldr-r-%d").build());

        int threads = config.getNumThreads();
        ThreadFactory writeThreadFactory = new ThreadFactoryBuilder().setNameFormat("ldr-w-%d").build();
        writerExecutor = new StatusingNotifyingBlockingThreadPoolExecutor(threads, threads * 2, 30, TimeUnit.SECONDS, writeThreadFactory);

        return 0;
    }

    private void setScreenLogging() {
        Logger logger = Logger.getRootLogger();
        AppenderSkeleton appender = (AppenderSkeleton) logger.getAppender("stdout");

        if (appender != null) {
            if (options.trace) {
                appender.setThreshold(Level.TRACE);
            } else if (options.debug) {
                appender.setThreshold(Level.DEBUG);
            } else if (options.verbose) {
                appender.setThreshold(Level.INFO);
            }
        }
    }

    private void setFileLogging() {
        Logger.getRootLogger().removeAppender("file");
        ;

        String fileName = null;
        Level newLevel = Level.WARN;
        boolean addLog = false;

        //
        // If they give us a log file, log INFO at a minimum
        if (StringUtils.isNotBlank(options.logFileName)) {
            fileName = options.logFileName;
            newLevel = Level.INFO;
            addLog = true;
        } else {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            fileName = "load-" + dateFormat.format(new Date()) + ".log";
        }

        if (options.traceLog) {
            newLevel = Level.TRACE;
            addLog = true;
        } else if (options.debugLog) {
            newLevel = Level.DEBUG;
            addLog = true;
        } else if (options.verboseLog) {
            newLevel = Level.INFO;
            addLog = true;
        }

        if (addLog) {
            FileAppender fa = new FileAppender();
            fa.setFile(fileName);
            fa.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p [%10.10t] %30.30c{1} - %m%n"));
            fa.setThreshold(newLevel);
            fa.activateOptions();
            Logger.getRootLogger().addAppender(fa);
        }
    }

    private void showUsage(JCommander jCommander) {
        jCommander.usage();
    }

    private int start() {
        log.info("Configuration complete, starting up");
        try {
            ConnectOptions options = new ConnectOptions();
            options.setMaxConnections(config.getNumThreads());
            options.setSocketTimeout(config.getSocketTimeout());
            options.setConnectionTimeout(config.getConnectionTimeout());

            config.client = new CloudantClient(config.getCloudantAccount(), config.getCloudantUser(), config.getCloudantPassword(), options);
            config.database = config.client.database(config.getCloudantDatabase(), false);
            log.info(" --- Connected to Cloudant --- ");
            // log.debug("Available databases - " + config.client.getAllDbs());
            // log.debug("Database shards - " + config.database.getShards().size());
        } catch (Exception e) {
            log.fatal("Unable to connect to the database", e);
            return -4;
        }

        try {
            for (DataTable table : config.getTables()) {
                if (table.isUseDatabase()) {
                    log.info("Submitting SQL DB reader for " + table.getSqlQuery());
                    readerExecutor.submit(new SqlDataTableReader(config, table, writerExecutor));
                } else {
                    switch (table.getFileType()) {
                        case CSV:
                            log.info("Submitting CSV file reader for " + table.getFileNames());
                            readerExecutor.submit(new CsvDataTableReader(config, table, writerExecutor));
                            break;
                        case JSON:
                        case XML:
                        default:
                            log.fatal("Files of type " + table.getFileType() + " are not supported yet");
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
