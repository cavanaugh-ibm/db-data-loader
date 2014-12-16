package com.cloudant.se.db.loader.read;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.cloudant.se.db.loader.config.AppConfig;
import com.cloudant.se.db.loader.config.DataTable;

public class SqlDataTableReader extends ADataTableReader {
	private static final Logger	log	= Logger.getLogger(SqlDataTableReader.class);

	public SqlDataTableReader(AppConfig config, DataTable table, ExecutorService writerExecutor) {
		super(config, table, writerExecutor);
	}

	@Override
	public Integer call() throws Exception {
		log.info("Sql reader starting file " + table.sqlQuery);

		try {
			Class.forName(table.sqlDriver);
		} catch (Exception e) {
			log.fatal("Unable to find driver specified - " + table.sqlDriver);
			return -1;
		}

		Connection conn = null;
		Statement stmt = null;

		try {
			conn = DriverManager.getConnection(table.sqlUrl, table.sqlUser, "<BLANK>".equalsIgnoreCase(table.sqlPass) ? "" : table.sqlPass);
			stmt = conn.createStatement();

			ResultSet rs = stmt.executeQuery(table.sqlQuery);
			ResultSetMetaData rsmd = rs.getMetaData();

			while (rs.next()) {
				//
				// Reset our state for the current row
				reset();

				int numColumns = rsmd.getColumnCount();
				for (int i = 1; i <= numColumns; i++) {
					addField(rsmd.getColumnName(i), rs.getString(i));
				}
				recordComplete();
			}
		} catch (SQLException e) {
			log.error("Error processing query", e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e2) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e2) {
				}
			}
		}

		log.info("Sql reader completed for " + table.fileNames + " - " + processed);
		return processed;
	}
}
