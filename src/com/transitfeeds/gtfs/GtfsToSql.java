package com.transitfeeds.gtfs;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;


public class GtfsToSql {

	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption("g", true, "GTFS Path");
		options.addOption("s", true, "JDBC Connection");
        options.addOption("e", true, "File(s) to exclude");
        options.addOption("o", false, "Run optimizer");
		options.addOption("dbusername", true, "Database username");
		options.addOption("dbpassword", true, "Database password");

		CommandLineParser parser = new GnuParser();
		CommandLine line = parser.parse(options, args);

		if (!line.hasOption("g")) {
			System.out.println("GTFS path must be specified");
			showHelp(options);
			System.exit(1);
		}
		
		if (!line.hasOption("s")) {
			System.err.println("JDBC path must be specified, examples:");
			System.err.println("\tPostgreSQL: jdbc:postgresql://localhost/dbname");
			System.err.println("\tSqlite:     jdbc:sqlite:/path/to/db.sqlite");
			showHelp(options);
			System.exit(2);
		}

		String gtfsPath = line.getOptionValue("g");
		File gtfsFile = new File(gtfsPath);

		String connStr = line.getOptionValue("s");
		
		if (connStr.startsWith("jdbc:sqlite:")) {
			// may not work without this call
			Class.forName("org.sqlite.JDBC");
		}
		
		Connection connection = DriverManager.getConnection(connStr, line.getOptionValue("dbusername"), line.getOptionValue("dbpassword"));

		GtfsParser gtfs = new GtfsParser(gtfsFile, connection);
		
		String[] exclude = line.getOptionValues("e");
		
		if (exclude != null) {		
			for (int i = 0; i < exclude.length; i++) {
				gtfs.exclude(exclude[i]);
			}
		}
		
		gtfs.parse();
		if (line.hasOption("o")) {
		    GtfsOptimizer optimizer = new GtfsOptimizer(connection);
		    optimizer.optimize();
        }
	}

	public static void showHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("GtfsToSql", options);
	}

}
