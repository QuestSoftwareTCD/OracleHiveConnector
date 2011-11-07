/**
 *   Copyright 2011 Quest Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.quest.orahive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

//import com.quest.oraoop.OraOopLog;
//import com.quest.oraoop.OraOopLogFactory;
//import com.quest.oraoop.OraOopOracleQueries;


/* **************************************************************************************
 * NOTES:
 * 
 * Things you need to do if you add a new command-line option:
 * 	(1) Add a field to class: OraHiveOptions
 * 	(2) Update HiveJdbcClient.processGeneralOptions() to include the new option.
 * 	(3) Update HiveJdbcClient.buildGeneralOptions() to include the new option.
 * 	(4) Update HiveJdbcClient.checkConfiguration() if the new option is compulsory.
 * 	(5) Update HiveJdbcClient.getOraHiveOptions() to include the new option.
 * 
 ***************************************************************************************/

public class HiveJdbcClient {

	private static final Log /*OraOopLog*/ LOG = /*OraOop*/LogFactory.getLog(HiveJdbcClient.class);

	public static void main(String[] args) {
		
		long mainStartTime = System.nanoTime();
		
		Configuration conf = new Configuration();
		Options options = new Options();
		
		parseGeneralOptions(options, conf, args);	//<- log4j will now be configured.

		showWelcomeMessage();		
		
		if(args.length == 0 || userWantsToSeeHelp(args)) { 
			printCommandLineHelp(options);
			System.exit(0);
		}
		
		checkConfiguration(conf);
		
		OraHiveOptions opts = getOraHiveOptions(conf);
		OraHiveCounters counters = new OraHiveCounters();
		
    	try {		
    		Connection hiveConnection = createHiveJdbcConnection(opts.hiveJdbcUrl, opts.hiveUserName, opts.hivePassword);
    		try {
    			Connection oracleConnection = createOracleJdbcConnection(opts.oracleJdbcUrl, opts.oracleUserName, opts.oraclePassword);
    			try {
    	    		initializeOracleSession(oracleConnection, opts);
    	    		 
		    		Statement statement = hiveConnection.createStatement();
			    
				    LOG.info("Running: " + opts.hql);

				    // Execute Hive Query...
		    		long start = System.nanoTime();
		    		ResultSet hiveResultSet = statement.executeQuery(opts.hql);
		    		counters.hiveQueryTimeNanoSec = System.nanoTime() - start; 
		    		
		    		// Get column definitions from the Hive resultset...
				    List<OracleTableColumn> oracleColumns = getOracleTableColumnsForHiveResults(hiveResultSet);
				    
				    // Create an Oracle table based on the columns in the Hive resultset...
		    		createOracleTableWithRetry(opts, oracleColumns, oracleConnection);	//<- Lets the user retry this if it fails. 
				    
		    		// Generate the Oracle insert statement...
				    String insertSql = generateOracleInsertStatement(opts, oracleColumns);
				    
				    // Insert the Hive data into Oracle...
				    insertHiveResultsIntoOracleTable(opts, insertSql, oracleColumns, oracleConnection, hiveResultSet, counters);
				    
				    //hiveResultSet.close();	//<- Not required/supported
				    statement.close();
		    	}
		    	finally {
				    oracleConnection.close();
		    	}
	    	}
	    	finally {
			    hiveConnection.close();
	    	}
		    
		} 
    	catch (SQLException ex) {
    		LOG.error(String.format("An error occurred in %s."
    								,Constants.ORAHIVE_PRODUCT_NAME)
    				, ex);
    	}

    	LOG.info(String.format("\n\n********************************************************************\n"+
    							"\tTotal time                        : %s sec.\n"+
    							"\tNumber of records processed       : %s\n"+
    							"\tTime spent executing HQL statement: %s sec.\n"+ 
    							"\tTime spent fetching Hive data     : %s sec.\n"+
    							"\tTime spent inserting into Oracle  : %s sec."
    							,(System.nanoTime() - mainStartTime) / Math.pow(10,9)
    							,counters.rowsProcessed
    							,counters.hiveQueryTimeNanoSec / Math.pow(10,9)
    							,counters.hiveFetchTimeNanoSec / Math.pow(10,9)
    							,counters.oracleInsertTimeNanoSec / Math.pow(10,9)));
		
	  }
	
	private static void showWelcomeMessage() {

		String msg1 = String.format("Using %s %s"
									, Constants.ORAHIVE_PRODUCT_NAME
									, Utilities.getOraHiveVersion());
		String msg2 = "Copyright 2011 Quest Software, Inc.";
		String msg3 = "ALL RIGHTS RESERVED.";
		
		int longestMessage = Math.max(msg1.length(), msg2.length());
		
		msg1 = Utilities.padRight(msg1, longestMessage);
		msg2 = Utilities.padRight(msg2, longestMessage);
		msg3 = Utilities.padRight(msg3, longestMessage);
		
		char[] asterisks = new char[longestMessage + 8];
		Arrays.fill(asterisks, '*');
		
		String msg = String.format("\n"+
									"%1$s\n"+
									"*** %2$s ***\n" +
									"*** %3$s ***\n" +
									"*** %4$s ***\n" +
									"%1$s"
									,new String(asterisks)
									,msg1
									,msg2
									,msg3);
		LOG.info(msg);
	}	
	
	  private static String[] parseGeneralOptions(Options opts, Configuration conf, String[] args) {
		    
		  opts = buildGeneralOptions(opts);
		  CommandLineParser parser = new GnuParser();
		  try {
			  CommandLine commandLine = parser.parse(opts, args, true);
			  processGeneralOptions(conf, commandLine);
			  return commandLine.getArgs();
		  } 
		  catch(ParseException e) {
			  LOG.warn("options parsing failed: " + e.getMessage());

			  HelpFormatter formatter = new HelpFormatter();
			  formatter.printHelp("general options are: ", opts);
		  }
		  return args;
	  }
	  
	  private static void processGeneralOptions(Configuration conf, CommandLine line) {
		    
		  processGeneralOption(conf, line, Constants.CONF_HIVE_JDBC_URL);
		  processGeneralOption(conf, line, Constants.CONF_HIVE_JDBC_USER);
		  processGeneralOption(conf, line, Constants.CONF_HIVE_JDBC_PASSWORD);
		  
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_JDBC_URL);
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_JDBC_USER);
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_JDBC_PASSWORD);
		  
		  processGeneralOption(conf, line, Constants.CONF_HIVE_QUERY);
		  processGeneralOption(conf, line, Constants.CONF_HIVE_QUERY_FILENAME);
		  
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_SCHEMA);
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_TABLENAME);
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_TABLESPACE);
		  
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_INSERT_BATCH_SIZE);
		  processGeneralOption(conf, line, Constants.CONF_ORACLE_INSERT_COMMIT_BATCH_COUNT);
		  
			
		  if(line.hasOption(Constants.CONF_LOG4J_PROPERTIES_FILE)) {
			  String value = line.getOptionValue(Constants.CONF_LOG4J_PROPERTIES_FILE);
			  PropertyConfigurator.configure(value);
		  }
	  }	  
	  
	  private static void processGeneralOption(Configuration conf, CommandLine line, String optionName) {
		    
		  if (line.hasOption(optionName)) {
			  String value = line.getOptionValue(optionName);
		      conf.set(optionName, value);
		  }
	  }	 	  
	
	  @SuppressWarnings("static-access")
	  private static Options buildGeneralOptions(Options opts) {
		  
		// HIVE CONNECTION
		  
	    Option hiveJdbcUrl = OptionBuilder.withArgName("hive-jdbc-url")
	    .hasArg()
	    .withDescription("The JDBC URL of the Hive service")
	    .create(Constants.CONF_HIVE_JDBC_URL);

	    Option hiveJdbcUser = OptionBuilder.withArgName("hive-user")
	    .hasArg()
	    .withDescription("The Hive user name")
	    .create(Constants.CONF_HIVE_JDBC_USER);
	    
	    Option hiveJdbcPassword = OptionBuilder.withArgName("hive-password")
	    .hasArg()
	    .withDescription("The Hive password")
	    .create(Constants.CONF_HIVE_JDBC_PASSWORD);
	    
	    opts.addOption(hiveJdbcUrl);
	    opts.addOption(hiveJdbcUser);
	    opts.addOption(hiveJdbcPassword);
	    
	    // ORACLE CONNECTION 
	    
	    Option oracleJdbcUrl = OptionBuilder.withArgName("oracle-jdbc-url")
	    .hasArg()
	    .withDescription("The JDBC URL of the Oracle service")
	    .create(Constants.CONF_ORACLE_JDBC_URL);

	    Option oracleJdbcUser = OptionBuilder.withArgName("oracle-user")
	    .hasArg()
	    .withDescription("The oracle user name")
	    .create(Constants.CONF_ORACLE_JDBC_USER);
	    
	    Option oracleJdbcPassword = OptionBuilder.withArgName("oracle-password")
	    .hasArg()
	    .withDescription("The oracle password")
	    .create(Constants.CONF_ORACLE_JDBC_PASSWORD);
	    
	    opts.addOption(oracleJdbcUrl);
	    opts.addOption(oracleJdbcUser);
	    opts.addOption(oracleJdbcPassword);	    
	    
	    // HQL
	    
	    Option hql = OptionBuilder.withArgName("hql")
	    .hasArg()
	    .withDescription("The Hive Query to execute")
	    .create(Constants.CONF_HIVE_QUERY);

	    Option hqlFile = OptionBuilder.withArgName("hqlfile")
	    .hasArg()
	    .withDescription("The file containing the Hive Query to execute")
	    .create(Constants.CONF_HIVE_QUERY_FILENAME);
	    
	    opts.addOption(hql);
	    opts.addOption(hqlFile);

	    // ORACLE
	    
	    Option oracleTable = OptionBuilder.withArgName("oracle-table")
	    .hasArg()
	    .withDescription("The name of the Oracle to create")
	    .create(Constants.CONF_ORACLE_TABLENAME);

	    Option oracleSchema = OptionBuilder.withArgName("oracle-schema")
	    .hasArg()
	    .withDescription("The Oracle schema to create the table within")
	    .create(Constants.CONF_ORACLE_SCHEMA);
	    
	    Option oracleTablespace = OptionBuilder.withArgName("oracle-tablespace")
	    .hasArg()
	    .withDescription("The Oracle tablespace to create the table within")
	    .create(Constants.CONF_ORACLE_TABLESPACE);
	    
	    Option oracleInsertBatchSize = OptionBuilder.withArgName("oracle-insert-batch-size")
	    .hasArg()
	    .withDescription("The number of rows to batch-insert into the Oracle table at one time")
	    .create(Constants.CONF_ORACLE_INSERT_BATCH_SIZE);	    

	    Option oracleInsertCommitBatchCount = OptionBuilder.withArgName("oracle-insert-commit-batch-count")
	    .hasArg()
	    .withDescription("The number of batch-inserts to perform before performing an Oracle commit")
	    .create(Constants.CONF_ORACLE_INSERT_COMMIT_BATCH_COUNT);		    
	    
	    opts.addOption(oracleTable);
	    opts.addOption(oracleSchema);
	    opts.addOption(oracleTablespace);	
	    opts.addOption(oracleInsertBatchSize);
	    opts.addOption(oracleInsertCommitBatchCount);
	    
	    // OTHER
	    Option log4j = OptionBuilder.withArgName("log4j-properties-file")
	    .hasArg()
	    .withDescription("A log4j configuration file")
	    .create(Constants.CONF_LOG4J_PROPERTIES_FILE);		    
	    
	    opts.addOption(log4j);
	    
	    
	    return opts;
	  }	
	
	private static boolean userWantsToSeeHelp(String[] args) {
		
		boolean result = false;
		if(args.length > 0) {
			result = args[0].equals("?") || 
					args[0].equals("-?") ||
					args[0].equals("--?") ||
					args[0].equals("/?") ||
					args[0].equals("-h") ||
					args[0].equals("--h") ||
					args[0].equals("/h") ||
					args[0].toLowerCase().contains("help");
		}
		return result;
	}
	
	private static void printCommandLineHelp(Options options) {
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("general options are: ", options);
	}
	
	private static void checkConfiguration(Configuration conf) {
		
		checkConfigurationPropertyIsNotEmpty(conf, Constants.CONF_HIVE_JDBC_URL, Constants.DEFAULT_LOCAL_HIVE_JDBC_URL);
		checkConfigurationPropertyIsNotEmpty(conf, Constants.CONF_HIVE_JDBC_USER, null);
		
		checkConfigurationPropertyIsNotEmpty(conf, Constants.CONF_ORACLE_JDBC_URL, null);
		checkConfigurationPropertyIsNotEmpty(conf, Constants.CONF_ORACLE_JDBC_USER, null);
		
		checkConfigurationPropertyIsNotEmpty(conf, Constants.CONF_ORACLE_TABLENAME, null);
	}
	
	private static void checkConfigurationPropertyIsNotEmpty(Configuration conf, String propertyName, String defaultValue) {
		
		String value = conf.get(propertyName, defaultValue);
		if(value == null ||
			value.isEmpty()) {
			
			LOG.error(String.format("The \"%s\" option must be supplied.", propertyName));
			System.exit(1);
		}
	}
	
	private static OraHiveOptions getOraHiveOptions(Configuration conf) {
		
		OraHiveOptions result = new OraHiveOptions();

		result.hiveJdbcUrl = conf.get(Constants.CONF_HIVE_JDBC_URL);
		result.hiveUserName = conf.get(Constants.CONF_HIVE_JDBC_USER);
		result.oracleJdbcUrl = conf.get(Constants.CONF_ORACLE_JDBC_URL);
		result.oracleUserName = conf.get(Constants.CONF_ORACLE_JDBC_USER);

		result.oracleSchema = conf.get(Constants.CONF_ORACLE_SCHEMA, "");
		result.oracleTable = conf.get(Constants.CONF_ORACLE_TABLENAME, "");
		result.oracleTablespace = conf.get(Constants.CONF_ORACLE_TABLESPACE, "");
		
		result.insertBatchSize = conf.getInt(Constants.CONF_ORACLE_INSERT_BATCH_SIZE, Constants.DEFAULT_ORACLE_INSERT_BATCH_SIZE);
		result.commitBatchCount = conf.getInt(Constants.CONF_ORACLE_INSERT_COMMIT_BATCH_COUNT, Constants.DEFAULT_ORACLE_INSERT_COMMIT_BATCH_COUNT);
		
		result.hql = conf.get(Constants.CONF_HIVE_QUERY);
		if(result.hql == null || result.hql.trim().isEmpty()) {
			LOG.debug(String.format("No HQL was provided via the \"%s\" argument."
									,Constants.CONF_HIVE_QUERY));
			
			String hqlFileName = conf.get(Constants.CONF_HIVE_QUERY_FILENAME);
			if(hqlFileName == null || hqlFileName.isEmpty()) {
				LOG.error(String.format("A Hive HQL statement must be provided.\n" +
										"Please specify one of the following command-line arguments:\n"+
										"\t" + Constants.CONF_HIVE_QUERY + "\n"+
										"\t" + Constants.CONF_HIVE_QUERY_FILENAME));
				System.exit(1);
			}
			try {
				result.hql = getHql(hqlFileName);
			}
			catch(FileNotFoundException ex) {
				LOG.error(String.format("Unable to load the HQL file named \"%s\".", hqlFileName), ex);
				System.exit(1);
			}
			catch(IOException ex) {
				LOG.error(String.format("Unable to load the HQL file named \"%s\".", hqlFileName), ex);
				System.exit(1);
			}			
		}
		
		result.hivePassword = getPassword(conf
				, Constants.CONF_HIVE_JDBC_PASSWORD
				, String.format("Enter the password for the Hive Service at %s :"
								, conf.get(Constants.CONF_HIVE_JDBC_URL)));
		
		result.oraclePassword = getPassword(conf
				, Constants.CONF_ORACLE_JDBC_PASSWORD
				, String.format("Enter the password for the Oracle database at %s :"
								, conf.get(Constants.CONF_ORACLE_JDBC_URL)));


		return result;
	}
	
	private static String getHql(String hqlFileName) 
		throws FileNotFoundException, IOException {
		
		return Utilities.readTextFile(hqlFileName);
	}
	
	private static String getPassword(Configuration conf, String propertyName, String userPromptText) {
		
		String result = conf.get(propertyName);
		if(result == null) {
			System.out.println(userPromptText);
			try { 
				result = Utilities.readLineFromStdIn(); 
			} 
			catch (IOException ex) { 
				ex.printStackTrace();
			} 			
		}
		return result;
	}

    private static Connection createHiveJdbcConnection(String url, String userName, String password) { 
    	
		try {
			Class.forName(Constants.HIVE_JDBC_DRIVER_CLASS);
	    } 
		catch (ClassNotFoundException ex) {
	      LOG.error(String.format("Unable to load the Hive JDBC driver \"%s\"."
	    		  				, Constants.HIVE_JDBC_DRIVER_CLASS)
	    		  	,ex);
	      System.exit(1);
	    }
		
    	try {
    		 return DriverManager.getConnection(url, userName, password);
    	}
    	catch(SQLException ex) {
    		LOG.fatal(String.format("Unable to connect to Hive via the JDBC URL \"%s\" as user \"%s\"."
    								,url
    								,userName)
    				, ex);
    		System.exit(1);
    	}   	
    	return null;
    }
    
    private static Connection createOracleJdbcConnection(String url, String userName, String password) { 
    	
		try {
			Class.forName(Constants.ORACLE_JDBC_DRIVER_CLASS);
	    } 
		catch (ClassNotFoundException ex) {
	      LOG.error(String.format("Unable to load the Oracle JDBC driver \"%s\"."
	    		  				, Constants.ORACLE_JDBC_DRIVER_CLASS)
	    		  	,ex);
	      System.exit(1);
	    }
		
    	try {
    		 return DriverManager.getConnection(url, userName, password);
    	}
    	catch(SQLException ex) {
    		LOG.error(String.format("Unable to connect to Oracle via the JDBC URL \"%s\" as user \"%s\"."
    								,url
    								,userName)
    				, ex);
    		System.exit(1);
    	}   	
    	return null;
    }    
    
    private static void initializeOracleSession(Connection connection, OraHiveOptions opts) {
    	
   		String sql = "";
   		try {
   			sql = "begin \n" +
   				 "  dbms_application_info.set_module(module_name => '%s', action_name => '%s'); \n" +
   				 "end;";
    			
   			sql = String.format(sql
   								, Constants.ORAHIVE_PRODUCT_NAME
   								, getOracleTableName(opts));
    			
   			Statement statement = connection.createStatement();
   			statement.execute(sql);
   			statement.close();
   		} 
   		catch(Exception ex) {
   			LOG.error(String.format("An error occurred while attempting to execute "+
   						 	"the following Oracle session-initialization statement:"+
   						 	"\n%s"+
   						 	"\nError:"+
   						 	"\n%s"
   						 	,sql
   						 	,ex.getMessage()));
   		}		
    }

    private static List<OracleTableColumn> getOracleTableColumnsForHiveResults(ResultSet resultSet) {
    	
    	List<OracleTableColumn> result = null;
    	
    	try {
	    	ResultSetMetaData metaData = resultSet.getMetaData();
	    	
	    	result = new ArrayList<OracleTableColumn>(metaData.getColumnCount());
	    	
	    	for(int idx = 0; idx < metaData.getColumnCount(); idx++) {
	    		OracleTableColumn column = new OracleTableColumn();
	    		result.add(column);
	    		
	    		// column Name...
	    		column.setName(metaData.getColumnLabel(idx+1));	//<- 1-based in JDBC;
	    		
	    		// Sql Type...
	    		column.sqlType = metaData.getColumnType(idx+1);	//<- 1-based in JDBC
	    		
	    		// column Oracle data-type...
	    		
	    		Constants.OracleType oracleType = javaSqlTypeToOracleType(column.sqlType);
	    		
	    		switch(oracleType) {
	    		
	    			case VARCHAR2: {
	    				
	    				column.oracleDataType = String.format("%s(%d)"
	    													, oracleType.toString()
	    													, 4000 // Max length for a varchar
	    													);
	    				break;
	    			}
	    			
	    			default: {
	    				column.oracleDataType = oracleType.toString();
	    				break;
	    			}    			
	    				
	    		}
	    	}
	    	
    	}
    	catch(SQLException ex) {
    		LOG.error("An error occurred when processing the metadata for the Hive result-set.", ex);
    		System.exit(1);
    	}   	
    	
    	return result;
    }
    
    private static Constants.OracleType javaSqlTypeToOracleType(int javaSqlType) {

		switch(javaSqlType) {
		
			case java.sql.Types.VARCHAR: return Constants.OracleType.VARCHAR2;
			
			case java.sql.Types.BOOLEAN: return Constants.OracleType.NUMBER; // Constants.OracleType.VARCHAR2;
			
			case java.sql.Types.BIGINT:
			case java.sql.Types.DECIMAL:
			case java.sql.Types.DOUBLE:
			case java.sql.Types.FLOAT:
			case java.sql.Types.INTEGER: 
			case java.sql.Types.NUMERIC:
			case java.sql.Types.REAL:
			case java.sql.Types.SMALLINT: return Constants.OracleType.NUMBER;
			
			default:
				throw new RuntimeException(String.format("Unsupported SQL type: %d.", javaSqlType));
				
		}
    	
    }
    
    private static void createOracleTableWithRetry(OraHiveOptions opts, List<OracleTableColumn> oracleColumns, Connection oracleConnection) { 
    	
    	try {
	
	    	Statement statement = oracleConnection.createStatement();
	    	
	    	StringBuilder columnClause = new StringBuilder();
	    	for(int idx = 0; idx < oracleColumns.size(); idx++) {
	    		OracleTableColumn column = oracleColumns.get(idx);
	    		if(idx > 0)
	    			columnClause.append(", ");
	    		columnClause.append(String.format("%s %s"
	    							,column.getName()
	    							,column.oracleDataType));
	    	}
	    	
	    	String sql = String.format("CREATE TABLE %s (%s)"
	    							,getOracleTableName(opts)
	    							,columnClause.toString());
	    	
	    	sql += getOracleTablespaceClause(opts);
	    	
	    	LOG.info(String.format("Executing SQL: %s", sql));
	    	
    		statement.execute(sql);
	    	statement.close();
    	}
	    catch(SQLException ex) {
	    	LOG.error("Unable to create an Oracle table to store the results of the Hive query.", ex);
	    	
	    	System.out.println(String.format("\nWould you like to retry creating the Oracle table \"%s\"?\n"+
	    									"(y/n)"
	    									,getOracleTableName(opts)));
	    	
	    	try {
	    		if(Utilities.readYNFromStdIn())
	    			// Recurse...
	    			createOracleTableWithRetry(opts, oracleColumns, oracleConnection);
	    		else
	    			System.exit(1);
	    	}
	    	catch(IOException e) {
	    		LOG.error(e.getMessage());
	    		System.exit(1);
	    	}
	    }   

    }
    
    private static String getOracleTableName(OraHiveOptions opts) {
    	
    	String result = opts.oracleTable;
    	if(!opts.oracleSchema.isEmpty())
    		result = opts.oracleSchema + "." + result;
    	return result;
    }
    
    private static String getOracleTablespaceClause(OraHiveOptions opts) {
    	
    	String result = "";
    	if(!opts.oracleTablespace.isEmpty())
    		result = String.format(" TABLESPACE %s ", opts.oracleTablespace);
    	return result;
    }    
    
    private static String generateOracleInsertStatement(OraHiveOptions opts, List<OracleTableColumn> oracleColumns) {
    
    	StringBuilder result = new StringBuilder();
    	
    	result.append(String.format("INSERT INTO %s\n", getOracleTableName(opts)));
    	
    	for(int idx = 0; idx < oracleColumns.size(); idx++) {
    		if(idx == 0)
    			result.append("(");
    		else
    			result.append(",");
    		result.append(oracleColumns.get(idx).getName());
    	}
    	result.append(")\n");
    	result.append("VALUES\n");
    	for(int idx = 0; idx < oracleColumns.size(); idx++) {
    		if(idx == 0)
    			result.append("(");
    		else
    			result.append(",");
    		result.append("?");
    	}    	
    	result.append(")\n");
    	
    	LOG.info(String.format("INSERT SQL:\n%s", result.toString()));
    	
    	return result.toString();
    }    
    
    private static void insertHiveResultsIntoOracleTable(OraHiveOptions opts
    													, String insertSql
    													, List<OracleTableColumn> oracleColumns
    													, Connection oracleConnection
    													, ResultSet resultSet
    													, OraHiveCounters counters) {
    
		long timerHiveFetching = 0;
		long timerOracleInserting = 0;  
		long rowsProcessed = 0;
    	
    	try {
	    	
	    	oracle.jdbc.OraclePreparedStatement statement = (oracle.jdbc.OraclePreparedStatement)oracleConnection.prepareStatement(insertSql);
	    
	    	int rowIdx = 0;
	    	int batchIdx = 0;
	    	int numberOfBatchesCommitted = 0;
	    	
      try
      {
        resultSet.setFetchSize(opts.insertBatchSize);
      }
      catch(SQLException e)
      {
        try
        {
          // Apply fetchN hack for much better performance with pre 0.8 JDBC driver
          LOG.info("Hive ResultSet does not implement setFetchSize. Wrapping with FetchNResultSet for better performance.");
          resultSet = new FetchNResultSet(resultSet);
          resultSet.setFetchSize(opts.insertBatchSize);
        }
        catch(IllegalArgumentException iae)
        {
          LOG.warn("Wrapping Hive ResultSet with FetchNResultSet failed. Performance may be poor for large result sets.");
          LOG.debug("FetchNResultSet exception was:", iae);
        }
      }
      long start = System.nanoTime();
      while (resultSet.next())
      {
        for (int idx = 0; idx < oracleColumns.size(); idx++)
        { // <- JDBC is 1-based
          statement.setObject(idx + 1, resultSet.getObject(idx + 1));
        }
        timerHiveFetching += System.nanoTime() - start;

        rowsProcessed++;
        statement.addBatch();

        rowIdx++;
        if (rowIdx == opts.insertBatchSize)
        {
          rowIdx = 0;

          start = System.nanoTime();

          // executeBatchWithRetry(statement, oracleConnection);
          statement.executeBatch();
          statement.clearBatch();

          timerOracleInserting += System.nanoTime() - start;

          batchIdx++;
        }

        if (batchIdx == opts.commitBatchCount)
        {
          batchIdx = 0;
          oracleConnection.commit();
          numberOfBatchesCommitted++;
          LOG.info(String.format("Number of rows inserted so far: %d",
                                 numberOfBatchesCommitted * (opts.insertBatchSize * opts.commitBatchCount)));
        }
        start = System.nanoTime();
      }
	    	
    		if(rowIdx > 0) {
        start = System.nanoTime();
	    		
	    		//executeBatchWithRetry(statement, oracleConnection);
	    		statement.executeBatch();
	    		
	    		timerOracleInserting += System.nanoTime() - start;	    			
    		}	    	
	    	
    		oracleConnection.commit();
    		
	    	statement.close();
    	}
    	catch(SQLException ex) {
    		
			if(Utilities.oracleSessionHasBeenKilled(ex)) {
				LOG.info("\n*********************************************************"+
						 "\nThe Oracle session in use has been killed by a 3rd party."+
                         "\n*********************************************************");
			}
			else
				LOG.error("An error occurred within the process of fetching Hive results "+
				          "and inserting them into an Oracle table. (1)", ex);
    		
    		try {
    			oracleConnection.rollback();
    		}
    		catch(SQLException e) {}	
    		
    		System.exit(1);
    	}
    	catch(Exception ex) {
            LOG.error("An error occurred within the process of fetching Hive results "+
                      "and inserting them into an Oracle table. (2)", ex);
    	}
    	finally {
    	    LOG.info(String.format("Number of rows obtained from Hive: %d"
    	                          ,rowsProcessed));
    	}
    	
    	counters.rowsProcessed = rowsProcessed;
    	counters.hiveFetchTimeNanoSec = timerHiveFetching;
    	counters.oracleInsertTimeNanoSec = timerOracleInserting;
    }
    
    
/*
 * HiveResultSet does not support the relative() method - so there's no way to retry a failed batch insert.
 * i.e. There's no way to rollback the current transaction, *reposition resultSet* and try again.    
 */
//    private static void executeBatchWithRetry(oracle.jdbc.OraclePreparedStatement statement, Connection oracleConnection) {
//    	
//    	try {
//    		statement.executeBatch();
//    	}
//    	catch(SQLException ex) {
//    		
//    		LOG.error("An error occurred inserting a batch of rows into the Oracle table.", ex);
//	    	
//	    	System.out.println("Would you like to reattempt the insertion of this batch of rows? (y/n)");
//	    	try {
//	    		if(Utilities.readYNFromStdIn())
//	    			// Recurse...
//	    			executeBatchWithRetry(statement, oracleConnection);
//	    		else {
//	        		try {
//	        			oracleConnection.rollback();
//	        		}
//	        		catch(SQLException e) {}	    			
//	    			System.exit(1);
//	    		}
//	    	}
//	    	catch(IOException e) {
//	    		LOG.error(e.getMessage());
//	    		System.exit(1);
//	    	}
//    	}  
//    }
    
}
