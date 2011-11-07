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

public class Constants {

  public static final String ORAHIVE_PRODUCT_NAME = "Quest® Data Transporter for Hive";
  public static final String ORAHIVE_JAR_FILENAME = "orahive.jar";
	
	public static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hadoop.hive.jdbc.HiveDriver";
	public static final String ORACLE_JDBC_DRIVER_CLASS = "oracle.jdbc.OracleDriver";
	
	public static enum OracleType {NUMBER, VARCHAR2};
//	public static final String ORACLE_DATA_TYPE_NUMBER = "NUMBER";
//	public static final String ORACLE_DATA_TYPE_VARCHAR2 = "VARCHAR2";
	
	public static final String CONF_HIVE_JDBC_URL = "hive";
	public static final String CONF_HIVE_JDBC_USER = "hiveuser";
	public static final String CONF_HIVE_JDBC_PASSWORD = "hivepassword";
	
	public static final String DEFAULT_LOCAL_HIVE_JDBC_URL = "jdbc:hive://localhost:10000/default";

	public static final String CONF_HIVE_QUERY = "hql";
	public static final String CONF_HIVE_QUERY_FILENAME = "hqlfile";
	
	public static final String CONF_ORACLE_JDBC_URL = "oracle";
	public static final String CONF_ORACLE_JDBC_USER = "oracleuser";
	public static final String CONF_ORACLE_JDBC_PASSWORD = "oraclepassword";
	
	public static final String CONF_ORACLE_SCHEMA = "oracleschema";
	public static final String CONF_ORACLE_TABLENAME = "oracletable";
	public static final String CONF_ORACLE_TABLESPACE = "oracletablespace";
	
	public static final String CONF_ORACLE_INSERT_BATCH_SIZE = "insertbatchsize";
	public static final int DEFAULT_ORACLE_INSERT_BATCH_SIZE = 500;
	
	public static final String CONF_ORACLE_INSERT_COMMIT_BATCH_COUNT = "commitbatchcount";
	public static final int DEFAULT_ORACLE_INSERT_COMMIT_BATCH_COUNT = 20;
	
	public static final String CONF_LOG4J_PROPERTIES_FILE = "log4j";
	
}
