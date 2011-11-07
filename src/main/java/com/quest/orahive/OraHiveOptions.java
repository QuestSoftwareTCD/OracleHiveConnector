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

import com.quest.orahive.Constants.ExportMode;

public class OraHiveOptions {

	public String hiveJdbcUrl;
	public String hiveUserName;
	public String hivePassword;

	public String oracleJdbcUrl;
	public String oracleUserName;
	public String oraclePassword;
	
	public String oracleSchema;
	public String oracleTable;
	public String oracleTablespace;

	public ExportMode exportMode;
	public String hql;
	
	public int insertBatchSize;
	public int commitBatchCount;
	
	public OraHiveOptions() {
		
		this.insertBatchSize = Constants.DEFAULT_ORACLE_INSERT_BATCH_SIZE;
		this.commitBatchCount = Constants.DEFAULT_ORACLE_INSERT_COMMIT_BATCH_COUNT;
		this.exportMode = Constants.DEFAULT_EXPORT_MODE;
	}
}
