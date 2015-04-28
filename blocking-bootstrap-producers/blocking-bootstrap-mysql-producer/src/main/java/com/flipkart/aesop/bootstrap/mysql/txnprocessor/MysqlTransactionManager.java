/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.bootstrap.mysql.txnprocessor;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;

import java.util.List;
import java.util.Map;

/**
 * <code>MysqlTransactionManager>/code> defines contracts specific for Mysql transactions. Inherits contracts from {@link TransactionProcessor} and {@link SourceProcessor} 
 */
public interface MysqlTransactionManager extends TransactionProcessor,SourceProcessor{

    /** Persists change events in event buffer */
	void performChanges(long tableId, BinlogEventV4Header eventHeader, List<Row> rowList, final DbusOpcode doc);

    /** Set the current bin log file number*/
	void setCurrFileNum(int currFileNum) ;

    /** Get the map of mysqlTableId to tableName mapping */
	Map<Long,String> getMysqlTableIdToTableNameMap();
	
	public void setShutdownRequested(boolean shutdownRequested);
}
