/*Copyright 2013, eBay Software Foundation
   Authored by Utkarsh Sengar
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package org.elasticsearch.river.cassandra;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;


public class CassandraDB {
	private static final StringSerializer STR = StringSerializer.get();
	
	public static final String COMPOSITE_SEPARATOR = "~";

	private Cluster cluster;
	private Keyspace keyspace;
	private int batchSize;

	private static HashMap<String, CassandraDB> instances = new HashMap<String, CassandraDB>();

	private CassandraDB(String hosts, String username, String password, String clustername, String keyspace) {
		init(clustername, hosts, username, password, keyspace);
	}

	protected void init(String clustername, String hosts, String username, String password, String keyspace) {
		CassandraHostConfigurator hostconfig = new CassandraHostConfigurator(hosts);
		hostconfig.setRetryDownedHosts(true);
		hostconfig.setRetryDownedHostsDelayInSeconds(5);
		hostconfig.setRetryDownedHostsQueueSize(-1); // no bounds		
		this.cluster = HFactory.getOrCreateCluster(clustername, hostconfig);
		
		Map<String,String> credentials = new HashMap<String, String>();
		if (username != null && username.length() > 0) {
			credentials.put("username", username);
			credentials.put("password", password);
		}
		
		this.keyspace = HFactory.createKeyspace(
				keyspace, 
				cluster, 
				new AllOneConsistencyLevelPolicy(), 
				FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);
	}

	public static CassandraDB getInstance(String hosts, String username, String password, String clustername, String keyspace) {
		String instanceKey = clustername + "|" + keyspace; // TODO A cleaner key would be nice
		CassandraDB instance = null;

		synchronized (CassandraDB.class) {
			instance = instances.get(instanceKey);
			if (instance == null) {
				instance = new CassandraDB(hosts, username, password, clustername, keyspace);
				instances.put(instanceKey, instance);
			}
		}

		return instance;
	}

	public int getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public CassandraCFData getCFData(String columnFamily, String start, int limit) {
		int columnLimit = 100;
		CassandraCFData data = new CassandraCFData();
		String lastEnd = null;
		
		Map<String, Map<String, String>> cfData = new HashMap<String, Map<String, String>>();
		RangeSlicesQuery<String, String, String> query = HFactory.createRangeSlicesQuery(keyspace, STR, STR, STR);
		query.setColumnFamily(columnFamily);
		query.setKeys(start, "");
		query.setRange("", "", false, columnLimit);
		query.setRowCount(limit);
		OrderedRows<String, String, String> rows = query.execute().get();
		if (rows.getCount() != 1) {
			lastEnd = rows.peekLast().getKey();
			data.start = lastEnd;
		} else {
			data.start = null;
			return data;
		}
		
		for(Row<String,String,String> row  : rows.getList()){
			Map<String, String> columnMap = new HashMap<String, String>();
			ColumnSlice<String, String> columnData = row.getColumnSlice();
			for (HColumn<String, String> column : columnData.getColumns()){
				columnMap.put(column.getName(), column.getValue());
			}
			
			cfData.put(row.getKey(), columnMap);
		}
		
		data.rowColumnMap = cfData;
		return data;
	}
}
