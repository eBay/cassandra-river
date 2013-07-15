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

import static org.elasticsearch.client.Requests.indexRequest;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

public class CassandraRiver extends AbstractRiverComponent implements River {

    private final Client client;
    
    private ExecutorService threadExecutor;
    private volatile boolean closed;
    
    //Cassandra settings
    private final String hosts;
    private final String username;
    private final String password;
    
	private final String clusterName;
	private final String keyspace;
	private final String columnFamily;
	private final int batchSize;
	
	//Index settings
	private final String typeName;
	private final String indexName;
    static final String DEFAULT_UNIQUE_KEY = "id";


    @Inject
    protected CassandraRiver(RiverName riverName, RiverSettings riverSettings, Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.client = client;
        
        if (riverSettings.settings().containsKey("cassandra")) {
            @SuppressWarnings("unchecked")
			Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("cassandra");
            this.clusterName = XContentMapValues.nodeStringValue(couchSettings.get("cluster_name"), "DEFAULT_CLUSTER");
            this.keyspace = XContentMapValues.nodeStringValue(couchSettings.get("keyspace"), "DEFAULT_KS");
            this.columnFamily = XContentMapValues.nodeStringValue(couchSettings.get("column_family"), "DEFAULT_CF");
            this.batchSize = XContentMapValues.nodeIntegerValue(couchSettings.get("batch_size"), 1000);
            this.hosts = XContentMapValues.nodeStringValue(couchSettings.get("hosts"), "host1:9161,host2:9161");
            this.username = XContentMapValues.nodeStringValue(couchSettings.get("username"), "USERNAME");
            this.password = XContentMapValues.nodeStringValue(couchSettings.get("password"), "P$$WD");
        } else {
        	/*
        	 * Set default values
        	 */
        	this.clusterName = "DEFAULT_CLUSTER";
        	this.keyspace = "DEFAULT_KS";
        	this.columnFamily = "DEFAULT_CF";
        	this.batchSize = 1000;
            this.hosts = "host1:9161,host2:9161";
            this.username = "USERNAME";
            this.password = "P$$WD";
        }
        
        if (riverSettings.settings().containsKey("index")) {
            @SuppressWarnings("unchecked")
			Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("index");
            this.indexName = XContentMapValues.nodeStringValue(couchSettings.get("index"), "DEFAULT_INDEX_NAME");
            this.typeName = XContentMapValues.nodeStringValue(couchSettings.get("type"), "DEFAULT_TYPE_NAME");
            
        } else {
        	this.indexName = "DEFAULT_INDEX_NAME";
        	this.typeName = "DEFAULT_TYPE_NAME";
        }
    }


    @Override
    public void start() {
    	ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder().setNameFormat("Queue-Indexer-thread-%d").setDaemon(false).build();
    	threadExecutor = Executors.newFixedThreadPool(10, daemonThreadFactory);
        
    	logger.info("Starting cassandra river");
		CassandraDB db = CassandraDB.getInstance(this.hosts, this.username, this.password, this.clusterName, this.keyspace);
        String start = "";
    	while(true){
            if (closed) {
                return;
            }
            CassandraCFData cassandraData = db.getCFData(columnFamily, start, 1000);
    		start = cassandraData.start;
    		threadExecutor.execute(new Indexer(this.batchSize, 
							    				this.typeName,
							    				this.indexName,
							    				cassandraData));	
    	}
    }


    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing cassandra river");
        closed = true;
        threadExecutor.shutdownNow();
    }
    
    private class Indexer implements Runnable {
    	private final int batchSize;
    	private final CassandraCFData keys;
    	private final String typeName;
    	private final String indexName;
    	
    	public Indexer(int batchSize, String typeName, String indexName, CassandraCFData keys){
        	this.batchSize = batchSize;
        	this.typeName = typeName;
        	this.indexName = indexName;
        	this.keys = keys;
    	}
    	
		@Override
		public void run() {
			logger.info("Starting thread with {} keys", this.keys.rowColumnMap.size());
                if (closed) {
                    return;
                }
                
                BulkRequestBuilder bulk = client.prepareBulk();
                for(String key : this.keys.rowColumnMap.keySet()){
                	
    				try {
    					String id = UUID.nameUUIDFromBytes(key.getBytes()).toString();
    					bulk.add(indexRequest(this.indexName).type(this.typeName)
							    							 .id(id)
							    							 .source(this.keys.rowColumnMap.get(key)));
    				} catch (Exception e) {
    					logger.error("failed to entry to bulk indexing");
    				}
    				
                	if(bulk.numberOfActions() >= this.batchSize){
                		saveToEs(bulk);
    					bulk = client.prepareBulk();
                	}
                }
		}

		/*
		 * Persists data to elastic search
		 */
		private boolean saveToEs(BulkRequestBuilder bulk) {
			logger.info("Inserting {} keys in ES", bulk.numberOfActions());

			try {
				bulk.execute().addListener(new Runnable() {
					@Override
					public void run() {
						logger.info("Processing done!");
					}
				});
			} catch (Exception e) {
				logger.error("failed to execute bulk", e);
				return false;
			}
			
			return true;
		}
    }
}