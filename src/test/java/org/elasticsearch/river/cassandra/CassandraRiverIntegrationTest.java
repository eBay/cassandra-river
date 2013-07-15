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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CassandraRiverIntegrationTest {
	
	Logger logger = LoggerFactory.getLogger(CassandraRiverIntegrationTest.class);
    private Node esNode;
    private Client esClient;
    
    private static final File DATA_DIR;
    private static final File ES_DATA_DIR;
    private static final boolean ES_HTTP_ENABLED = true;

    static {
        String tmpDir = System.getProperty("tempDir", System.getProperty("java.io.tmpdir"));
        if (tmpDir == null) {
            throw new RuntimeException("No system property 'tempDir' or 'java.io.tmpdir' defined");
        }
        DATA_DIR = new File(tmpDir, "test-" + CassandraRiverIntegrationTest.class.getSimpleName() + "-" + System.currentTimeMillis());
        ES_DATA_DIR = new File(DATA_DIR, "elasticsearch");
    }

    @BeforeClass
    public void beforeClass() throws Exception {
        //fires elasticsearch node
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.data", ES_DATA_DIR.getAbsolutePath())
                .put("http.enabled", ES_HTTP_ENABLED)
                .build();
        esNode = NodeBuilder.nodeBuilder().clusterName("cassandra-river-test").local(true).settings(settings).build();
        esNode.start();
        esClient = esNode.client();
    }

    @AfterClass
    public void afterClass() throws Exception {
        esClient.close();
        esNode.close();
    }

    @BeforeMethod
    public void beforeMethod() throws IOException {
        //removes data from elasticsearch (both registered river if existing and imported data)
        String[] indices = esClient.admin().cluster().state(new ClusterStateRequest().local(true))
                .actionGet().getState().getMetaData().getConcreteAllIndices();
        esClient.admin().indices().prepareDelete(indices).execute().actionGet();
    }

    @Test
    public void testImportDefaultValues() throws Exception {
        registerRiver();
    }

    @Test
    public void testImportWithRows() throws Exception {
        registerRiver(ImmutableMap.of("rows", 20), null);
    }

    private void registerRiver() throws Exception {
        registerRiver(null, null, null);
    }

    private void registerRiver(Map<String, ? extends Object> solrConfig,
                               Map<String, ? extends Object> indexConfig) throws Exception {
        registerRiver(solrConfig, indexConfig, null);
    }

    private void registerRiver(Map<String, ? extends Object> solrConfig,
                               Map<String, ? extends Object> indexConfig,
                               Map<String, ? extends Object> mainConfig) throws Exception {

        registerRiver(solrConfig, indexConfig, mainConfig, null);
    }

    private void registerRiver(Map<String, ? extends Object> solrConfig,
                               Map<String, ? extends Object> indexConfig,
                               Map<String, ? extends Object> mainConfig,
                               Map<String, ? extends Object> transformConfig) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().startObject();
        builder.field("type", "cassandra");

        if (mainConfig != null) {
            for (Map.Entry<String, ? extends Object> entry : mainConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        builder.startObject("cassandra");
        builder.field("url", "http://localhost:8983/cassandra-river/");
        if (solrConfig != null) {
            for (Map.Entry<String, ? extends Object> entry : solrConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();

        if (indexConfig != null) {
            builder.startObject("index");
            for (Map.Entry<String, ? extends Object> entry : indexConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (transformConfig != null) {
            builder.startObject("transform");
            for (Map.Entry<String, ? extends Object> entry : transformConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        builder.endObject();

        logger.debug("Registering river \n{}", builder.string());

        esClient.index(Requests.indexRequest("_river").type("cassandra_river").id("_meta").source(builder)).actionGet();
    }
}