Cassandra river for Elastic search.

This river was a proof of concept for integration of cassandra with elasticsearch, its a 2 day hacked up solution.  

##Setup

Build: `mvn clean package`

Install:
`./bin/plugin -url file:elasticsearch-river-cassandra/target/releases/elasticsearch-river-cassandra-1.0.0-SNAPSHOT.zip -install river-cassandra`


Remove:
`./bin/plugin -remove river-cassandra`



##Init

    curl -XPUT 'localhost:9200/_river/prodinfo/_meta' -d '{
        "type" : "cassandra",
        "cassandra" : {
    		"cluster_name" : "test-cluster",
    		"keyspace" : "catalogks",
    		"column_family" : "info",
    		"batch_size" : 1000,
    		"hosts" : "host1:9161,host2:9161",
    		"username" : "username",
    		"password" : "password"
        },
        "index" : {
            "index" : "prodinfo",
            "type" : "product"
        }
    }'


##Query
 1. localhost:9200/info/_search  
 2. localhost:9200/info/_count


##References  
 1. http://jfarrell.github.com/    
 2. Setup elasticsearch-head and bigdesk to monitor ES
 
 
 
##Improvements
 1. http://mail-archives.apache.org/mod_mbox/cassandra-user/201303.mbox/%3CEB07A386-F9E3-4CF3-BBC6-9DA3B9CAA79F@thelastpickle.com%3E  
 2. https://groups.google.com/forum/?fromgroups=#!topic/elasticsearch/M1aJqvAIpZE
 3. Tests  