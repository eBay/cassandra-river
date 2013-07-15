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

package org.elasticsearch.plugin.river.cassandra;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.cassandra.CassandraRiverModule;

public class CassandraRiverPlugin extends AbstractPlugin {

    @Inject
    public CassandraRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-cassandra";
    }

    @Override
    public String description() {
        return "River Cassandra plugin";
    }

    /**
     * Registers the {@link CassandraRiverModule}
     * @param module the elasticsearch module used to handle rivers
     */
    public void onModule(RiversModule module) {
        module.registerRiver("cassandra", CassandraRiverModule.class);
    }
}
