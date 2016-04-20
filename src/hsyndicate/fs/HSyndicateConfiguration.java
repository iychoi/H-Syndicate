/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package hsyndicate.fs;

import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class HSyndicateConfiguration {

    public static final int DEFAULT_PORT_NUMBER = 8888;
            
    private List<String> hosts = new ArrayList<String>();
    private int defaultPort = DEFAULT_PORT_NUMBER;
    
    public HSyndicateConfiguration() {
        
    }
    
    @JsonProperty("hosts")
    public synchronized void addHosts(List<String> hosts) {
        this.hosts.addAll(hosts);
    }
    
    @JsonIgnore
    public synchronized void addHost(String host) {
        this.hosts.add(host);
    }
    
    @JsonProperty("hosts")
    public synchronized List<String> getHosts() {
        return this.hosts;
    }
    
    @JsonProperty("default_port")
    public synchronized void setDefaultPort(int default_port) {
        this.defaultPort = default_port;
    }
    
    @JsonProperty("default_port")
    public synchronized int getDefaultPort() {
        return this.defaultPort;
    }
}
