/*
   Copyright 2016 The Trustees of University of Arizona

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

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class SyndicateFSConfiguration {

    private String host = "localhost";
    private int port = 8888;
    private String sessionName;
    private String sessionKey;
    
    public SyndicateFSConfiguration() {
        
    }
    
    @JsonProperty("host")
    public synchronized void setHost(String host) {
        this.host = host;
    }
    
    @JsonProperty("host")
    public synchronized String getHost() {
        return this.host;
    }
    
    @JsonProperty("port")
    public synchronized void setPort(int port) {
        this.port = port;
    }
    
    @JsonProperty("port")
    public synchronized int getPort() {
        return this.port;
    }
    
    @JsonIgnore
    @Override
    public synchronized String toString() {
        return this.host + ":" + this.port;
    }

    @JsonIgnore
    public synchronized String getAddress() {
        return this.host + ":" + this.port;
    }
}
