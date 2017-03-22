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
package hsyndicate.hadoop.dfs;

public class HSyndicateUGMonitorResults<T> {
    private String hostname;
    private T result;
    
    public HSyndicateUGMonitorResults(String hostname) {
        this.hostname = hostname;
    }
    
    public HSyndicateUGMonitorResults(String hostname, T result) {
        this.hostname = hostname;
        this.result = result;
    }
    
    public void setResult(T result) {
        this.result = result;
    }
    
    public String getHostname() {
        return this.hostname;
    }
    
    public T getResult() {
        return this.result;
    }
}
