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
package hsyndicate.rest.common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author iychoi
 */
public class WebParamBuilder {
    private String resourceURL;
    private Map<String, String> params = new LinkedHashMap<String, String>();
    
    public WebParamBuilder(String contentURL) {
        this.resourceURL = contentURL;
    }
    
    public void addParam(String key, String value) {
        this.params.put(key, value);
    }
    
    public void addParam(String key, int value) {
        this.params.put(key, Integer.toString(value));
    }
    
    public void addParam(String key, long value) {
        this.params.put(key, Long.toString(value));
    }
    
    public void removeParam(String key) {
        this.params.remove(key);
    }
    
    public String build() {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, String>> entrySet = this.params.entrySet();
        for(Map.Entry<String, String> entry : entrySet) {
            if(sb.length() != 0) {
                sb.append("&");
            }
            
            if(entry.getValue() == null) {
                sb.append(entry.getKey());
            } else {
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return this.resourceURL + "?" + sb.toString();
    }
}
