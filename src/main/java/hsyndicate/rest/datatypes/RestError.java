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
package hsyndicate.rest.datatypes;

import hsyndicate.rest.common.RestfulException;
import org.codehaus.jackson.annotate.JsonProperty;

public class RestError {

    private boolean result;
    private String name;
    private String message;
    
    private int httpErrno;
    private String path;
    
    public RestError() {
    }
    
    @JsonProperty("result")
    public boolean getResult() {
        return result;
    }

    @JsonProperty("result")
    public void setResult(boolean result) {
        this.result = result;
    }
    
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("message")
    public void setMessage(String message) {
        this.message = message;
    }

    public int getHttpErrno() {
        return httpErrno;
    }

    public void setHttpErrno(int httpErrno) {
        this.httpErrno = httpErrno;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
    
    @Override
    public String toString() {
        return String.format("%s (result:%s, name:%s)", this.message, String.valueOf(this.result), this.name);
    }
    
    public RestfulException makeException() {
        return new RestfulException(this.toString());
    }
}
