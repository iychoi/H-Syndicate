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

import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class SessionList {
    private List<SessionInfo> sessions = new ArrayList<SessionInfo>();

    public SessionList() {
        
    }
    
    @JsonProperty("sessions")
    public List<SessionInfo> getSessions() {
        return this.sessions;
    }

    @JsonProperty("sessions")
    public void addSessions(List<SessionInfo> sessions) {
        this.sessions.addAll(sessions);
    }
    
    @JsonIgnore
    public void addEntry(SessionInfo session) {
        this.sessions.add(session);
    }
}
