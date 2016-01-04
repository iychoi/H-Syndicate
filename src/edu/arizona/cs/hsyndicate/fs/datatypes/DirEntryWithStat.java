/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.arizona.cs.hsyndicate.fs.datatypes;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class DirEntryWithStat {
    private String path;
    private Stat stat;

    public DirEntryWithStat() {
        
    }
    
    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    @JsonProperty("path")
    public void setPath(String path) {
        this.path = path;
    }

    @JsonProperty("stat")
    public Stat getStat() {
        return stat;
    }

    @JsonProperty("stat")
    public void setStat(Stat stat) {
        this.stat = stat;
    }
}
