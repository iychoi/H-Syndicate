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
package edu.arizona.cs.hsyndicate.dfs.util;

import edu.arizona.cs.hsyndicate.fs.AHSyndicateFileSystemBase;
import edu.arizona.cs.hsyndicate.fs.SyndicateFSConfiguration;
import edu.arizona.cs.hsyndicate.fs.SyndicateFileSystem;
import java.util.Map;
import org.apache.commons.collections4.map.PassiveExpiringMap;

public class SyndicateFileSystemFactory {
    private static final int DEFAULT_FILESYSTEM_TIMETOLIVE = 600000; // 600 sec
    private static Map<String, AHSyndicateFileSystemBase> fsCache = new PassiveExpiringMap<String, AHSyndicateFileSystemBase>(DEFAULT_FILESYSTEM_TIMETOLIVE);
    
    public static synchronized AHSyndicateFileSystemBase getInstance(SyndicateFSConfiguration sconf) throws InstantiationException {
        AHSyndicateFileSystemBase cachedFS = fsCache.get(sconf.getAddress());
        if(cachedFS != null && cachedFS.isClosed()) {
            fsCache.remove(sconf.getAddress());
            cachedFS = null;
        }
        
        if(cachedFS == null) {
            cachedFS = new SyndicateFileSystem(sconf);
            fsCache.put(sconf.getAddress(), cachedFS);
        }
        
        return cachedFS;
    }
}
