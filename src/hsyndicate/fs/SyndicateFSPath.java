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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateFSPath implements Comparable {

    private static final Log LOG = LogFactory.getLog(SyndicateFSPath.class);
    
    // stores path hierarchy
    private URI uri;
    
    /*
     * Construct a path from parent/child pairs
     */
    public SyndicateFSPath(String parent, String child) {
        this(new SyndicateFSPath(parent), new SyndicateFSPath(child));
    }
    
    public SyndicateFSPath(SyndicateFSPath parent, String child) {
        this(parent, new SyndicateFSPath(child));
    }
    
    public SyndicateFSPath(String parent, SyndicateFSPath child) {
        this(new SyndicateFSPath(parent), child);
    }
    
    public SyndicateFSPath(SyndicateFSPath parent, SyndicateFSPath child) {
        if (parent == null)
            throw new IllegalArgumentException("Can not resolve a path from a null parent");
        if (child == null)
            throw new IllegalArgumentException("Can not resolve a path from a null child");
        
        URI parentUri = parent.uri;
        if (parentUri == null)
            throw new IllegalArgumentException("Can not resolve a path from a null parent URI");
        
        String parentPath = parentUri.getPath();
        
        if (!(parentPath.equals("/") || parentPath.equals(""))) {
            // parent path is not empty -- need to parse
            try {
                parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath() + "/", null, parentUri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        
        URI resolved = parentUri.resolve(child.uri);
        
        // assign resolved uri to member field
        this.uri = createPathUri(resolved.getScheme(), resolved.getAuthority(), normalizePath(resolved.getPath()));
        LOG.info("path - " + uri.toString());
    }
    
    /*
     * Construct a path from string
     */
    public SyndicateFSPath(String path) {
        if (path == null)
            throw new IllegalArgumentException("Can not create a path from a null string");
        if (path.length() == 0)
            throw new IllegalArgumentException("Can not create a path from an empty string");
        
        String uriScheme = null;
        String uriAuthority = null;
        String uriPath = null;
        
        int start = 0;

        // parse uri scheme
        int colon = path.indexOf(':');
        if (colon != -1) {
            uriScheme = path.substring(0, colon);
            start = colon + 1;
            
            // parse uri authority
            if (path.startsWith("//", start)
                && (path.length() - start > 2)) {
                // have authority
                int nextSlash = path.indexOf('/', start + 2);
                int authEnd;
                if (nextSlash != -1)
                    authEnd = nextSlash;
                else 
                    authEnd = path.length();
                
                uriAuthority = path.substring(start + 2, authEnd);
                start = authEnd;
            }
        }
        
        // uri path
        if(start < path.length())
            uriPath = path.substring(start, path.length());

        // assign resolved uri to member field
        this.uri = createPathUri(uriScheme, uriAuthority, uriPath);
        LOG.info("path - " + uri.toString());
    }
    
    /*
     * Construct a path from URI
     */
    public SyndicateFSPath(URI uri) {
        this.uri = uri;
        LOG.info("path - " + uri.toString());
    }
    
    private URI createPathUri(String scheme, String authority, String path) {
        try {
            URI uri = new URI(scheme, authority, normalizePath(path), null, null);
            return uri.normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    private String normalizePath(String path) {
        // replace all "//" and "\" to "/"
        path = path.replace("//", "/");
        path = path.replace("\\", "/");

        // trim trailing slash
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }
    
    /*
     * Return URI formatted path
     */
    public URI toUri() {
        return this.uri;
    }
    
    /*
     * True if the path is absolute
     */
    public boolean isAbsolute() {
        return this.uri.getPath().startsWith("/");
    }
    
    /*
     * Return file name
     */
    public String getName() {
        String path = this.uri.getPath();
        int slash = path.lastIndexOf('/');
        return path.substring(slash + 1, path.length());
    }
    
    /*
     * Return the parent path, Null if parent is root
     */
    public SyndicateFSPath getParent() {
        String path = this.uri.getPath();
        int lastSlash = path.lastIndexOf('/');
        // empty
        if (path.length() == 0)
            return null;
        // root
        if (path.length() == 1 && lastSlash == 0)
            return null;
        
        if (lastSlash == -1) {
            return new SyndicateFSPath(createPathUri(this.uri.getScheme(), this.uri.getAuthority(), "."));
        } else if (lastSlash == 0) {
            return new SyndicateFSPath(createPathUri(this.uri.getScheme(), this.uri.getAuthority(), "/"));
        } else {
            String parent = path.substring(0, lastSlash);
            return new SyndicateFSPath(createPathUri(this.uri.getScheme(), this.uri.getAuthority(), parent));
        }
    }
    
    /*
     * Return the stringfied path that contains scheme, authority and path
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        if (uri.getScheme() != null) {
            sb.append(uri.getScheme());
            sb.append(":");
        }
        if (uri.getAuthority() != null) {
            sb.append("//");
            sb.append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            sb.append(path);
        }
        
        return sb.toString();
    }
    
    /*
     * Return the stringfied path that does not contains scheme and authority
     */
    public String getPath() {
        StringBuilder sb = new StringBuilder();
        
        if (uri.getPath() != null) {
            String path = uri.getPath();
            sb.append(path);
        }
        
        return sb.toString();
    }
    
    public int depth() {
        String path = uri.getPath();
        
        if(path.length() == 1 && path.startsWith("/"))
            return 0;
        
        int depth = 0;
        int slash = 0;
        
        while(slash != -1) {
            // slash starts from 1
            slash = path.indexOf("/", slash + 1);
            depth++;
        }
        
        return depth;
    }
    
    public SyndicateFSPath[] getAncestors() {
        List<SyndicateFSPath> ancestors = new ArrayList<SyndicateFSPath>();
        
        SyndicateFSPath parent = getParent();
        while(parent != null) {
            ancestors.add(0, parent);
            
            parent = parent.getParent();
        }
        
        SyndicateFSPath[] ancestors_array = ancestors.toArray(new SyndicateFSPath[0]);
        return ancestors_array;
    }
    
    public SyndicateFSPath suffix(String suffix) {
        return new SyndicateFSPath(getPath() + suffix);
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SyndicateFSPath))
            return false;
        
        SyndicateFSPath other = (SyndicateFSPath) o;
        return this.uri.equals(other.uri);
    }
    
    @Override
    public int hashCode() {
        return this.uri.hashCode();
    }
    
    @Override
    public int compareTo(Object o) {
        SyndicateFSPath other = (SyndicateFSPath) o;
        return this.uri.compareTo(other.uri);
    }
}
