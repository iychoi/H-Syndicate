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
package hsyndicate.rest.client;

import hsyndicate.rest.common.RestfulClient;
import hsyndicate.rest.common.WebParamBuilder;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import hsyndicate.rest.common.RestfulException;
import hsyndicate.rest.datatypes.DirectoryEntries;
import hsyndicate.rest.datatypes.FileDescriptor;
import hsyndicate.rest.datatypes.StatRaw;
import hsyndicate.rest.datatypes.Statvfs;
import hsyndicate.rest.datatypes.Xattr;
import hsyndicate.rest.datatypes.XattrKeyList;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyndicateUGHttpClient implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SyndicateUGHttpClient.class);
    
    private static final String GET_STATVFS = "statvfs";
    private static final String GET_STAT = "stat";
    private static final String GET_XATTR = "getxattr";
    private static final String GET_XATTR_KEY = "key";
    private static final String LIST_XATTR = "listxattr";
    private static final String LIST_DIR = "listdir";
    private static final String OPEN = "open";
    private static final String OPEN_FLAG = "flag";
    private static final String READ = "read";
    private static final String READ_FD = "fd";
    private static final String READ_OFFSET = "offset";
    private static final String READ_LENGTH = "len";
    private static final String MAKE_DIR = "mkdir";
    private static final String MAKE_DIR_MODE = "mode";
    private static final String SET_XATTR = "setxattr";
    private static final String SET_XATTR_KEY = "key";
    private static final String SET_XATTR_VALUE = "value";
    private static final String WRITE = "write";
    private static final String WRITE_FD = "fd";
    private static final String WRITE_OFFSET = "offset";
    private static final String WRITE_LENGTH = "len";
    private static final String EXTEND_TTL = "extendttl";
    private static final String EXTEND_TTL_FD = "fd";
    private static final String UPDATE_TIMES = "utimes";
    private static final String UPDATE_TIMES_TIME = "time";
    private static final String RENAME = "rename";
    private static final String RENAME_TO = "to";
    private static final String REMOVE_DIR = "rmdir";
    private static final String UNLINK = "unlink";
    private static final String REMOVE_XATTR = "rmxattr";
    private static final String REMOVE_XATTR_KEY = "key";
    private static final String CLOSE = "close";
    private static final String CLOSE_FD = "fd";
    
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    
    private URI serviceURI;
    private RestfulClient client;
    
    public SyndicateUGHttpClient(String host, int port) throws InstantiationException {
        if(host == null) {
            throw new IllegalArgumentException("host is null");
        }
        
        if(port <= 0) {
            throw new IllegalArgumentException("port is illegal");
        }
        
        try {
            URI serviceURI = new URI("http://" + host + ":" + port);
            initialize(serviceURI);
        } catch (URISyntaxException ex) {
            LOG.error("exception occurred", ex);
            throw new InstantiationException(ex.getMessage());
        }
    }
    
    private void initialize(URI serviceURI) {
        if(serviceURI == null) {
            throw new IllegalArgumentException("serviceURI is null");
        }
        
        LOG.info("connect to " + serviceURI.toASCIIString());
        
        this.serviceURI = serviceURI;
        this.client = new RestfulClient(serviceURI, DEFAULT_THREAD_POOL_SIZE);
    }

    public URI getServiceURI() {
        return this.serviceURI;
    }
    
    public String getServiceHost() {
        return this.serviceURI.getHost();
    }
    
    public int getServicePort() {
        return this.serviceURI.getPort();
    }
    
    @Override
    public void close() throws IOException {
        this.client.close();
    }
    
    public Future<ClientResponse> getStatvfs() throws IOException {
        WebParamBuilder builder = new WebParamBuilder("/");
        builder.addParam(GET_STATVFS, null);
        return this.client.getAsync(builder.build());
    }
    
    public Statvfs processGetStatvfs(Future<ClientResponse> future) throws IOException, RestfulException {
        return (Statvfs)this.client.processGet(future, new GenericType<Statvfs>(){});
    }

    public Future<ClientResponse> getStat(String path) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(GET_STAT, null);
        return this.client.getAsync(builder.build());
    }
    
    public StatRaw processGetStat(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        return (StatRaw)this.client.processGet(future, new GenericType<StatRaw>(){});
    }
    
    public Future<ClientResponse> getXattr(String path, String key) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(GET_XATTR, null);
        builder.addParam(GET_XATTR_KEY, key);
        return this.client.getAsync(builder.build());
    }
    
    public Xattr processGetXattr(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        return (Xattr)this.client.processGet(future, new GenericType<Xattr>(){});
    }
    
    public Future<ClientResponse> listXattr(String path) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(LIST_XATTR, null);
        return this.client.getAsync(builder.build());
    }
    
    public XattrKeyList processListXattr(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        return (XattrKeyList)this.client.processGet(future, new GenericType<XattrKeyList>(){});
    }
    
    public Future<ClientResponse> listDir(String path) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(LIST_DIR, null);
        return this.client.getAsync(builder.build());
    }
    
    public DirectoryEntries processListDir(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        return (DirectoryEntries)this.client.processGet(future, new GenericType<DirectoryEntries>(){});
    }
    
    public Future<ClientResponse> open(String path, String flag) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(OPEN, null);
        builder.addParam(OPEN_FLAG, flag);
        return this.client.getAsync(builder.build());
    }
    
    public FileDescriptor processOpen(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        return (FileDescriptor)this.client.processGet(future, new GenericType<FileDescriptor>(){});
    }
    
    public Future<ClientResponse> read(String path, FileDescriptor fi, long offset, int len) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(READ, null);
        builder.addParam(READ_FD, fi.getFd());
        builder.addParam(READ_OFFSET, offset);
        builder.addParam(READ_LENGTH, len);
        return this.client.downloadAsync(builder.build());
    }
    
    public InputStream processRead(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        return (InputStream)this.client.processDownload(future);
    }
    
    public Future<ClientResponse> makeDir(String path, int mode) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(MAKE_DIR, null);
        builder.addParam(MAKE_DIR_MODE, mode);
        return this.client.postAsync(builder.build(), null);
    }
    
    public void processMakeDir(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processPost(future, null);
    }
    
    public Future<ClientResponse> setXattr(String path, String key, String value) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(SET_XATTR, null);
        builder.addParam(SET_XATTR_KEY, key);
        builder.addParam(SET_XATTR_VALUE, value);
        return this.client.postAsync(builder.build(), null);
    }
    
    public void processSetXattr(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processPost(future, null);
    }
    
    public Future<ClientResponse> write(String path, FileDescriptor fi, long offset, int len, InputStream is) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(WRITE, null);
        builder.addParam(WRITE_FD, fi.getFd());
        builder.addParam(WRITE_OFFSET, offset);
        builder.addParam(WRITE_LENGTH, len);
        return this.client.postAsync(builder.build(), is);
    }
    
    public void processWrite(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processPost(future, null);
    }
    
    public Future<ClientResponse> extendTtl(String path, FileDescriptor fi) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(EXTEND_TTL, null);
        builder.addParam(EXTEND_TTL_FD, fi.getFd());
        return this.client.postAsync(builder.build(), null);
    }
    
    public void processExtendTtl(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processPost(future, null);
    }
    
    public Future<ClientResponse> updateTimes(String path, long time) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(UPDATE_TIMES, null);
        builder.addParam(UPDATE_TIMES_TIME, time);
        return this.client.postAsync(builder.build(), null);
    }
    
    public void processUpdateTimes(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processPost(future, null);
    }
    
    public Future<ClientResponse> rename(String path, String toPath) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(RENAME, null);
        builder.addParam(RENAME_TO, toPath);
        return this.client.postAsync(builder.build(), null);
    }
    
    public void processRename(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processPost(future, null);
    }
    
    public Future<ClientResponse> removeDir(String path) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(REMOVE_DIR, null);
        return this.client.deleteAsync(builder.build());
    }
    
    public void processRemoveDir(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processDelete(future, null);
    }
    
    public Future<ClientResponse> unlink(String path) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(UNLINK, null);
        return this.client.deleteAsync(builder.build());
    }
    
    public void processUnlink(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processDelete(future, null);
    }
    
    public Future<ClientResponse> removeXattr(String path, String key) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(REMOVE_XATTR, null);
        builder.addParam(REMOVE_XATTR_KEY, key);
        return this.client.deleteAsync(builder.build());
    }
    
    public void processRemoveXattr(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processDelete(future, null);
    }
    
    public Future<ClientResponse> close(String path, FileDescriptor fi) throws IOException {
        WebParamBuilder builder = new WebParamBuilder(path);
        builder.addParam(CLOSE, null);
        builder.addParam(CLOSE_FD, fi.getFd());
        return this.client.deleteAsync(builder.build());
    }
    
    public void processClose(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        this.client.processDelete(future, null);
    }
}
