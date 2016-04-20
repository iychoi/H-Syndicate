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

import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import hsyndicate.rest.datatypes.RestError;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

/**
 *
 * @author iychoi
 */
public class RestfulClient {
    
    private static final Log LOG = LogFactory.getLog(RestfulClient.class);
    
    private URI serviceURL;
    private ClientConfig httpClientConfig;
    private Client httpClient;
    
    public RestfulClient(URI serviceURL, int threadPoolSize) {
        if(serviceURL == null) {
            throw new IllegalArgumentException("serviceURL is null");
        }
        
        if(threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize is invalid");
        }
        
        this.serviceURL = serviceURL;
        
        this.httpClientConfig = new DefaultClientConfig();
        this.httpClientConfig.getClasses().add(JacksonJsonProvider.class);
        this.httpClientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        this.httpClientConfig.getProperties().put(ClientConfig.PROPERTY_THREADPOOL_SIZE, threadPoolSize);

        this.httpClient = Client.create(this.httpClientConfig);
    }
    
    public void close() {
        this.httpClient.getExecutorService().shutdownNow();
        this.httpClient.destroy();
    }
    
    public Object post(String path, Object request, GenericType<?> generic) throws IOException, FileNotFoundException, RestfulException {
        Future<ClientResponse> future = postAsync(path, request);
        return processPost(future, generic);
    }
    
    public Future<ClientResponse> postAsync(String path, Object request) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(request == null) {
            throw new IllegalArgumentException("request is null");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        return (Future<ClientResponse>) webResource.accept("application/json").type("application/json").post(ClientResponse.class, request);
    }
    
    public Object processPost(Future<ClientResponse> future, GenericType<?> generic) throws IOException, FileNotFoundException, RestfulException {
        if(future == null) {
            throw new IllegalArgumentException("future is null");
        }
        
        //if(generic == null) {
        //    throw new IllegalArgumentException("generic is null");
        //}
        
        // wait for completion
        try {
            ClientResponse response = future.get();
            if(response.getStatus() >= 200 && response.getStatus() <= 299) {
                if(generic == null) {
                    return true;
                } else {
                    return response.getEntity(generic);
                }
            } else if(response.getStatus() == 404) {
                throw new FileNotFoundException(response.toString());
            } else {
                RestError err = response.getEntity(new GenericType<RestError>(){});
                err.setHttpErrno(response.getStatus());
                err.setPath(response.getLocation().toString());
                throw err.makeException();
            }
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
    
    public Object get(String path, GenericType<?> generic) throws IOException, FileNotFoundException, RestfulException {
        Future<ClientResponse> future = getAsync(path);
        return processGet(future, generic);
    }
    
    public Future<ClientResponse> getAsync(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        return (Future<ClientResponse>) webResource.accept("application/json").type("application/json").get(ClientResponse.class);
    }
    
    public Object processGet(Future<ClientResponse> future, GenericType<?> generic) throws IOException, FileNotFoundException, RestfulException {
        if(future == null) {
            throw new IllegalArgumentException("future is null");
        }
        
        if(generic == null) {
            throw new IllegalArgumentException("generic is null");
        }
        
        // wait for completion
        try {
            ClientResponse response = future.get();
            if(response.getStatus() >= 200 && response.getStatus() <= 299) {
                return response.getEntity(generic);
            } else if(response.getStatus() == 404) {
                throw new FileNotFoundException(response.toString());
            } else {
                RestError err = response.getEntity(new GenericType<RestError>(){});
                err.setHttpErrno(response.getStatus());
                err.setPath(response.getLocation().toString());
                throw err.makeException();
            }
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
    
    public Object delete(String path, GenericType<?> generic) throws IOException, FileNotFoundException, RestfulException {
        Future<ClientResponse> future = deleteAsync(path);
        return processDelete(future, generic);
    }
    
    public Future<ClientResponse> deleteAsync(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        return (Future<ClientResponse>) webResource.accept("application/json").type("application/json").delete(ClientResponse.class);
    }
    
    public Object processDelete(Future<ClientResponse> future, GenericType<?> generic) throws IOException, FileNotFoundException, RestfulException {
        if(future == null) {
            throw new IllegalArgumentException("future is null");
        }
        
        //if(generic == null) {
        //    throw new IllegalArgumentException("generic is null");
        //}
        
        // wait for completion
        try {
            ClientResponse response = future.get();
            if(response.getStatus() >= 200 && response.getStatus() <= 299) {
                if(generic == null) {
                    return true;
                } else {
                    return response.getEntity(generic);
                }
            } else if(response.getStatus() == 404) {
                throw new FileNotFoundException(response.toString());
            } else {
                RestError err = response.getEntity(new GenericType<RestError>(){});
                err.setHttpErrno(response.getStatus());
                err.setPath(response.getLocation().toString());
                throw err.makeException();
            }
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
    
    public InputStream download(String path) throws IOException, FileNotFoundException, RestfulException {
        Future<ClientResponse> future = deleteAsync(path);
        return processDownload(future);
    }
    
    public Future<ClientResponse> downloadAsync(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        return (Future<ClientResponse>) webResource.accept("application/octet-stream").type("application/json").get(ClientResponse.class);
    }
    
    public InputStream processDownload(Future<ClientResponse> future) throws IOException, FileNotFoundException, RestfulException {
        if(future == null) {
            throw new IllegalArgumentException("future is null");
        }
        
        // wait for completition
        try {
            ClientResponse response = future.get();
            if(response.getStatus() >= 200 && response.getStatus() <= 299) {
                return response.getEntityInputStream();
            } else if(response.getStatus() == 404) {
                throw new FileNotFoundException(response.toString());
            } else {
                RestError err = response.getEntity(new GenericType<RestError>(){});
                err.setHttpErrno(response.getStatus());
                err.setPath(response.getLocation().toString());
                throw err.makeException();
            }
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
}
