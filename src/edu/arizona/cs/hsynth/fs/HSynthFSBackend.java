package edu.arizona.cs.hsynth.fs;

import java.util.Hashtable;

public class HSynthFSBackend {
    private static Hashtable<String, Class<? extends HSynthFSConfiguration>> BACKEND_CONF_TABLE = new Hashtable<String, Class<? extends HSynthFSConfiguration>>();
    
    public static void registerBackend(Class<? extends HSynthFSConfiguration> clazz) throws InstantiationException, IllegalAccessException {
        HSynthFSConfiguration conf = clazz.newInstance();
        String backendName = conf.getBackendName();
        
        if(!BACKEND_CONF_TABLE.containsKey(backendName)) {
            BACKEND_CONF_TABLE.put(backendName, clazz);
        }
    }
    
    public static void registerBackend(String backendName, Class<? extends HSynthFSConfiguration> clazz) throws InstantiationException, IllegalAccessException {
        if(!BACKEND_CONF_TABLE.containsKey(backendName)) {
            BACKEND_CONF_TABLE.put(backendName, clazz);
        }
    }
    
    public static Class<? extends HSynthFSConfiguration> findBackendConfigurationByName(String name) {
        return BACKEND_CONF_TABLE.get(name);
    }
}
