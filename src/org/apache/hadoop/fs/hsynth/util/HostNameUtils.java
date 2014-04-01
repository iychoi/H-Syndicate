package org.apache.hadoop.fs.hsynth.util;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HostNameUtils {

    public static final Log LOG = LogFactory.getLog(HostNameUtils.class);
    
    public static String getLocalHostName() throws IOException {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            String hostname = addr.getHostName();

            LOG.info("Host Name = " + hostname);
            return hostname;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String getLocalIPAddress() throws IOException {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            String ipAddress = addr.getHostAddress();
            
            LOG.info("IP Address = " + ipAddress);
            return ipAddress;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String getIPByAddress(String address) throws IOException {
        try {
            InetAddress addr = InetAddress.getByName(address);
            String ipAddress = addr.getHostAddress();
            
            LOG.info("IP Address = " + ipAddress);
            return ipAddress;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String getHostNameByAdress(String address) throws IOException {
        try {
            InetAddress addr = InetAddress.getByName(address);
            String hostname = addr.getHostName();
            
            LOG.info("Host Name = " + hostname);
            return hostname;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
