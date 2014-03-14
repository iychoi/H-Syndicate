package org.apache.hadoop.fs.hsynth.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IPAddressUtil {
    
    public static final Log LOG = LogFactory.getLog(IPAddressUtil.class);
    
    private static List<NetworkInterface> interfaces;
    private static List<String> addresses;
    
    public static boolean isMyIPAddress(String ipAddress) {
        try {
            for(String myAddr : getIPAddresses()) {
                if(myAddr.equals(ipAddress)) {
                    return true;
                }
            }
            return false;
        } catch (SocketException ex) {
            LOG.error(ex);
            return false;
        }
    }
    
    public static List<String> getIPAddresses() throws SocketException {
        if(addresses == null) {
            addresses = new ArrayList<String>();
            for(NetworkInterface ni : getNetworkInterfaces()) {
                Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();

                while (inetAddresses.hasMoreElements()) {
                    InetAddress ia = inetAddresses.nextElement();
                    if (!ia.isLinkLocalAddress()) {
                        addresses.add(ia.getHostAddress().trim());
                    }
                }
            }
        }
        
        return addresses;
    }
    
    private synchronized static List<NetworkInterface> getNetworkInterfaces() throws SocketException {
        if(interfaces == null) {
            interfaces = new ArrayList<NetworkInterface>();
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while(networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();
                interfaces.add(ni);
            }
        }
        return interfaces;
    }
}
