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
package hsyndicate.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPUtils {
    
    private static final Log LOG = LogFactory.getLog(IPUtils.class);
    
    private static String cachedPublicIP;
    private static List<String> cachedHostAddr = new ArrayList<String>();
    private static List<String> cachedIPAddr = new ArrayList<String>();
    
    
    private static final String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
    
    private static final String DOMAINNAME_PATTERN = "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$";
    
    private static Pattern ipPattern = Pattern.compile(IPADDRESS_PATTERN);
    private static Pattern domainPattern = Pattern.compile(DOMAINNAME_PATTERN);
    
    private static boolean isProperHostAddress(String addr) {
        if(addr == null || addr.isEmpty()) {
            return false;
        }
        
        if(addr.equalsIgnoreCase("localhost")) {
            return false;
        } else if(addr.equalsIgnoreCase("ip6-localhost")) {
            return false;
        } else if(addr.equalsIgnoreCase("127.0.0.1")) {
            return false;
        } else if(addr.indexOf(":") > 0) {
            return false;
        }
        return true;
    }
    
    public static Collection<String> getHostAddress() {
        if(!cachedHostAddr.isEmpty()) {
            return Collections.unmodifiableCollection(cachedHostAddr);
        } else {
            try {
                Enumeration e = NetworkInterface.getNetworkInterfaces();
                while (e.hasMoreElements()) {
                    NetworkInterface n = (NetworkInterface) e.nextElement();
                    Enumeration ee = n.getInetAddresses();
                    while (ee.hasMoreElements()) {
                        InetAddress i = (InetAddress) ee.nextElement();

                        String hostAddress = i.getHostAddress();
                        if(isProperHostAddress(hostAddress)) {
                            if(!cachedHostAddr.contains(hostAddress)) {
                                cachedHostAddr.add(hostAddress);
                            }
                        }

                        String hostName = i.getHostName();
                        if(isProperHostAddress(hostName)) {
                            if(!cachedHostAddr.contains(hostName)) {
                                cachedHostAddr.add(hostName);
                            }
                        }

                        String canonicalHostName = i.getCanonicalHostName();
                        if(isProperHostAddress(canonicalHostName)) {
                            if(!cachedHostAddr.contains(canonicalHostName)) {
                                cachedHostAddr.add(canonicalHostName);
                            }
                        }
                    }
                }
            } catch (SocketException ex) {
                LOG.error("Exception occurred while scanning local interfaces", ex);
            }
            
            return Collections.unmodifiableCollection(cachedHostAddr);
        }
    }
    
    public static Collection<String> getIPAddress() {
        if(!cachedIPAddr.isEmpty()) {
            return Collections.unmodifiableCollection(cachedIPAddr);
        } else {
            try {
                Enumeration e = NetworkInterface.getNetworkInterfaces();
                while (e.hasMoreElements()) {
                    NetworkInterface n = (NetworkInterface) e.nextElement();
                    Enumeration ee = n.getInetAddresses();
                    while (ee.hasMoreElements()) {
                        InetAddress i = (InetAddress) ee.nextElement();
                        if(!i.isLoopbackAddress()) {
                            String hostAddress = i.getHostAddress();
                            cachedIPAddr.add(hostAddress);
                        }
                    }
                }
            } catch (SocketException ex) {
                LOG.error("Exception occurred while scanning local interfaces", ex);
            }

            return Collections.unmodifiableCollection(cachedIPAddr);
        }
    }
    
    public static String getPublicIPAddress() {
        if(cachedPublicIP != null && !cachedPublicIP.isEmpty()) {
            return cachedPublicIP;
        } else {
            try {
                URL whatismyip = new URL("http://checkip.amazonaws.com");
                BufferedReader in = null;
                try {
                    in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
                    String ip = in.readLine();
                    
                    // cache
                    cachedPublicIP = ip;

                    return ip;
                } catch (IOException ex) {
                    LOG.error("Exception occurred while querying public ip address to amazonaws.com", ex);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                            LOG.error("Exception occurred while querying public ip address to amazonaws.com", e);
                        }
                    }
                }
            } catch (MalformedURLException ex) {
                LOG.error("Exception occurred while querying public ip address to amazonaws.com", ex);
            }
        }
        return null;
    }
    
    public static boolean isIPAddress(String address) {
        Matcher matcher = ipPattern.matcher(address);
        return matcher.matches();
    }
    
    public static boolean isDomainName(String address) {
        Matcher matcher = domainPattern.matcher(address);
        return matcher.matches();
    }

    public static boolean isPublicIPAddress(String address) {
        Matcher matcher = ipPattern.matcher(address);
        if(matcher.matches()) {
            String first = matcher.group(1);
            String second = matcher.group(2);
            
            int f = Integer.parseInt(first);
            int s = Integer.parseInt(second);
            
            if(f == 192 && s == 168) {
                return false;
            }
            
            if(f == 172 && s >= 16 && s <= 31) {
                return false;
            }
            
            if(f == 10) {
                return false;
            }
            
            return true;
        }
        
        return false;
    }
    
    public static boolean isLocalIPAddress(String address) {
        Collection<String> localhostAddress = IPUtils.getHostAddress();
        Set<String> localhostAddrSet = new HashSet<String>();
        localhostAddrSet.addAll(localhostAddress);
        
        String hostname = address;
        int pos = hostname.indexOf(":");
        if(pos > 0) {
            hostname = hostname.substring(0, pos);
        }

        if(hostname.equalsIgnoreCase("localhost") || hostname.equals("127.0.0.1")) {
            // localloop
            return true;
        }

        if(localhostAddrSet.contains(hostname)) {
            return true;
        }
        
        return false;
    }
    
    public static String selectLocalIPAddress(List<String> addresses) {
        return selectLocalIPAddress(addresses.toArray(new String[0]));
    }
    
    public static String selectLocalIPAddress(String[] addresses) {
        Collection<String> localhostAddress = IPUtils.getHostAddress();
        Set<String> localhostAddrSet = new HashSet<String>();
        localhostAddrSet.addAll(localhostAddress);
        
        for(String addr: addresses) {
            String hostname = addr;
            int pos = hostname.indexOf(":");
            if(pos > 0) {
                hostname = hostname.substring(0, pos);
            }
            
            if(hostname.equalsIgnoreCase("localhost") || hostname.equals("127.0.0.1")) {
                // localloop
                return addr;
            }
            
            if(localhostAddrSet.contains(hostname)) {
                return addr;
            }
        }
        
        return null;
    }

    public static String parseHost(String address) {
        int pos = address.indexOf(":");
        if(pos > 0) {
            return address.substring(0, pos);
        } else {
            return address;
        }
    }
}
