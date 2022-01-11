package io.ingestr.framework.service.utils;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.net.Inet4Address;

@Singleton
@Slf4j
public class HostingService {
    private String hostname = null;
    private String nodeIdentifier = null;


    public synchronized String getNodeIdentifier() {
        if (nodeIdentifier == null) {
            nodeIdentifier = getHostname() + "-" + RandomStringUtils.randomNumeric(5);
        }
        return nodeIdentifier;
    }

    public synchronized String getHostname() {
        if (hostname == null) {
            try {
                hostname = Inet4Address.getLocalHost().getHostAddress();
            } catch (Exception e) {
                log.warn("Could not determine host address: {}", e.getMessage(), e);
                //default to localhost
                hostname = "localhost";
            }
        }
        return hostname;
    }
}
