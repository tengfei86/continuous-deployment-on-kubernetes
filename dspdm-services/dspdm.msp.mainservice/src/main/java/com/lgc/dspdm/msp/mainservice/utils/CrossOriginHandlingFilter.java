package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.List;

@Provider
public class CrossOriginHandlingFilter implements ContainerResponseFilter {
    private DSPDMLogger logger = new DSPDMLogger(CrossOriginHandlingFilter.class);
    
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        List<String> referers = requestContext.getHeaders().get("Host");
        if (CollectionUtils.hasValue(referers)) {
            referers.forEach(referer -> logger.debug("CORS Request referer '{}'", referer));
        }
        responseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
        responseContext.getHeaders().add("Access-Control-Allow-Headers", "origin,accept,authorization,cache-control,content-type,expires,pragma");
        responseContext.getHeaders().add("Access-Control-Allow-Credentials", "true");
        responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD");
        responseContext.getHeaders().add("Access-Control-Max-Age", "1209600");
        // HTTP 1.1
        responseContext.getHeaders().add("Cache-Control", "no-cache, no-store, must-revalidate");
        // HTTP 1.0
        responseContext.getHeaders().add("Pragma", "no-cache");
        // Proxies.
        responseContext.getHeaders().add("Expires", 0);
    }
}
