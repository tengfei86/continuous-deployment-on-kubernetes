package com.lgc.dspdm.msp.mainservice;


import java.net.URI;

import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;


class GrizzlyServer {
    protected static DSPDMLogger logger = new DSPDMLogger(GrizzlyServer.class);

    public static final String HOST = "http://0.0.0.0:";
    public static String DEFAULT_PORT = "8086";
    public static final String ROOT_PATH = "/msp";

    public static void main(String[] args) {
        try {
            for(int i=0; i<args.length; i++){
                // if we get -p PORT from cmd, we'll get this PORT otherwise the default port is 8086
                if(args[i].equalsIgnoreCase("-p")){
                    DEFAULT_PORT = args[i+1];
                    break;
                }
            }
            String baseURI = HOST + DEFAULT_PORT + ROOT_PATH;
            final HttpServer httpServer = startServer(baseURI);
            // add jvm shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    logger.info("Shutting down the application...");
                    httpServer.shutdownNow();
                    logger.info("Done, exit.");
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }));
            logger.info(String.format("Application started.%nStop the application using CTRL+C"));
            // block and wait shut down signal, like CTRL+C
            Thread.currentThread().join();
        } catch (InterruptedException ex) {
            logger.error(ex.getMessage(), ex);
        }
    }
    // Starts Grizzly HTTP server
    public static HttpServer startServer(String baseURI) {
        // scan packages
        final ResourceConfig config = new ResourceConfig();
        config.register(MultiPartFeature.class);
        config.register(mainserviceImpl.class);
        config.register(MetadataChangeRestServiceImpl.class);
        //config.register(dspdmRestApi.class);
        logger.info("Starting Server........");
        final HttpServer httpServer =
                GrizzlyHttpServerFactory.createHttpServer(URI.create(baseURI), config);
        return httpServer;
    }
}
