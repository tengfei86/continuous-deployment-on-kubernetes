package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.config.ConfigProperties;

import jakarta.ws.rs.core.Response;
import org.apache.commons.cli.CommandLine;


/**
 * Defines a Microservice application and provides basic information about the application.
 *
 * @author service developer
 *
 */

public class Application {
    public AppInfo start(CommandLine cmd) {
        return new AppInfo() {
            @Override
            public String getAppTitle() {
                return "MSDP Microservice Example";
            }

            @Override
            public String getAppName() {
                return "mainservice";
            }

            @Override
            public String getAppDescription() {
                return "A simple Microservice example";
            }

            @Override
            public String getAppAuthor() {
                return "Service developer";
            }

            @Override
            public HealthInfo getHealthInfo() {
                return new HealthInfo(HealthInfo.GOOD);
            }

            @Override
            public String getAppVersion() {
                return AppInfo.super.getAppVersion();
            }
        };
    }
}

interface AppInfo {
    default String getAppTitle() {
        return "";
    }

    default String getAppName() {
        return "";
    }

    default String getAppDescription() {
        return "";
    }

    default String getAppAuthor() {
        return "";
    }

    default String getAppHomePath() {
        return "";
    }

    default HealthInfo getHealthInfo() {
        return null;
    }

    default String getMetrics() {
        return null;
    }

    default Response readinessProbe() {
        return Response.ok().build();
    }

    default String[] getResourcePackages() {
        return new String[]{this.getClass().getPackage().getName()};
    }

    default Class[] getComponents() {
        return null;
    }

    default String getHealthCheckPath() {
        return "";
    }

    default String getStatisticsPath() {
        return "";
    }

    default String getAppVersion() {
        return ConfigProperties.getInstance().version.getPropertyValue();
    }
}

class HealthInfo {
    public static String GOOD = "Good";
    public static String WARNING = "Warning";
    public static String BAD = "Bad";
    private String status;

    public HealthInfo(String status) {
        this.status = status;
    }

    public String getStatus() {
        return this.status;
    }
}
