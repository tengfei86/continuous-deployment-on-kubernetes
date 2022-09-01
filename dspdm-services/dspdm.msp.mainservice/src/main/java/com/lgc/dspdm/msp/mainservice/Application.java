package com.lgc.dspdm.msp.mainservice;

import org.apache.commons.cli.CommandLine;

import com.lgc.dist.core.msp.service.AppInfo;
import com.lgc.dist.core.msp.service.HealthInfo;
import com.lgc.dist.core.msp.service.ApplicationAnnotation.MApplication;
import com.lgc.dist.core.msp.service.ApplicationAnnotation.Start;

/**
 * Defines a Microservice application and provides basic information about the application.
 * 
 * @author service developer
 *
 */
@MApplication
public class Application {
	@Start
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
