package com.lgc.dspdm.config;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.config.WafByPassRulesProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;

/**
 * Main class to initialize application configurations
 *
 * @author Muhammad Imran Ansari
 * @since 19-May-2021
 */
public class AppConfigInit {
    private static DSPDMLogger logger = new DSPDMLogger(AppConfigInit.class);

    /**
     * should be called once at startup
     *
     * @param propertySource
     * @param executionContext
     */
    public static void init(PropertySource propertySource, ExecutionContext executionContext) {

        try {
            // init connection
            ConnectionProperties.init(propertySource, null, executionContext);
        } catch (Exception e) {
            logger.error("Unable to get configuration for collection name connection");
            DSPDMException.throwException(e, executionContext.getExecutorLocale());
        }

        try {
            // init config
            ConfigProperties.init(propertySource, null, executionContext);
        } catch (Exception e) {
            logger.error("Unable to get configuration for the collection name config");
            DSPDMException.throwException(e, executionContext.getExecutorLocale());
        }

        try {
            // init waf bypass rules
            WafByPassRulesProperties.init(propertySource, null, executionContext);
        } catch (Exception e) {
            logger.error("Unable to get configuration for the collection WAF By Pass Rules");
            DSPDMException.throwException(e, executionContext.getExecutorLocale());
        }
    }
}
