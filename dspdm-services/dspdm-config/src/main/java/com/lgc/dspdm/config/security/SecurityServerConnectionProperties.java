package com.lgc.dspdm.config.security;

import com.lgc.dspdm.core.common.config.AbstractBaseProperties;
import com.lgc.dspdm.core.common.config.BaseProperty;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.Locale;
import java.util.Objects;

public class SecurityServerConnectionProperties extends AbstractBaseProperties {
    private static DSPDMLogger logger = new DSPDMLogger(SecurityServerConnectionProperties.class);
    private static SecurityServerConnectionProperties singleton = null;

    public SecurityServerConnectionProperty SECURITY_SERVER_USER = new SecurityServerConnectionProperty("SECURITY_SERVER_USER");
    public SecurityServerConnectionProperty SECURITY_SERVER_ENCPASS = new SecurityServerConnectionProperty("SECURITY_SERVER_ENCPASS");
    public SecurityServerConnectionProperty SECURITY_SERVER_REALM = new SecurityServerConnectionProperty("SECURITY_SERVER_REALM");
    public SecurityServerConnectionProperty SECURITY_SERVER_PDM_CLIENT_NAME = new SecurityServerConnectionProperty("SECURITY_SERVER_PDM_CLIENT_NAME");
    public SecurityServerConnectionProperty SECURITY_SERVER_PDM_CLIENT_ID = new SecurityServerConnectionProperty("SECURITY_SERVER_PDM_CLIENT_ID");
    public SecurityServerConnectionProperty SECURITY_SERVER_PDM_CLIENT_SECRET = new SecurityServerConnectionProperty("SECURITY_SERVER_PDM_CLIENT_SECRET");
    public SecurityServerConnectionProperty SECURITY_SERVER_ROOT_URL = new SecurityServerConnectionProperty("SECURITY_SERVER_ROOT_URL");
    public SecurityServerConnectionProperty SECURITY_SERVER_TOKEN_URL_FORMAT = new SecurityServerConnectionProperty("SECURITY_SERVER_TOKEN_URL_FORMAT", "%s/realms/%s/protocol/openid-connect/token");

    private SecurityServerConnectionProperties() {
        super();
    }

    public static SecurityServerConnectionProperties getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            synchronized (SecurityServerConnectionProperties.class) {
                if (singleton == null) {
                    singleton = new SecurityServerConnectionProperties();
                    singleton.initialize(executionContext);
                }
            }
        }
        return singleton;
    }

    public static void reload(ExecutionContext executionContext) {
        SecurityServerConnectionProperties newSingleton = new SecurityServerConnectionProperties();
        newSingleton.initialize(executionContext);
        synchronized (SecurityServerConnectionProperties.class) {
            singleton = newSingleton;
        }
    }

    private void initialize(ExecutionContext executionContext) {
        try {
            logger.info("===============================================");
            logger.info("Going to initialize security server connection properties");
            logger.info("===============================================");
            PropertySource usedPropertySource = initialize(PropertySource.FILE, null, DSPDMConstants.ConfigFileNames.SECURITY_SERVER_FILE_NAME, executionContext);
            logger.info("===============================================");
            if (usedPropertySource == PropertySource.FILE) {
                logger.info("All security server connection properties loaded from file successfully.");
            } else {
                logger.info("All security server connection properties loaded from environment variables successfully.");
            }
            logger.info("===============================================");
        } catch (Throwable fileNotFoundException) {
            try {
                initialize(PropertySource.ENV_VARS, null, null, executionContext);
                logger.info("===============================================");
                logger.info("All security server connection properties loaded from environment variables successfully.");
                logger.info("===============================================");
            } catch (Exception exception) {
                exception.addSuppressed(fileNotFoundException);
                DSPDMException.throwException(exception, executionContext);
            }
        }
    }

    @Override
    protected void addUnknownProperty(String key, String value) {
        logger.info("Cannot add an unknown property to security server connection properties. Property name : " + key);
    }

    public class SecurityServerConnectionProperty implements BaseProperty {

        private SecurityServerConnectionProperty(String propertyKey) {
            this.propertyKey = propertyKey;
            if (!values().contains(this)) {
                values().add(this);
            }
        }

        private SecurityServerConnectionProperty(String propertyKey, String defaultValue) {
            this(propertyKey);
            this.defaultValue = defaultValue;
        }

        private String propertyKey = null;
        private String propertyValue = null;
        private String defaultValue = null;

        @Override
        public String getPropertyKey() {
            return propertyKey;
        }

        @Override
        public String getPropertyValue() {
            if (propertyValue == null) {
                throw new DSPDMException("Security server connection property value is used before its initialization for key '{}'", Locale.ENGLISH, propertyKey);
            }
            return propertyValue;
        }

        @Override
        public String getDefaultValue() {
            return defaultValue;
        }

        @Override
        public void setPropertyValue(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        @Override
        public boolean getBooleanValue() {
            return Boolean.valueOf(getPropertyValue().trim());
        }

        @Override
        public int getIntegerValue() {
            return Integer.parseInt(getPropertyValue());
        }

        @Override
        public float getFloatValue() {
            return Float.parseFloat(getPropertyValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SecurityServerConnectionProperty that = (SecurityServerConnectionProperty) o;
            return Objects.equals(propertyKey, that.propertyKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKey);
        }
    }

}
