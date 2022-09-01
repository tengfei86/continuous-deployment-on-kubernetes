package com.lgc.dspdm.core.common.config;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * @author H200760
 */
public class ConnectionProperties extends AbstractBaseProperties {
    private static DSPDMLogger logger = new DSPDMLogger(ConnectionProperties.class);
    private static ConnectionProperties singleton = null;

    /**
     * jdbc connection properties
     */
    public ConnectionProperty env = new ConnectionProperty("env");
    // data model db configurations
    public ConnectionProperty data_model_db_sql_dialect = new ConnectionProperty("data_model_db_sql_dialect");
    public ConnectionProperty data_model_db_min_connection_pool_size = new ConnectionProperty("data_model_db_min_connection_pool_size");
    public ConnectionProperty data_model_db_max_connection_pool_size = new ConnectionProperty("data_model_db_max_connection_pool_size");
    public ConnectionProperty data_model_db_transaction_timeout_seconds = new ConnectionProperty("data_model_db_transaction_timeout_seconds");
    public ConnectionProperty data_model_db_jdbc_driver_class = new ConnectionProperty("data_model_db_jdbc_driver_class");
    public ConnectionProperty data_model_db_jdbc_url = new ConnectionProperty("data_model_db_jdbc_url");
    public ConnectionProperty data_model_db_jdbc_user = new ConnectionProperty("data_model_db_jdbc_user");
    public ConnectionProperty data_model_db_jdbc_password = new ConnectionProperty("data_model_db_jdbc_password");
    // service db configurations
    public ConnectionProperty service_db_sql_dialect = new ConnectionProperty("service_db_sql_dialect");
    public ConnectionProperty service_db_jdbc_driver_class = new ConnectionProperty("service_db_jdbc_driver_class");
    public ConnectionProperty service_db_jdbc_url = new ConnectionProperty("service_db_jdbc_url");
    public ConnectionProperty service_db_jdbc_user = new ConnectionProperty("service_db_jdbc_user");
    public ConnectionProperty service_db_jdbc_password = new ConnectionProperty("service_db_jdbc_password");
    public ConnectionProperty service_db_min_connection_pool_size = new ConnectionProperty("service_db_min_connection_pool_size");
    public ConnectionProperty service_db_max_connection_pool_size = new ConnectionProperty("service_db_max_connection_pool_size");

    private ConnectionProperties() {
        super();
    }

    public static boolean isPostgresDialect() {
        return getInstance().data_model_db_sql_dialect.getPropertyValue().trim().equalsIgnoreCase(DSPDMConstants.SQL.SQLDialect.POSTGRES);
    }

    public static boolean isMSSQLServerDialect() {
        return getInstance().data_model_db_sql_dialect.getPropertyValue().trim().equalsIgnoreCase(DSPDMConstants.SQL.SQLDialect.MSSQLSERVER);
    }

    public static boolean isMSSQLServerMetadataDialect() {
        return getInstance().service_db_sql_dialect.getPropertyValue().trim().equalsIgnoreCase(DSPDMConstants.SQL.SQLDialect.MSSQLSERVER);
    }

    public static ConnectionProperties getInstance() {
        if (singleton == null) {
            throw new DSPDMException("Connection properties are not initialized.", Locale.getDefault());
        }
        return (ConnectionProperties) singleton;
    }

    public static void reload(PropertySource propertySource, Map<String, String> map, ExecutionContext executionContext) {
        init(propertySource, map, executionContext);
    }

    public static void init(PropertySource propertySource, Map<String, String> map, ExecutionContext executionContext) {
        ConnectionProperties newSingleton = new ConnectionProperties();
        newSingleton.initialize(map, executionContext);
        if (singleton != null) {
            synchronized (singleton) {
                singleton = newSingleton;
            }
        } else {
            synchronized (ConnectionProperties.class) {
                singleton = newSingleton;
            }
        }
    }

    private void initialize(Map<String, String> map, ExecutionContext executionContext) {
        try {
            if (CollectionUtils.hasValue(map)) {
                super.initialize(PropertySource.MAP_PROVIDED, map, null, executionContext);
                logger.info("===============================================");
                logger.info("All connection properties loaded from config map successfully.");
                logger.info("===============================================");
            } else {
                throw new DSPDMException("Config map not provided.", executionContext.getExecutorLocale());
            }
        } catch (Throwable mapException) {
            try {
                super.initialize(PropertySource.FILE, null, DSPDMConstants.ConfigFileNames.CONNECTION_FILE_NAME, executionContext);
                logger.info("===============================================");
                logger.info("All connection properties loaded from file successfully.");
                logger.info("===============================================");
            } catch (Throwable fileNotFoundException) {
                fileNotFoundException.addSuppressed(mapException);
                try {
                    super.initialize(PropertySource.ENV_VARS, null, null, executionContext);
                    logger.info("===============================================");
                    logger.info("All connection properties loaded from environment variables successfully.");
                    logger.info("===============================================");
                } catch (Exception exception) {
                    exception.addSuppressed(fileNotFoundException);
                    DSPDMException.throwException(exception, executionContext);
                }
            }
        }
    }

    @Override
    protected void addUnknownProperty(String key, String value) {
        logger.info("Cannot add an unknown property to connection properties. Property name : " + key);
    }

    public class ConnectionProperty implements BaseProperty {

        private ConnectionProperty(String propertyKey) {
            this.propertyKey = propertyKey;
            if (!ConnectionProperties.super.values().contains(this)) {
                ConnectionProperties.super.values().add(this);
            }
        }

        private ConnectionProperty(String propertyKey, String defaultValue) {
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
                throw new DSPDMException("Connection property value is used before its initialization for key '{}'", Locale.ENGLISH, propertyKey);
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
            ConnectionProperty that = (ConnectionProperty) o;
            return Objects.equals(propertyKey, that.propertyKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKey);
        }
    }
}
