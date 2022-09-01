package com.lgc.dspdm.core.common.config;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * @author Muhammad Imran Ansari
 */
public class ConfigProperties extends AbstractBaseProperties {

    private static DSPDMLogger logger = new DSPDMLogger(ConfigProperties.class);
    private static ConfigProperties singleton = null;

    /**
     * main properties
     */
    public ConfigProperty version = new ConfigProperty("version");
    public ConfigProperty is_security_enabled = new ConfigProperty("is_security_enabled");
    public ConfigProperty allow_sql_stats_collection = new ConfigProperty("allow_sql_stats_collection");
    public ConfigProperty print_exception_stack_trace_in_response = new ConfigProperty("print_exception_stack_trace_in_response");
    public ConfigProperty read_before_update = new ConfigProperty("read_before_update");
    public ConfigProperty do_tiny_update = new ConfigProperty("do_tiny_update");
    public ConfigProperty default_max_records_to_read = new ConfigProperty("default_max_records_to_read");
    public ConfigProperty max_read_parent_hierarchy_level = new ConfigProperty("max_read_parent_hierarchy_level");
    public ConfigProperty fill_default_values_from_metadata = new ConfigProperty("fill_default_values_from_metadata");
    public ConfigProperty use_utc_timezone_to_save = new ConfigProperty("use_utc_timezone_to_save");
    public ConfigProperty use_client_timezone_to_display = new ConfigProperty("use_client_timezone_to_display");
    public ConfigProperty use_nvarchar_for_mssqlserver = new ConfigProperty("use_nvarchar_for_mssqlserver");
    public ConfigProperty use_unit_sources = new ConfigProperty("use_unit_sources");
    public ConfigProperty use_unit_base_type_ids = new ConfigProperty("use_unit_base_type_ids");
    public ConfigProperty max_page_size = new ConfigProperty("max_page_size");
    public ConfigProperty max_records_to_import = new ConfigProperty("max_records_to_import");
    public ConfigProperty max_records_to_export = new ConfigProperty("max_records_to_export");
    public ConfigProperty max_records_to_read = new ConfigProperty("max_records_to_read");
    public ConfigProperty max_records_to_save = new ConfigProperty("max_records_to_save");
    public ConfigProperty max_records_to_parse = new ConfigProperty("max_records_to_parse");
    public ConfigProperty max_jsonb_length_to_save = new ConfigProperty("max_jsonb_length_to_save");
    public ConfigProperty max_bytes_to_save = new ConfigProperty("max_bytes_to_save");
    public ConfigProperty max_allowed_length_for_string_data_type = new ConfigProperty("max_allowed_length_for_string_data_type");
    public ConfigProperty max_records_for_reference_data = new ConfigProperty("max_records_for_reference_data");
    public ConfigProperty export_streaming_rows_window_size = new ConfigProperty("export_streaming_rows_window_size");
    public ConfigProperty mssqlserver_use_default_collation = new ConfigProperty("mssqlserver_use_default_collation");
    public ConfigProperty mssqlserver_overrided_collation_name = new ConfigProperty("mssqlserver_overrided_collation_name");
    public ConfigProperty read_existing_search_record_index_before_insert_new = new ConfigProperty("read_existing_search_record_index_before_insert_new");
    public ConfigProperty allow_read_record_permission_by_default = new ConfigProperty("allow_read_record_permission_by_default", Boolean.TRUE.toString());
    public ConfigProperty generate_metadata_reference_table_prefix = new ConfigProperty("generate_metadata_reference_table_prefix");
    public ConfigProperty use_postgresql_search_db = new ConfigProperty("use_postgresql_search_db", Boolean.TRUE.toString());
    public ConfigProperty history_change_track_enabled = new ConfigProperty("history_change_track_enabled", Boolean.TRUE.toString());
    private ConfigProperties() {
        super();
    }

    public static ConfigProperties getInstance() {
        if (singleton == null) {
            throw new DSPDMException("Config properties are not initialized.", Locale.getDefault());
        }
        return (ConfigProperties) singleton;
    }

    public static void reload(PropertySource propertySource, Map<String, String> map, ExecutionContext executionContext) {
        init(propertySource, map, executionContext);
    }

    public static void init(PropertySource propertySource, Map<String, String> map, ExecutionContext executionContext) {
        ConfigProperties newSingleton = new ConfigProperties();
        newSingleton.initialize(map, executionContext);
        if (singleton != null) {
            synchronized (singleton) {
                singleton = newSingleton;
            }
        } else {
            synchronized (ConfigProperties.class) {
                singleton = newSingleton;
            }
        }
    }

    private void initialize(Map<String, String> map, ExecutionContext executionContext) {
        try {
            if (CollectionUtils.hasValue(map)) {
                super.initialize(PropertySource.MAP_PROVIDED, map, null, executionContext);
                logger.info("===============================================");
                logger.info("All config properties loaded from provided config map successfully.");
                logger.info("===============================================");
            } else {
                throw new DSPDMException("Config map not provided.", executionContext.getExecutorLocale());
            }
        } catch (Throwable mapException) {
            try {
                super.initialize(PropertySource.FILE, null, DSPDMConstants.ConfigFileNames.CONFIG_FILE_NAME, executionContext);
                logger.info("===============================================");
                logger.info("All config properties loaded from file successfully.");
                logger.info("===============================================");
            } catch (Throwable fileNotFoundException) {
                fileNotFoundException.addSuppressed(mapException);
                try {
                    super.initialize(PropertySource.ENV_VARS, null, null, executionContext);
                    logger.info("===============================================");
                    logger.info("All config properties loaded from environment variables successfully.");
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
        logger.info("Cannot add an unknown property to config properties. Property name : " + key);
    }

    public class ConfigProperty implements BaseProperty {

        private ConfigProperty(String propertyKey) {
            this.propertyKey = propertyKey;
            if (!ConfigProperties.super.values().contains(this)) {
                ConfigProperties.super.values().add(this);
            }
        }

        private ConfigProperty(String propertyKey, String defaultValue) {
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
                throw new DSPDMException("Config property value is used before its initialization for key '{}'", Locale.ENGLISH, propertyKey);
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
            ConfigProperty that = (ConfigProperty) o;
            return Objects.equals(propertyKey, that.propertyKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKey);
        }
    }
}
