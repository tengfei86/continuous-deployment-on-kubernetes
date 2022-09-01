package com.lgc.dspdm.core.common.config;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Muhammad Imran Ansari
 */
public abstract class AbstractBaseProperties {

    private static DSPDMLogger logger = new DSPDMLogger(AbstractBaseProperties.class);
    private File file = null;
    private Set<BaseProperty> values = new LinkedHashSet<>(25);

    protected Set<BaseProperty> values() {
        return values;
    }

    protected abstract void addUnknownProperty(String key, String value);

    protected PropertySource initialize(PropertySource propertySource, Map<String, String> configMap, String propertyFileName, ExecutionContext executionContext) {
        PropertySource usedPropertySource = propertySource;
        try {
            switch (propertySource) {
                case MAP_PROVIDED:
                    loadPropertiesFromMap(configMap, executionContext);
                    break;
                case FILE:
                    try {
                        loadPropertiesFromFile(propertyFileName, executionContext);
                    } catch (FileNotFoundException e) {
                        logger.info("Unable to load properties from file : " + e.getMessage());
                        try {
                            loadPropertiesFromEnvironmentVariables(executionContext);
                            usedPropertySource = PropertySource.ENV_VARS;
                        } catch (Exception exception) {
                            logger.error(e.getMessage(), e);
                            exception.addSuppressed(e);
                            throw exception;
                        }
                    }
                    break;
                case ENV_VARS:
                    loadPropertiesFromEnvironmentVariables(executionContext);
                    break;
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return usedPropertySource;
    }

    private void loadPropertiesFromEnvironmentVariables(ExecutionContext executionContext) {
        logger.info("===============================================");
        String propertyValue = null;
        for (BaseProperty baseProperty : this.values()) {
            propertyValue = System.getenv(baseProperty.getPropertyKey());
            if (StringUtils.isNullOrEmpty(propertyValue)) {
                if(StringUtils.isNullOrEmpty(baseProperty.getDefaultValue())) {
                    baseProperty.setPropertyValue(null);
                    throw new DSPDMException("Property value is null for key '{}' in environment variables", executionContext.getExecutorLocale(), baseProperty.getPropertyKey());
                } else {
                    logger.info("Property value for the key '{}' not found in environment variables but setting default value as default value exists", baseProperty.getPropertyKey());
                    propertyValue = baseProperty.getDefaultValue();
                }
            }
            baseProperty.setPropertyValue(propertyValue.trim());
            logger.info("{} : {}", baseProperty.getPropertyKey(), baseProperty.getPropertyValue());
        }
    }

    private void loadPropertiesFromMap(Map<String, String> map, ExecutionContext executionContext) {
        logger.info("===============================================");
        if (CollectionUtils.isNullOrEmpty(map)) {
            throw new DSPDMException("Provided config map is empty. please switch to other options to initialize.", executionContext.getExecutorLocale());
        }
        if (this.values().size() > 0) {
            String propertyValue = null;
            for (BaseProperty baseProperty : this.values()) {
                propertyValue = map.get(baseProperty.getPropertyKey());
                if (StringUtils.isNullOrEmpty(propertyValue)) {
                    if (StringUtils.isNullOrEmpty(baseProperty.getDefaultValue())) {
                        baseProperty.setPropertyValue(null);
                        throw new DSPDMException("Property value is null for key '{}' in config map", executionContext.getExecutorLocale(), baseProperty.getPropertyKey());
                    } else {
                        logger.info("Property value for the key '{}' not found in config map but setting default value as default value exists", baseProperty.getPropertyKey());
                        propertyValue = baseProperty.getDefaultValue();
                    }
                }
                baseProperty.setPropertyValue(propertyValue.trim());
                logger.info("{} : {}", baseProperty.getPropertyKey(), baseProperty.getPropertyValue());
            }
        } else {
            for (String propertyName : map.keySet()) {
                this.addUnknownProperty(propertyName, map.get(propertyName));
            }
        }
    }

    private void loadPropertiesFromFile(String propertyFileName, ExecutionContext executionContext) throws FileNotFoundException {
        InputStream inputStream = null;
        try {
            file = PropertiesUtils.getConfigFileFromExternalLocation(propertyFileName);
            if (file != null) {
                try {
                    inputStream = new FileInputStream(file);
                    logger.info("===============================================");
                    logger.info("Going to read property file located at absolute path found successfully : {}", file.getPath());
                    logger.info("===============================================");
                } catch (Exception e) {
                    // eat exception here because we will try the class path method now as external did not work
//                    logger.error(e);
                    file = null;
                    throw new FileNotFoundException("Properties file '" + propertyFileName + "' not found.");
                }
                // load contents
                loadPropertiesFromInputStream(inputStream, ExecutionContext.getSystemUserExecutionContext());
            } else {
                throw new FileNotFoundException("Properties file '" + propertyFileName + "' not found.");
            }
//            if (inputStream == null) {
//                inputStream = PropertiesUtils.getConfigFileInputStreamFromClassPath(propertyFileName);
//                logger.info("===============================================");
//                logger.info("Going to read property file located at java class path.");
//                logger.info("===============================================");
//                if (inputStream == null) {
//                    logger.fatal("Properties file not found in class path : {}", propertyFileName);
//                    throw new FileNotFoundException("Properties file '" + propertyFileName + "' not found on both places inside the app and outside the app");
//                }
//                loadPropertiesFromInputStream(inputStream, ExecutionContext.getSystemUserExecutionContext());
//            }

        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void loadPropertiesFromInputStream(InputStream inputStream, ExecutionContext executionContext) {
        OrderedProperties properties = PropertiesUtils.loadProperties(inputStream, executionContext);
        logger.info("===============================================");
        if(this.values().size() > 0) {
            String propertyValue = null;
            for (BaseProperty baseProperty : this.values()) {
                propertyValue = properties.getProperty(baseProperty.getPropertyKey());
                if (StringUtils.isNullOrEmpty(propertyValue)) {
                    if (StringUtils.isNullOrEmpty(baseProperty.getDefaultValue())) {
                        baseProperty.setPropertyValue(null);
                        throw new DSPDMException("Property value is null for key '{}' with file '{}'", executionContext.getExecutorLocale(), baseProperty.getPropertyKey(), file.getPath());
                    } else {
                        logger.info("Property value for the key '{}' not found in file but setting default value as default value exists", baseProperty.getPropertyKey());
                        propertyValue = baseProperty.getDefaultValue();
                    }
                }
                baseProperty.setPropertyValue(propertyValue.trim());
                logger.info("{} : {}", baseProperty.getPropertyKey(), baseProperty.getPropertyValue());
            }
        } else {
            for (String propertyName : properties.stringPropertyNames()) {
                this.addUnknownProperty(propertyName, properties.getProperty(propertyName));
            }
        }
    }
}
