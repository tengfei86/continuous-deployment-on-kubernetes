package com.lgc.dspdm.core.common.config;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;

import java.util.*;

/**
 * @author H200760
 */
public class WafByPassRulesProperties extends AbstractBaseProperties {
    private static DSPDMLogger logger = new DSPDMLogger(WafByPassRulesProperties.class);
    private static WafByPassRulesProperties singleton = null;
    private Map<String, String> wafByPassRulesWhiteListMap = null;
    private List<WAFByPassRuleProperty> wafByPassProperties = null;

    private WafByPassRulesProperties() {
        super();
        wafByPassRulesWhiteListMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        wafByPassProperties = new ArrayList<>(25);
    }

    public static WafByPassRulesProperties getInstance() {
        if (singleton == null) {
            throw new DSPDMException("WafByPassRules properties are not initialized.", Locale.getDefault());
        }
        return (WafByPassRulesProperties) singleton;
    }

    public static void reload(PropertySource propertySource, Map<String, String> map, ExecutionContext executionContext) {
        init(propertySource, map, executionContext);
    }

    public static void init(PropertySource propertySource, Map<String, String> map, ExecutionContext executionContext) {
        WafByPassRulesProperties newSingleton = new WafByPassRulesProperties();
        newSingleton.initialize(map, executionContext);
        if (singleton != null) {
            synchronized (singleton) {
                singleton = newSingleton;
            }
        } else {
            synchronized (WafByPassRulesProperties.class) {
                singleton = newSingleton;
            }
        }
    }

    private void initialize(Map<String, String> map, ExecutionContext executionContext) {
        try {
            if (CollectionUtils.hasValue(map)) {
                super.initialize(PropertySource.MAP_PROVIDED, map, null, executionContext);
                logger.info("===============================================");
                logger.info("All WafByPassRules properties loaded from config map successfully.");
                logger.info("===============================================");
            } else {
                throw new DSPDMException("Config map not provided.", executionContext.getExecutorLocale());
            }
        } catch (Throwable mapException) {
            try {
                super.initialize(PropertySource.FILE, null, DSPDMConstants.ConfigFileNames.WAF_BYPASS_RULES_FILE_NAME, executionContext);
                logger.info("===============================================");
                logger.info("All WafByPassRules properties loaded from file successfully.");
                logger.info("===============================================");
            } catch (Throwable fileNotFoundException) {
                fileNotFoundException.addSuppressed(mapException);
                try {
                    super.initialize(PropertySource.ENV_VARS, null, null, executionContext);
                    logger.info("===============================================");
                    logger.info("All WafByPassRules properties loaded from environment variables successfully.");
                    logger.info("===============================================");
                } catch (Exception exception) {
                    exception.addSuppressed(fileNotFoundException);
                    DSPDMException.throwException(exception, executionContext);
                }
            }
        }
    }

    public Map<String, String> getWafByPassRulesWhiteListMap() {
        return this.wafByPassRulesWhiteListMap;
    }

    public List<WAFByPassRuleProperty> getWafByPassProperties() {
        return wafByPassProperties;
    }

    @Override
    protected void addUnknownProperty(String key, String value) {
        WAFByPassRuleProperty wafByPassProperty = new WAFByPassRuleProperty(key, value);
        wafByPassProperties.add(wafByPassProperty);
        if (!(wafByPassRulesWhiteListMap.containsKey(key))) {
            wafByPassRulesWhiteListMap.put(key, value);
        }
    }

    public class WAFByPassRuleProperty implements BaseProperty {

        private WAFByPassRuleProperty(String propertyKey) {
            this.propertyKey = propertyKey;
            if (!WafByPassRulesProperties.super.values().contains(this)) {
                WafByPassRulesProperties.super.values().add(this);
            }
        }

        private WAFByPassRuleProperty(String propertyKey, String propertyValue) {
            this(propertyKey);
            this.propertyValue = propertyValue;
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
                throw new DSPDMException("WAF property value is used before its initialization for key '{}'", Locale.ENGLISH, propertyKey);
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
            WAFByPassRuleProperty that = (WAFByPassRuleProperty) o;
            return Objects.equals(propertyKey, that.propertyKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKey);
        }

        /**
         * the value which is going to be replaced.
         * It will have some special characters like $$$ etc
         *
         * @return property value
         */
        public String getReplacee() {
            return propertyValue;
        }

        /**
         * It will give you the original value which was earlier and the WAF was not allowing this value by defuault
         *
         * @return property key
         */
        public String getReplacer() {
            return propertyKey;
        }

        /**
         * removes all the extra characters added and returns the new value.
         * If no extra character found then returns the same value unchanged
         *
         * @param text
         * @return converted text
         */
        public String applyWafRule(String text) {
            if (StringUtils.containsIgnoreCase(text, getReplacee())) {
                // converting it to original and removing special characters from in between
                return StringUtils.replaceAllIgnoreCase(text, getReplacee(), getReplacer());
            } else {
                return text;
            }
        }
    }
}
