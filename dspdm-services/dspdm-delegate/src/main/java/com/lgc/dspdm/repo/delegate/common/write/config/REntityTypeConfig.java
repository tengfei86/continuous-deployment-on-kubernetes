package com.lgc.dspdm.repo.delegate.common.write.config;

import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.util.*;
/**
 *
 * Get all the configuration of R_Entity_Type logic processing.
 *
 * R_Entity_Type logic for example Node table：
 *      Node table have 2 columns: entity_name , entity_type in the configs
 *      if (Node.entity_type = ‘Well')
 *          entity_name's source is from Well's 'xxx' column
 *      else if (Node.entity_type = ‘Facility’)
 *          entity_name's source is from Facility's 'xxx' column
 *
 *      Purpose: We need to ensure entity_name value from entity_type table. this relationship will define in the R_Entity_Type table.
 *      R_Entity_Type saved bo_name and default_attribute_name(this is 'xxx' in the example)
 *      So we just need to make sure that Node.entity_name is in 'bo_name'.'default_attribute_name' through Node.entity_type = R_Entity_Type.entity_type
 *
 * @author Changming
 * @since 1-20-2021
 */
public class REntityTypeConfig {

    /**
     * This map saves all bo_names and related bo_attr_names that use R_Entity_Type to implement this specific special logic
     * key = bo_name
     * value = source and target bo_attr_name columns of this bo_name
     */
    private Map<String, List<Node>> configMap;

    private static volatile REntityTypeConfig instance;

    private REntityTypeConfig() {
        loadConfig();
    }

    public static REntityTypeConfig getInstance() {
        if (instance == null) {
            synchronized (REntityTypeConfig.class) {
                if (instance == null) {
                    instance = new REntityTypeConfig();
                }
            }
        }
        return instance;
    }


    public Map<String, List<Node>> getConfigMap() {
        return configMap;
    }

    private void loadConfig() {
        // For the current do not need to read from the configuration file,
        // If necessary in the future, we can update this method read from the configuration file.
        configMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        // Add Node
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(new Node(DSPDMConstants.BoAttrName.ENTITY_TYPE, DSPDMConstants.BoAttrName.ENTITY_NAME));
        configMap.put(DSPDMConstants.BoName.NODE, nodeList);
    }

    public class Node {
        private String sourceBoAttrName;
        private String targetBoAttrName;

        public Node(String sourceBoAttrName, String targetBoAttrName) {
            this.sourceBoAttrName = sourceBoAttrName;
            this.targetBoAttrName = targetBoAttrName;
        }

        public String getSourceBoAttrName() {
            return sourceBoAttrName;
        }

        public void setSourceBoAttrName(String sourceBoAttrName) {
            this.sourceBoAttrName = sourceBoAttrName;
        }

        public String getTargetBoAttrName() {
            return targetBoAttrName;
        }

        public void setTargetBoAttrName(String targetBoAttrName) {
            this.targetBoAttrName = targetBoAttrName;
        }
    }
}
