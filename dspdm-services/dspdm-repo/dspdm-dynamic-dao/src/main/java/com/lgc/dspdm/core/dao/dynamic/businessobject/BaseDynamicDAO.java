package com.lgc.dspdm.core.dao.dynamic.businessobject;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.*;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * This class is used for the dynamic dao processing using the metadata inside this class.
 * This is the main class serving as the base class for all dynamic dao classes.
 * The metadata in this class is initialized for all the business object dynamic dao instances at startup of application from DAO Factory build or rebuild method.
 * This metadata is later used by child classes to read write the data.
 *
 * @author Muhammad Imran Ansari
 * @date 17-Jun-2019
 */
public abstract class BaseDynamicDAO extends MetadataCacheDAO implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(BaseDynamicDAO.class);
    // hash map to hold all the columns. Key is the value inside the database column bo_attr_name 
    private BusinessObjectDTO businessObjectDTO = null;
    private Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = null;
    private Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap = null;
    private Map<String, Object> defaultValuesMap = null;
    private Set<String> physicalAttributeNames = null;
    private List<BusinessObjectAttributeDTO> businessObjectAttributeDTOS = null;
    private List<BusinessObjectAttributeConstraintsDTO> businessObjectAttributeConstraintsDTOS = null;
    private List<BusinessObjectAttributeSearchIndexesDTO> businessObjectAttributeSearchIndexesDTOS = null;
    private List<BusinessObjectRelationshipDTO> businessObjectRelationshipDTOS = null;
    private List<BusinessObjectAttributeUnitDTO> businessObjectAttributeUnitDTOS = null;
    private List<BusinessObjectAttributeDTO> primaryKeyColumns = null;
    private List<String> primaryKeyColumnNames = null;
    private List<BusinessObjectAttributeDTO> mandatoryColumns = null;
    // bo name represented by this dynamic DAO instance
    private String type = null;

    protected BaseDynamicDAO(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
        this.businessObjectDTO = businessObjectDTO;
        this.type = businessObjectDTO.getBoName();
        boAttributeNamesMetadataMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        physicalColumnNamesMetadataMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        physicalAttributeNames = new LinkedHashSet<>();
        businessObjectAttributeDTOS = new ArrayList<>();
        businessObjectAttributeConstraintsDTOS = new ArrayList<>();
        businessObjectAttributeSearchIndexesDTOS = new ArrayList<>();
        businessObjectRelationshipDTOS = new ArrayList<>();
        businessObjectAttributeUnitDTOS = new ArrayList<>();
        primaryKeyColumns = new ArrayList<>(1);
        primaryKeyColumnNames = new ArrayList<>(1);
        mandatoryColumns = new ArrayList<>(5);
        defaultValuesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    /**
     * @return bo name
     * @author Muhammad Imran Ansari
     * @date 20-Jun-2019
     */
    @Override
    protected String getType() {
        return type;
    }

    public BusinessObjectDTO getBusinessObjectDTO() {
        return businessObjectDTO;
    }

    protected static IDynamicDAO getDynamicDAO(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext);
    }

    protected static IDynamicDAOImpl getDynamicDAOImpl(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAOImpl(boName, executionContext);
    }

    /**
     * Registers a column to a bo
     *
     * @param columnName
     * @param boAttrDTO
     * @author Muhammad Imran Ansari
     * @date 20-Jun-2019
     */
    @Override
    protected void addColumn(String columnName, BusinessObjectAttributeDTO boAttrDTO, ExecutionContext executionContext) {
        if (boAttributeNamesMetadataMap.containsKey(columnName)) {
            throw new DSPDMException("Cannot register column '{}'. Column already exists for BO '{}'", executionContext.getExecutorLocale(), columnName, type);
        }
        boAttributeNamesMetadataMap.put(columnName, boAttrDTO);
        physicalColumnNamesMetadataMap.put(boAttrDTO.getAttributeName(), boAttrDTO);

        physicalAttributeNames.add(boAttrDTO.getAttributeName());
        businessObjectAttributeDTOS.add(boAttrDTO);
        if (boAttrDTO.getPrimaryKey()) {
            primaryKeyColumns.add(boAttrDTO);
            primaryKeyColumnNames.add(boAttrDTO.getBoAttrName());
        }

        if ((StringUtils.hasValue(boAttrDTO.getAttributeDefault())) && (!("NULL".equalsIgnoreCase(boAttrDTO.getAttributeDefault())))) {
            Object defaultValue = null;
            if (boAttrDTO.getJavaDataType() == java.lang.String.class) {
                defaultValue = boAttrDTO.getAttributeDefault();
            } else {
                // added check for non-pk fields. because all pk fields are sequence next value based
                if (!(boAttrDTO.getPrimaryKey())) {
                    try {
                        defaultValue = MetadataUtils.convertValueToJavaDataTypeFromString(boAttrDTO.getAttributeDefault(), boAttrDTO.getJavaDataType(), boAttrDTO.getBoName(), boAttrDTO.getBoAttrName(), executionContext);
                    } catch (Exception e) {
                        // each exception and print message
                        if ((!(boAttrDTO.getAttributeDefault() instanceof String)) || (!(((String) boAttrDTO.getAttributeDefault()).contains("("))))
                            logger.debug("Cannot convert default value : '{}' to data type : '{}' for attribute : '{}' and business object : '{}'",
                                    boAttrDTO.getAttributeDefault(), boAttrDTO.getJavaDataType().getSimpleName(), boAttrDTO.getBoAttrName(), boAttrDTO.getBoName());
                    }
                }
            }
            defaultValuesMap.put(boAttrDTO.getBoAttrName(), defaultValue);
        } else if (boAttrDTO.getMandatory()) {
            mandatoryColumns.add(boAttrDTO);
        }
        // add column to the cache
        super.addColumn(columnName, boAttrDTO, executionContext);
    }

    @Override
    protected void addConstraints(BusinessObjectAttributeConstraintsDTO boAttributeConstraintsDTO, ExecutionContext executionContext) {
        if (businessObjectAttributeConstraintsDTOS.contains(boAttributeConstraintsDTO)) {
            throw new DSPDMException("Cannot register constraint '{}'. Constraint already exists for BO '{}' and BO Attribute '{}'", executionContext.getExecutorLocale(), boAttributeConstraintsDTO.getConstraintName(), boAttributeConstraintsDTO.getBoName(), boAttributeConstraintsDTO.getBoAttrName());
        }
        businessObjectAttributeConstraintsDTOS.add(boAttributeConstraintsDTO);
        // add constraints to the cache
        super.addConstraints(boAttributeConstraintsDTO, executionContext);
    }

    @Override
    protected void addSearchIndexes(BusinessObjectAttributeSearchIndexesDTO boAttributeSearchIndexesDTO, ExecutionContext executionContext) {
        if (businessObjectAttributeSearchIndexesDTOS.contains(boAttributeSearchIndexesDTO)) {
            throw new DSPDMException("Cannot register search index '{}'. Index already exists for BO '{}' and BO Attribute '{}'", executionContext.getExecutorLocale(), boAttributeSearchIndexesDTO.getIndexName(), boAttributeSearchIndexesDTO.getBoName(), boAttributeSearchIndexesDTO.getBoAttrName());
        }
        businessObjectAttributeSearchIndexesDTOS.add(boAttributeSearchIndexesDTO);
        // add constraints to the cache
        super.addSearchIndexes(boAttributeSearchIndexesDTO, executionContext);
    }

    @Override
    protected void addRelationship(BusinessObjectRelationshipDTO boRelationshipDTO, ExecutionContext executionContext) {
        if (businessObjectRelationshipDTOS.contains(boRelationshipDTO)) {
            throw new DSPDMException("Cannot register relationship '{}'. Relationship already '{}'", executionContext.getExecutorLocale(), boRelationshipDTO.getName(), boRelationshipDTO.getName());
        }
        businessObjectRelationshipDTOS.add(boRelationshipDTO);
        // add constraints to the cache
        super.addRelationship(boRelationshipDTO, executionContext);
    }

    @Override
    protected void addAttributeUnit(BusinessObjectAttributeUnitDTO attributeUnitDTO, ExecutionContext executionContext) {
        if (businessObjectAttributeUnitDTOS.contains(attributeUnitDTO)) {
            throw new DSPDMException("Cannot register an attribute unit for BO name '{}' and attribute name '{}'. Attribute Unit already registered", executionContext.getExecutorLocale(), attributeUnitDTO.getBoName(), attributeUnitDTO.getBoAttrName());
        }
        businessObjectAttributeUnitDTOS.add(attributeUnitDTO);
        // add constraints to the cache
        super.addAttributeUnit(attributeUnitDTO, executionContext);
    }

    protected BusinessObjectAttributeDTO getColumn(String columnName) {
        return boAttributeNamesMetadataMap.get(columnName);
    }

    public Set<String> getPhysicalAttributeNames() {
        return physicalAttributeNames;
    }

    public List<BusinessObjectAttributeDTO> getBusinessObjectAttributeDTOS() {
        return businessObjectAttributeDTOS;
    }

    public List<BusinessObjectAttributeConstraintsDTO> getBusinessObjectAttributeConstraintsDTOS() {
        return businessObjectAttributeConstraintsDTOS;
    }

    public List<BusinessObjectAttributeSearchIndexesDTO> getBusinessObjectAttributeSearchIndexesDTOS() {
        return businessObjectAttributeSearchIndexesDTOS;
    }

    public List<BusinessObjectRelationshipDTO> getBusinessObjectRelationshipDTOS() {
        return businessObjectRelationshipDTOS;
    }

    public List<BusinessObjectAttributeUnitDTO> getBusinessObjectAttributeUnitDTOS() {
        return businessObjectAttributeUnitDTOS;
    }

    public Map<String, BusinessObjectAttributeDTO> getBoAttributeNamesMetadataMap() {
        return boAttributeNamesMetadataMap;
    }

    public Map<String, BusinessObjectAttributeDTO> getPhysicalColumnNamesMetadataMap() {
        return physicalColumnNamesMetadataMap;
    }

    /* ******************************************************** */
    /* ****************** Metadata Methods ********************* */
    /* ******************************************************** */

    /**
     * Returns physical database table name taking from the first column in the list
     *
     * @return
     * @author Muhammad Imran Ansari
     * @date 20-Jun-2019
     */
    protected String getDatabaseTableName() {
        return businessObjectAttributeDTOS.get(0).getEntityName();
    }

    /**
     * Returns the list of columns involved in primary key
     *
     * @return
     * @author Muhammad Imran Ansari
     * @date 20-Jun-2019
     */
    protected List<BusinessObjectAttributeDTO> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    @Override
    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    protected boolean isCompositePrimaryKey() {
        return (getPrimaryKeyColumns().size() > 1);
    }

    protected List<BusinessObjectAttributeDTO> getMandatoryColumns() {
        return mandatoryColumns;
    }

    protected Map<String, Object> getDefaultValuesMap() {
        return defaultValuesMap;
    }

    protected List<BusinessObjectRelationshipDTO> getBusinessObjectRelationshipDTOS(String parentBoName, String childBoName, Boolean isPrimaryKey) {
        List<BusinessObjectRelationshipDTO> list = new ArrayList<>(3);
        for (BusinessObjectRelationshipDTO relationshipDTO : businessObjectRelationshipDTOS) {
            if ((relationshipDTO.getParentBoName().equalsIgnoreCase(parentBoName)) && (relationshipDTO.getChildBoName().equalsIgnoreCase(childBoName))) {
                if ((isPrimaryKey == null) || (relationshipDTO.getPrimaryKeyRelationship().equals(isPrimaryKey))) {
                    list.add(relationshipDTO);
                }
            }
        }
        return list;
    }

    /**
     * If no order is specified in a read request then this order will be applied. Making here and storing to save time on each call.
     *
     * @return
     */
    protected String getDefaultOrderByClause(BusinessObjectInfo businessObjectInfo) {
        String defaultOrderByClause = null;
        if (CollectionUtils.hasValue(primaryKeyColumns)) {
            List<String> physicalNames = new ArrayList<>(primaryKeyColumns.size());
            for (BusinessObjectAttributeDTO attributeDTO : primaryKeyColumns) {
                if ((businessObjectInfo.isJoiningRead()) && (StringUtils.hasValue(businessObjectInfo.getAlias()))) {
                    physicalNames.add(businessObjectInfo.getAlias() + "." + attributeDTO.getAttributeName());
                } else {
                    physicalNames.add(attributeDTO.getAttributeName());
                }
            }
            String joinedWithComma = CollectionUtils.getCommaSeparated(physicalNames);
            if (StringUtils.hasValue(joinedWithComma)) {
                defaultOrderByClause = " ORDER BY " + joinedWithComma;
            }
        } else {
            defaultOrderByClause = " ORDER BY 1";
        }
        return defaultOrderByClause;
    }

    protected static boolean isChangeHistoryTrackEnabledForBoName(String boName, ExecutionContext executionContext) {
        boolean flag = false;
        if ((!(executionContext.isUnitTestCall())) && (ConfigProperties.getInstance().history_change_track_enabled.getBooleanValue())) {
            if (!(CollectionUtils.containsIgnoreCase(DSPDMConstants.NO_CHANGE_TRACK_BO_NAMES, boName))) {
                flag = true;
            }
        }
        return flag;
    }

    protected static void addChangeHistoryDTOToExecutionContext(String boName, Integer pkId, String boAttrName, Object oldValue, Object newValue, Timestamp timestamp, Integer businessOperationId, ExecutionContext executionContext) {
        if (oldValue != null) {
            if (!(oldValue instanceof String)) {
                oldValue = MetadataUtils.convertValueToStringFromJavaDataType(oldValue, executionContext);
            }
            if (((String) oldValue).length() > 1000) {
                oldValue = ((String) oldValue).substring(0, 999);
            }
        }
        if (newValue != null) {
            if (!(newValue instanceof String)) {
                newValue = MetadataUtils.convertValueToStringFromJavaDataType(newValue, executionContext);
            }
            if (((String) newValue).length() > 1000) {
                newValue = ((String) newValue).substring(0, 999);
            }
        }
        DynamicDTO changeHistoryDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, null, executionContext);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, businessOperationId);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.PK_ID, pkId);// Primary Key
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrName);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.OLD_VALUE, oldValue);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.CHANGED_VALUE, newValue);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.OPR_SEQUENCE_NUMBER, executionContext.getOperationSequenceNumber());
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_DATE, timestamp);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_BY, executionContext.getExecutorName());
        executionContext.addBusObjAttrChangeHistoryDTOList(changeHistoryDynamicDTO);
    }

    // This will return database name of connected db
    protected String getCurrentUserDBName(Connection connection, ExecutionContext executionContext){
        String databaseName = null;
        try {
            databaseName = connection.getCatalog();
        } catch (SQLException sqlException) {
            DSPDMException.throwException(sqlException, executionContext.getExecutorLocale());
        }
        return databaseName;
    }

    // This will return schema name of connected db
    protected String getCurrentDBSchemaName(Connection connection, ExecutionContext executionContext){
        String schemaName = null;
        try {
            schemaName = connection.getSchema();
        } catch (SQLException sqlException) {
            DSPDMException.throwException(sqlException, executionContext.getExecutorLocale());
        }
        return schemaName;
    }
}
