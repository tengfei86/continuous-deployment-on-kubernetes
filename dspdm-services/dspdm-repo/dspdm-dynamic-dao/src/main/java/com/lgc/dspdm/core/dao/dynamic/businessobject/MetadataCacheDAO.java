package com.lgc.dspdm.core.dao.dynamic.businessobject;

import com.lgc.dspdm.core.common.data.annotation.AnnotationProcessor;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.*;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.ReflectionUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.BaseDAO;

import java.lang.reflect.Field;
import java.util.*;

/**
 * This class is just holding the metadata instances in the form of dynamic dtos.
 * This class serves as just a cache to metadata and serves the data to read requests coming from the service client ends.
 * The metadata in this class is is not being used/involved for any dynamic dao processing and dynamic dao business.
 * The metadata in this class is initialized for all the business object dynamic dao instances at startup of application from DAO Factory build or rebuild method.
 * This metadata is later is not used by child classes to read write the data.
 *
 * @author Muhammad Imran Ansari
 * @since 25-Aug-2019
 */
public abstract class MetadataCacheDAO extends BaseDAO {
    private DynamicDTO businessObject = null;
    private List<String> metadataBOAttrNames = null;
    private List<DynamicDTO> metadataList = null;
    private Map<String, DynamicDTO> metadataMap = null;
    private List<DynamicDTO> activeMetadataList = null;
    private List<DynamicDTO> metadataConstraintsList = null;
    private List<DynamicDTO> metadataSearchIndexesList = null;
    private List<DynamicDTO> activeMetadataConstraintsList = null;
    private List<DynamicDTO> activeMetadataSearchIndexesList = null;
    private List<DynamicDTO> metadataRelationshipsList = null;
    private Map<String, List<DynamicDTO>> metadataParentRelationshipsMap = null;
    private Map<String, List<DynamicDTO>> metadataChildRelationshipsMap = null;
    private List<DynamicDTO> metadataAttributeUnitsList = null;

    public MetadataCacheDAO(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        businessObject = createDynamicDTO(businessObjectDTO, executionContext);
        metadataBOAttrNames = new ArrayList<>();
        metadataList = new ArrayList<>();
        metadataMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        activeMetadataList = new ArrayList<>();
        metadataConstraintsList = new ArrayList<>();
        metadataSearchIndexesList = new ArrayList<>();
        activeMetadataConstraintsList = new ArrayList<>();
        activeMetadataSearchIndexesList = new ArrayList<>();
        metadataRelationshipsList = new ArrayList<>();
        metadataParentRelationshipsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadataChildRelationshipsMap = new LinkedHashMap<>();
        metadataAttributeUnitsList = new ArrayList<>();
    }

    /**
     * it will make everything immutable
     */
    public void makeDAOUnmodifiable() {
        metadataBOAttrNames = Collections.unmodifiableList(metadataBOAttrNames);
        metadataList = Collections.unmodifiableList(metadataList);
        metadataMap = Collections.unmodifiableSortedMap((TreeMap<String, DynamicDTO>) metadataMap);
        activeMetadataList = Collections.unmodifiableList(activeMetadataList);
        metadataConstraintsList = Collections.unmodifiableList(metadataConstraintsList);
        metadataSearchIndexesList = Collections.unmodifiableList(metadataSearchIndexesList);
        activeMetadataConstraintsList = Collections.unmodifiableList(activeMetadataConstraintsList);
        activeMetadataSearchIndexesList = Collections.unmodifiableList(activeMetadataSearchIndexesList);
        metadataRelationshipsList = Collections.unmodifiableList(metadataRelationshipsList);
        metadataParentRelationshipsMap = Collections.unmodifiableSortedMap((TreeMap<String, List<DynamicDTO>>) metadataParentRelationshipsMap);
        metadataChildRelationshipsMap = Collections.unmodifiableMap(metadataChildRelationshipsMap);
        metadataAttributeUnitsList = Collections.unmodifiableList(metadataAttributeUnitsList);
    }

    public DynamicDTO getBusinessObject() {
        return businessObject;
    }

    public List<String> getMetadataBOAttrNames() {
        return metadataBOAttrNames;
    }

    public List<DynamicDTO> getMetadataList() {
        return metadataList;
    }

    public Map<String, DynamicDTO> getMetadataMap() {
        return metadataMap;
    }

    public List<DynamicDTO> getActiveMetadataList() {
        return activeMetadataList;
    }

    public List<DynamicDTO> getMetadataConstraintsList() {
        return metadataConstraintsList;
    }

    public List<DynamicDTO> getMetadataSearchIndexesList() {
        return metadataSearchIndexesList;
    }

    public List<DynamicDTO> getActiveMetadataConstraintsList() { return activeMetadataConstraintsList; }

    public List<DynamicDTO> getActiveMetadataSearchIndexesList() { return activeMetadataSearchIndexesList; }

    public List<DynamicDTO> getMetadataRelationshipsList() {
        return metadataRelationshipsList;
    }

    public Map<String, List<DynamicDTO>> getMetadataParentRelationshipsMap() {
        return metadataParentRelationshipsMap;
    }

    public Map<String, List<DynamicDTO>> getMetadataChildRelationshipsMap() {
        return metadataChildRelationshipsMap;
    }

    public List<DynamicDTO> getMetadataAttributeUnitsList() {
        return metadataAttributeUnitsList;
    }

    protected void addColumn(String columnName, BusinessObjectAttributeDTO boAttrDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = createDynamicDTO(boAttrDTO, executionContext);
        metadataBOAttrNames.add(columnName);
        metadataList.add(dynamicDTO);
        metadataMap.put(boAttrDTO.getBoAttrName(), dynamicDTO);
        if (boAttrDTO.getActive()) {
            activeMetadataList.add(dynamicDTO);
        }
    }

    protected void addConstraints(BusinessObjectAttributeConstraintsDTO boAttributeConstraintsDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = createDynamicDTO(boAttributeConstraintsDTO, executionContext);
        metadataConstraintsList.add(dynamicDTO);
        if (boAttributeConstraintsDTO.getActive()) {
            activeMetadataConstraintsList.add(dynamicDTO);
        }
    }

    protected void addSearchIndexes(BusinessObjectAttributeSearchIndexesDTO boAttributeSearchIndexesDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = createDynamicDTO(boAttributeSearchIndexesDTO, executionContext);
        metadataSearchIndexesList.add(dynamicDTO);
        if (boAttributeSearchIndexesDTO.getActive()) {
            activeMetadataSearchIndexesList.add(dynamicDTO);
        }
    }

    protected void addRelationship(BusinessObjectRelationshipDTO boRelationshipDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = createDynamicDTO(boRelationshipDTO, executionContext);
        metadataRelationshipsList.add(dynamicDTO);
        // if current business object type or the current dao is a child in the current relationship being added then
        // add this relationship to the parents list otherwise add to the children list
        if(businessObject.get(DSPDMConstants.BoAttrName.BO_NAME).equals(dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME))) {
            // current business object is child then the relationship business object is parent. Get parent bo name from relationship
            String parentBoName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
            List<DynamicDTO> metadataParentRelationshipsList = metadataParentRelationshipsMap.get(parentBoName);
            if(metadataParentRelationshipsList == null) {
                metadataParentRelationshipsList = new ArrayList<>();
                metadataParentRelationshipsMap.put(parentBoName, metadataParentRelationshipsList);
            }
            metadataParentRelationshipsList.add(dynamicDTO);
        }
        // do not put in else put a separate if block. If you put in else then self recursive relationship will be left out.
        if(businessObject.get(DSPDMConstants.BoAttrName.BO_NAME).equals(dynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME))) {
            // current business object is parent then the relationship business object is child. Get child bo name from relationship
            String childBoName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
            List<DynamicDTO> metadataChildRelationshipsList = metadataChildRelationshipsMap.get(childBoName);
            if(metadataChildRelationshipsList == null) {
                metadataChildRelationshipsList = new ArrayList<>();
                metadataChildRelationshipsMap.put(childBoName, metadataChildRelationshipsList);
            }
            metadataChildRelationshipsList.add(dynamicDTO);
        }
    }

    protected void addAttributeUnit(BusinessObjectAttributeUnitDTO attributeUnitDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = createDynamicDTO(attributeUnitDTO, executionContext);
        metadataAttributeUnitsList.add(dynamicDTO);
    }

    private DynamicDTO createDynamicDTO(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = new DynamicDTO(MetadataUtils.getBONameForDTO(BusinessObjectDTO.class, executionContext), AnnotationProcessor.getPrimaryKeyColumnNames(BusinessObjectDTO.class, executionContext), executionContext);
        try {
            Field declaredField = null;
            String boAttributeNameForField = null;
            for (BusinessObjectDTO.properties property : BusinessObjectDTO.properties.values()) {
                declaredField = businessObjectDTO.getClass().getDeclaredField(property.name());
                boAttributeNameForField = MetadataUtils.getBOAttributeNameForField(businessObjectDTO.getClass(), declaredField.getName(), executionContext);
                dynamicDTO.put(boAttributeNameForField, ReflectionUtils.getGetterForField(declaredField, executionContext).invoke(businessObjectDTO));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }

    private DynamicDTO createDynamicDTO(BusinessObjectAttributeDTO boAttrDTO, ExecutionContext executionContext) {
        // BO_NAME = BUSINESS_OBJECT_ATTR
        DynamicDTO dynamicDTO = new DynamicDTO(MetadataUtils.getBONameForDTO(BusinessObjectAttributeDTO.class, executionContext), AnnotationProcessor.getPrimaryKeyColumnNames(BusinessObjectAttributeDTO.class, executionContext), executionContext);
        try {
            Field declaredField = null;
            String boAttributeNameForField = null;
            for (BusinessObjectAttributeDTO.properties property : BusinessObjectAttributeDTO.properties.values()) {
                declaredField = boAttrDTO.getClass().getDeclaredField(property.name());
                boAttributeNameForField = MetadataUtils.getBOAttributeNameForField(boAttrDTO.getClass(), declaredField.getName(), executionContext);
                dynamicDTO.put(boAttributeNameForField, ReflectionUtils.getGetterForField(declaredField, executionContext).invoke(boAttrDTO));
            }
            dynamicDTO.put(DSPDMConstants.BoAttrName.JAVA_DATA_TYPE, boAttrDTO.getJavaDataType());
            dynamicDTO.put(DSPDMConstants.BoAttrName.SQL_DATA_TYPE, boAttrDTO.getSqlDataType());
            dynamicDTO.put(DSPDMConstants.BoAttrName.MAX_ALLOWED_LENGTH, boAttrDTO.getMaxAllowedLength());
            dynamicDTO.put(DSPDMConstants.BoAttrName.MAX_ALLOWED_DECIMAL_PLACES, boAttrDTO.getMaxAllowedDecimalPlaces());
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }

    private DynamicDTO createDynamicDTO(BusinessObjectAttributeConstraintsDTO boAttrConstraintsDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = new DynamicDTO(MetadataUtils.getBONameForDTO(BusinessObjectAttributeConstraintsDTO.class, executionContext), AnnotationProcessor.getPrimaryKeyColumnNames(BusinessObjectAttributeConstraintsDTO.class, executionContext), executionContext);
        try {
            Field declaredField = null;
            String boAttributeNameForField = null;
            for (BusinessObjectAttributeConstraintsDTO.properties property : BusinessObjectAttributeConstraintsDTO.properties.values()) {
                declaredField = boAttrConstraintsDTO.getClass().getDeclaredField(property.name());
                boAttributeNameForField = MetadataUtils.getBOAttributeNameForField(boAttrConstraintsDTO.getClass(), declaredField.getName(), executionContext);
                dynamicDTO.put(boAttributeNameForField, ReflectionUtils.getGetterForField(declaredField, executionContext).invoke(boAttrConstraintsDTO));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }

    private DynamicDTO createDynamicDTO(BusinessObjectAttributeSearchIndexesDTO boAttrSearchIndexesDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = new DynamicDTO(MetadataUtils.getBONameForDTO(BusinessObjectAttributeSearchIndexesDTO.class, executionContext), AnnotationProcessor.getPrimaryKeyColumnNames(BusinessObjectAttributeSearchIndexesDTO.class, executionContext), executionContext);
        try {
            Field declaredField = null;
            String boAttributeNameForField = null;
            for (BusinessObjectAttributeSearchIndexesDTO.properties property : BusinessObjectAttributeSearchIndexesDTO.properties.values()) {
                declaredField = boAttrSearchIndexesDTO.getClass().getDeclaredField(property.name());
                boAttributeNameForField = MetadataUtils.getBOAttributeNameForField(boAttrSearchIndexesDTO.getClass(), declaredField.getName(), executionContext);
                dynamicDTO.put(boAttributeNameForField, ReflectionUtils.getGetterForField(declaredField, executionContext).invoke(boAttrSearchIndexesDTO));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }

    private DynamicDTO createDynamicDTO(BusinessObjectRelationshipDTO relationshipDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = new DynamicDTO(MetadataUtils.getBONameForDTO(BusinessObjectRelationshipDTO.class, executionContext), AnnotationProcessor.getPrimaryKeyColumnNames(BusinessObjectRelationshipDTO.class, executionContext), executionContext);
        try {
            Field declaredField = null;
            String boAttributeNameForField = null;
            for (BusinessObjectRelationshipDTO.properties property : BusinessObjectRelationshipDTO.properties.values()) {
                declaredField = relationshipDTO.getClass().getDeclaredField(property.name());
                boAttributeNameForField = MetadataUtils.getBOAttributeNameForField(relationshipDTO.getClass(), declaredField.getName(), executionContext);
                dynamicDTO.put(boAttributeNameForField, ReflectionUtils.getGetterForField(declaredField, executionContext).invoke(relationshipDTO));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }

    private DynamicDTO createDynamicDTO(BusinessObjectAttributeUnitDTO attributeUnitDTO, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = new DynamicDTO(MetadataUtils.getBONameForDTO(BusinessObjectRelationshipDTO.class, executionContext), AnnotationProcessor.getPrimaryKeyColumnNames(BusinessObjectRelationshipDTO.class, executionContext), executionContext);
        try {
            Field declaredField = null;
            String boAttributeNameForField = null;
            for (BusinessObjectAttributeUnitDTO.properties property : BusinessObjectAttributeUnitDTO.properties.values()) {
                declaredField = attributeUnitDTO.getClass().getDeclaredField(property.name());
                boAttributeNameForField = MetadataUtils.getBOAttributeNameForField(attributeUnitDTO.getClass(), declaredField.getName(), executionContext);
                dynamicDTO.put(boAttributeNameForField, ReflectionUtils.getGetterForField(declaredField, executionContext).invoke(attributeUnitDTO));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }
}
