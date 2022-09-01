package com.lgc.dspdm.core.dao.dynamic.businessobject;

import com.lgc.dspdm.core.common.data.annotation.AnnotationProcessor;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.*;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.fixed.GenericDAO;

import java.util.*;

public class DynamicDAOFactory {
    private static DSPDMLogger logger = new DSPDMLogger(DynamicDAOFactory.class);
    private static DynamicDAOFactory singleton = null;

    /**
     * the following map must be non-static to be rebuilt in parallel
     */
    private List<DynamicDTO> businessObjects = null;
    /**
     * the following map must be non-static to be rebuilt in parallel
     */
    private Map<String, BusinessObjectDTO> businessObjectsMap = null;
    /**
     * the following map must be non-static to be rebuilt in parallel
     */
    private Map<String, IDynamicDAO> dynamicDAOMap = null;

    private DynamicDAOFactory(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Constructor");
        businessObjects = new ArrayList<>();
        businessObjectsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        dynamicDAOMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.rebuildDAOFactory(executionContext);
        // now dao factory has been built.
        // make everything unmodifiable
        businessObjects = Collections.unmodifiableList(businessObjects);
        businessObjectsMap = Collections.unmodifiableSortedMap((TreeMap<String, BusinessObjectDTO>) businessObjectsMap);
        dynamicDAOMap = Collections.unmodifiableSortedMap((TreeMap<String, IDynamicDAO>) dynamicDAOMap);
        for (IDynamicDAO dao : dynamicDAOMap.values()) {
            ((MetadataCacheDAO) dao).makeDAOUnmodifiable();
        }
    }

    public static synchronized DynamicDAOFactory getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new DynamicDAOFactory(executionContext);
        }
        return singleton;
    }

    public List<DynamicDTO> getBusinessObjects(ExecutionContext executionContext) {
        return businessObjects;
    }

    public Map<String, BusinessObjectDTO> getBusinessObjectsMap(ExecutionContext executionContext) {
        return businessObjectsMap;
    }

    public BusinessObjectDTO getBusinessObjectDTO(String boName, ExecutionContext executionContext) {
        BusinessObjectDTO businessObjectDTO = null;
        if (StringUtils.hasValue(boName)) {
            boName = boName.trim();
            businessObjectDTO = businessObjectsMap.get(boName);
        }
        if (businessObjectDTO == null) {
            if (dynamicDAOMap.size() == 0) {
                throw new DSPDMException("Service not initialized successfully. Please check service startup logs", executionContext.getExecutorLocale(), boName);
            } else {
                throw new DSPDMException("Business object not found for boName {}", executionContext.getExecutorLocale(), boName);
            }
        }
        return businessObjectDTO;
    }

    public IDynamicDAO getDynamicDAO(String boName, ExecutionContext executionContext) {
        IDynamicDAO dynamicDAO = null;
        if (StringUtils.hasValue(boName)) {
            boName = boName.trim();
            dynamicDAO = dynamicDAOMap.get(boName);
        }
        if (dynamicDAO == null) {
            if (dynamicDAOMap.size() == 0) {
                throw new DSPDMException("Service not initialized successfully. Please check service startup logs", executionContext.getExecutorLocale(), boName);
            } else {
                throw new DSPDMException("Business Object '{}' is unknown.", executionContext.getExecutorLocale(), boName);
            }
        }
        return dynamicDAO;
    }

    public IDynamicDAOImpl getDynamicDAOImpl(String boName, ExecutionContext executionContext) {
        IDynamicDAO dynamicDAO = null;
        if (StringUtils.hasValue(boName)) {
            boName = boName.trim();
            dynamicDAO = dynamicDAOMap.get(boName);
        }
        if (dynamicDAO == null) {
            if (dynamicDAOMap.size() == 0) {
                throw new DSPDMException("Service not initialized successfully. Please check service startup logs", executionContext.getExecutorLocale(), boName);
            } else {
                throw new DSPDMException("Business Object '{}' is unknown.", executionContext.getExecutorLocale(), boName);
            }
        }
        return (IDynamicDAOImpl) dynamicDAO;
    }

    public void rebuildDAOFactory(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Rebuild DAO Factory");
        List<BusinessObjectDTO> allBusinessObjects = getAllBusinessObjects(executionContext);
        List<BusinessObjectAttributeDTO> allBusinessObjectAttributes = getAllBusinessObjectAttributes(executionContext);
        List<BusinessObjectAttributeConstraintsDTO> allBusinessObjectAttributeConstraints = getAllBusinessObjectAttributeConstraints(executionContext);
        List<BusinessObjectAttributeSearchIndexesDTO> allBusinessObjectAttributeSearchIndexes = getAllBusinessObjectAttributeSearchIndexes(executionContext);
        List<BusinessObjectRelationshipDTO> allBusinessObjectRelationships = getAllBusinessObjectRelationships(executionContext);
//        List<BusinessObjectAttributeUnitDTO> allBusinessObjectAttributeUnits = getAllBusinessObjectAttributeUnits(executionContext);
        // BUSINESS OBJECTS
        buildBusinessObjects(allBusinessObjects, executionContext);
        // METADATA ATTRIBUTES
        buildMetadata(allBusinessObjectAttributes, executionContext);
        // METADATA CONSTRAINTS
        buildMetadataConstraints(allBusinessObjectAttributeConstraints, executionContext);
        // METADATA SEARCH INDEXES
        buildMetadataSearchIndexes(allBusinessObjectAttributeSearchIndexes, executionContext);
        // METADATA RELATIONSHIPS
        buildMetadataRelationships(allBusinessObjectRelationships, executionContext);
        // METADATA ATTRIBUTE UNITS
//        buildMetadataAttributeUnits(allBusinessObjectAttributeUnits, executionContext);
    }

    public void rebuildDAOFactoryInParalell(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Rebuild DAO Factory In Parallel");
        // build in paralell
        DynamicDAOFactory dynamicDAOFactoryInParalell = new DynamicDAOFactory(executionContext);
        // built in paralell and assigned to singleton
        synchronized (singleton) {
            singleton = dynamicDAOFactoryInParalell;
        }
    }

    private void buildBusinessObjects(List<BusinessObjectDTO> list, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Build Business Objects");
        businessObjectsMap.clear();
        list.forEach(businessObjectDTO -> businessObjectsMap.put(businessObjectDTO.getBoName(), businessObjectDTO));
    }

    private void buildMetadata(List<BusinessObjectAttributeDTO> list, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Build Metadata");
        businessObjects.clear();
        dynamicDAOMap.clear();
        IDynamicDAO dynamicDAO = null;
        String boName = null;
        for (BusinessObjectAttributeDTO boAttrDTO : list) {
            if (StringUtils.isNullOrEmpty(boAttrDTO.getBoName())) {
                throw new DSPDMException("Error in reading business object attributes. BO Name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(boAttrDTO.getBoAttrName())) {
                throw new DSPDMException("Error in reading business object attributes. BO Attribute Name cannot be null or empty", executionContext.getExecutorLocale());
            }
            boName = boAttrDTO.getBoName().trim();
            dynamicDAO = dynamicDAOMap.get(boName);
            if (dynamicDAO == null) {
                dynamicDAO = new DynamicDAO(getBusinessObjectDTO(boName, executionContext), executionContext);
                dynamicDAOMap.put(boName, dynamicDAO);
                businessObjects.add(dynamicDAO.readMetadataBusinessObject(executionContext));
            }
            ((DynamicDAO) dynamicDAO).addColumn(boAttrDTO.getBoAttrName().trim(), boAttrDTO, executionContext);
        }
        logger.info("{} Dynamic DAO metadata loaded successfully", dynamicDAOMap.size());
    }

    private void buildMetadataConstraints(List<BusinessObjectAttributeConstraintsDTO> list, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Build Metadata Constraints");
        IDynamicDAO dynamicDAO = null;
        String boName = null;
        for (BusinessObjectAttributeConstraintsDTO boAttrConsDTO : list) {
            if (StringUtils.isNullOrEmpty(boAttrConsDTO.getBoName())) {
                throw new DSPDMException("Error in reading business object attributes unique constraints. BO Name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(boAttrConsDTO.getBoAttrName())) {
                throw new DSPDMException("Error in reading business object attributes unique constraints. BO Attribute Name cannot be null or empty", executionContext.getExecutorLocale());
            }
            boName = boAttrConsDTO.getBoName().trim();
            dynamicDAO = dynamicDAOMap.get(boName);
            if (dynamicDAO == null) {
                dynamicDAO = new DynamicDAO(getBusinessObjectDTO(boName, executionContext), executionContext);
                dynamicDAOMap.put(boName, dynamicDAO);
                businessObjects.add(dynamicDAO.readMetadataBusinessObject(executionContext));
            }
            ((DynamicDAO) dynamicDAO).addConstraints(boAttrConsDTO, executionContext);
        }
        logger.info("{} Dynamic DAO constraints loaded successfully", dynamicDAOMap.size());
    }

    private void buildMetadataSearchIndexes(List<BusinessObjectAttributeSearchIndexesDTO> list, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Build Metadata Search Indexes");
        IDynamicDAO dynamicDAO = null;
        String boName = null;
        for (BusinessObjectAttributeSearchIndexesDTO boAttrSearchIndexesDTO : list) {
            if (StringUtils.isNullOrEmpty(boAttrSearchIndexesDTO.getBoName())) {
                throw new DSPDMException("Error in reading business object attributes search indexes. BO Name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(boAttrSearchIndexesDTO.getBoAttrName())) {
                throw new DSPDMException("Error in reading business object attributes search indexes. BO Attribute Name cannot be null or empty", executionContext.getExecutorLocale());
            }
            boName = boAttrSearchIndexesDTO.getBoName().trim();
            dynamicDAO = dynamicDAOMap.get(boName);
            if (dynamicDAO == null) {
                dynamicDAO = new DynamicDAO(getBusinessObjectDTO(boName, executionContext), executionContext);
                dynamicDAOMap.put(boName, dynamicDAO);
                businessObjects.add(dynamicDAO.readMetadataBusinessObject(executionContext));
            }
            ((DynamicDAO) dynamicDAO).addSearchIndexes(boAttrSearchIndexesDTO, executionContext);
        }
        logger.info("{} Dynamic DAO search indexes loaded successfully", dynamicDAOMap.size());
    }

    private void buildMetadataRelationships(List<BusinessObjectRelationshipDTO> list, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Build Metadata Business Object Relationships");
        IDynamicDAO dynamicDAO = null;
        String boName = null;
        for (BusinessObjectRelationshipDTO relationshipDTO : list) {
            if (StringUtils.isNullOrEmpty(relationshipDTO.getName())) {
                throw new DSPDMException("Error in reading business object relationships. Relationship name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(relationshipDTO.getChildBoName())) {
                throw new DSPDMException("Error in reading business object relationships. Child BO name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(relationshipDTO.getChildBoAttrName())) {
                throw new DSPDMException("Error in reading business object relationships. Child BO Attr name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(relationshipDTO.getParentBoName())) {
                throw new DSPDMException("Error in reading business object relationships. Parent BO name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(relationshipDTO.getParentBoAttrName())) {
                throw new DSPDMException("Error in reading business object relationships. Parent BO Attr name cannot be null or empty", executionContext.getExecutorLocale());
            }
            // make sure child bo exists
            boName = relationshipDTO.getChildBoName().trim();
            dynamicDAO = dynamicDAOMap.get(boName);
            if (dynamicDAO == null) {
                try {
                    dynamicDAO = new DynamicDAO(getBusinessObjectDTO(boName, executionContext), executionContext);
                    dynamicDAOMap.put(boName, dynamicDAO);
                    businessObjects.add(dynamicDAO.readMetadataBusinessObject(executionContext));
                    // add relationship tp parent dao
                    ((DynamicDAO) dynamicDAO).addRelationship(relationshipDTO, executionContext);
                } catch (Exception e) {
                    throw new DSPDMException("Error in reading business object relationships. {}", e, executionContext.getExecutorLocale(), e.getMessage());
                }
            } else {
                // add relationship to child dao
                ((DynamicDAO) dynamicDAO).addRelationship(relationshipDTO, executionContext);
            }
            // if child and parent bo names are different it means it is not a self join then add the relationship to the parent bo too.
            if (!(relationshipDTO.getChildBoName().trim().equalsIgnoreCase(relationshipDTO.getParentBoName().trim()))) {
                // make sure parent bo exists
                boName = relationshipDTO.getParentBoName().trim();
                dynamicDAO = dynamicDAOMap.get(boName);
                if (dynamicDAO == null) {
                    try {
                        dynamicDAO = new DynamicDAO(getBusinessObjectDTO(boName, executionContext), executionContext);
                        dynamicDAOMap.put(boName, dynamicDAO);
                        businessObjects.add(dynamicDAO.readMetadataBusinessObject(executionContext));
                        // add relationship tp parent dao
                        ((DynamicDAO) dynamicDAO).addRelationship(relationshipDTO, executionContext);
                    } catch (Exception e) {
                        throw new DSPDMException("Error in reading business object relationships. {}", e, executionContext.getExecutorLocale(), e.getMessage());
                    }
                } else {
                    // add relationship to parent dao
                    ((DynamicDAO) dynamicDAO).addRelationship(relationshipDTO, executionContext);
                }
            }
        }
        logger.info("{} Dynamic DAO relationships loaded successfully", dynamicDAOMap.size());
    }

    private void buildMetadataAttributeUnits(List<BusinessObjectAttributeUnitDTO> list, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Build Metadata Business Object Relationships");
        IDynamicDAO dynamicDAO = null;
        String boName = null;
        for (BusinessObjectAttributeUnitDTO attributeUnitDTO : list) {
            if (StringUtils.isNullOrEmpty(attributeUnitDTO.getBoName())) {
                throw new DSPDMException("Error in reading business object attribute units. BO Name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(attributeUnitDTO.getBoAttrName())) {
                throw new DSPDMException("Error in reading business object attribute units. BO Attribute Name cannot be null or empty", executionContext.getExecutorLocale());
            } else if (StringUtils.isNullOrEmpty(attributeUnitDTO.getBoAttrUnitAttrName())) {
                throw new DSPDMException("Error in reading business object attribute units. BO Attribute Unit Attribute Name cannot be null or empty", executionContext.getExecutorLocale());
            }
            // make sure child bo exists
            boName = attributeUnitDTO.getBoName().trim();
            dynamicDAO = dynamicDAOMap.get(boName);
            if (dynamicDAO == null) {
                try {
                    dynamicDAO = new DynamicDAO(getBusinessObjectDTO(boName, executionContext), executionContext);
                    dynamicDAOMap.put(boName, dynamicDAO);
                    businessObjects.add(dynamicDAO.readMetadataBusinessObject(executionContext));
                    // add relationship tp parent dao
                    ((DynamicDAO) dynamicDAO).addAttributeUnit(attributeUnitDTO, executionContext);
                } catch (Exception e) {
                    throw new DSPDMException("Error in reading business object attribute units. {}", e, executionContext.getExecutorLocale(), e.getMessage());
                }
            } else {
                // add relationship to child dao
                ((DynamicDAO) dynamicDAO).addAttributeUnit(attributeUnitDTO, executionContext);
            }
        }
        logger.info("{} Dynamic DAO Attribute Units loaded successfully", dynamicDAOMap.size());
    }

    private List<BusinessObjectDTO> getAllBusinessObjects(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Business Objects");
        List<BusinessObjectDTO> list = null;
        try {
            String boNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectDTO.class.getDeclaredField(BusinessObjectDTO.properties.boName.name()));
            list = GenericDAO.getInstance().findAll(BusinessObjectDTO.class, Arrays.asList(boNamePhysicalColumnName), executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<BusinessObjectAttributeDTO> getAllBusinessObjectAttributes(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Business Object Attributes");
        List<BusinessObjectAttributeDTO> list = null;
        try {
            String boNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeDTO.class.getDeclaredField(BusinessObjectAttributeDTO.properties.boName.name()));
            String sequenceNumberPhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeDTO.class.getDeclaredField(BusinessObjectAttributeDTO.properties.sequenceNumber.name()));
            // Call database
            list = GenericDAO.getInstance().findAll(BusinessObjectAttributeDTO.class, Arrays.asList(boNamePhysicalColumnName, sequenceNumberPhysicalColumnName), executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<BusinessObjectAttributeConstraintsDTO> getAllBusinessObjectAttributeConstraints(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Business Object Attribute Constraints");
        List<BusinessObjectAttributeConstraintsDTO> list = null;
        try {
            String constraintNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeConstraintsDTO.class.getDeclaredField(BusinessObjectAttributeConstraintsDTO.properties.constraintName.name()));
            String boNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeConstraintsDTO.class.getDeclaredField(BusinessObjectAttributeConstraintsDTO.properties.boName.name()));
            String sequenceNumberPhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeConstraintsDTO.class.getDeclaredField(BusinessObjectAttributeConstraintsDTO.properties.boAttrName.name()));
            // call database
            list = GenericDAO.getInstance().findAll(BusinessObjectAttributeConstraintsDTO.class, Arrays.asList(constraintNamePhysicalColumnName, boNamePhysicalColumnName, sequenceNumberPhysicalColumnName), executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<BusinessObjectAttributeSearchIndexesDTO> getAllBusinessObjectAttributeSearchIndexes(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Business Object Attribute Search Indexes");
        List<BusinessObjectAttributeSearchIndexesDTO> list = null;
        try {
            String indexNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeSearchIndexesDTO.class.getDeclaredField(BusinessObjectAttributeSearchIndexesDTO.properties.indexName.name()));
            String boNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeSearchIndexesDTO.class.getDeclaredField(BusinessObjectAttributeSearchIndexesDTO.properties.boName.name()));
            String sequenceNumberPhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeSearchIndexesDTO.class.getDeclaredField(BusinessObjectAttributeSearchIndexesDTO.properties.boAttrName.name()));
            // call database
            list = GenericDAO.getInstance().findAll(BusinessObjectAttributeSearchIndexesDTO.class, Arrays.asList(indexNamePhysicalColumnName, boNamePhysicalColumnName, sequenceNumberPhysicalColumnName), executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<BusinessObjectRelationshipDTO> getAllBusinessObjectRelationships(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Business Object Relationships");
        List<BusinessObjectRelationshipDTO> list = null;
        try {
            String parentBoName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectRelationshipDTO.class.getDeclaredField(BusinessObjectRelationshipDTO.properties.parentBoName.name()));
            String childOrder = AnnotationProcessor.getDatabaseColumnName(BusinessObjectRelationshipDTO.class.getDeclaredField(BusinessObjectRelationshipDTO.properties.childOrder.name()));
            String relationshipName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectRelationshipDTO.class.getDeclaredField(BusinessObjectRelationshipDTO.properties.name.name()));
            String childBoName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectRelationshipDTO.class.getDeclaredField(BusinessObjectRelationshipDTO.properties.childBoName.name()));
            String childBoAttrName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectRelationshipDTO.class.getDeclaredField(BusinessObjectRelationshipDTO.properties.childBoAttrName.name()));
            // call database
            list = GenericDAO.getInstance().findAll(BusinessObjectRelationshipDTO.class, Arrays.asList(parentBoName, childOrder, relationshipName, childBoName, childBoAttrName), executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<BusinessObjectAttributeUnitDTO> getAllBusinessObjectAttributeUnits(ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Business Object Attribute Units");
        List<BusinessObjectAttributeUnitDTO> list = null;
        try {
            String boName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeUnitDTO.class.getDeclaredField(BusinessObjectAttributeUnitDTO.properties.boName.name()));
            String boAttrName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeUnitDTO.class.getDeclaredField(BusinessObjectAttributeUnitDTO.properties.boAttrName.name()));
            String boAttrUnitAttrName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeUnitDTO.class.getDeclaredField(BusinessObjectAttributeUnitDTO.properties.boAttrUnitAttrName.name()));
            // call database
            list = GenericDAO.getInstance().findAll(BusinessObjectAttributeUnitDTO.class, Arrays.asList(boName, boAttrName, boAttrUnitAttrName), executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }
}
