package com.lgc.dspdm.repo.delegate.referencedata.read;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.JoinType;
import com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition;
import com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.common.read.BusinessObjectReadDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegateImpl;

import java.util.*;

public class ReferenceDataReadDelegateImpl implements IReferenceDataReadDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(ReferenceDataReadDelegateImpl.class);
    private static IReferenceDataReadDelegate singleton = null;

    private ReferenceDataReadDelegateImpl() {

    }

    public static IReferenceDataReadDelegate getInstance() {
        if (singleton == null) {
            singleton = new ReferenceDataReadDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public Map<String, PagedList<DynamicDTO>> read(String boName, ExecutionContext executionContext) {
        return read(boName, null, executionContext);
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> read(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        Map<String, PagedList<DynamicDTO>> finalMap = new LinkedHashMap<>(4);
        Map<String, BusinessObjectInfo> boNameMapForReading = buildReadReferenceDataBusinessObjectInfo(boName, metadataList, executionContext);
        // check if there is some reference data found to be read
        if (boNameMapForReading.size() > 0) {
            List<DynamicDTO> referenceDataList = null;
            String isActiveBOAttrName = (String) MetadataUtils.getBOAttributeNameForFieldIsActive(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.isActive.name(), executionContext);
            for (BusinessObjectInfo referenceBusinessObjectInfo : boNameMapForReading.values()) {
                logger.info("Going to read reference data.");
                referenceDataList = BusinessObjectReadDelegateImpl.getInstance().readSimple(referenceBusinessObjectInfo, executionContext);
                if (CollectionUtils.hasValue(referenceDataList)) {
                    for (DynamicDTO dynamicDTO : referenceDataList) {
                        // if is_active was not selected then add it now as true
                        if (!(dynamicDTO.containsKey(isActiveBOAttrName))) {
                            dynamicDTO.put(isActiveBOAttrName, true);
                        }
                    }
                    finalMap.put(referenceBusinessObjectInfo.getBusinessObjectType(), new PagedList<>(referenceDataList.size(), referenceDataList));
                }
            }
        }
        return finalMap;
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> readFromCurrent(String boName, ExecutionContext executionContext) {
        return readFromCurrent(boName, null, executionContext);
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> readFromCurrent(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        Map<String, PagedList<DynamicDTO>> finalMap = new LinkedHashMap<>(4);
        Map<String, BusinessObjectInfo> boNameMapForReading = buildReadReferenceDataBusinessObjectInfo(boName, metadataList, executionContext);
        // check if there is some reference data found to be read
        if (boNameMapForReading.size() > 0) {
            IDynamicDAO businessObjectDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext);
            // read all parent relationships where current bo is a child
            Map<String, List<DynamicDTO>> parentRelationshipsMap = businessObjectDAO.readMetadataParentRelationships(executionContext);
            if (!CollectionUtils.hasValue(parentRelationshipsMap)) {
                return finalMap;
            }
            List<DynamicDTO> referenceDataList = null;
            String isActiveBOAttrName = (String) MetadataUtils.getBOAttributeNameForFieldIsActive(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.isActive.name(), executionContext);
            for (BusinessObjectInfo referenceBusinessObjectInfo : boNameMapForReading.values()) {
                referenceBusinessObjectInfo.setAlias("a");//reference bo is parent
                List<DynamicDTO> parentRelationships = parentRelationshipsMap.get(referenceBusinessObjectInfo.getBusinessObjectType());
                if (!CollectionUtils.hasValue(parentRelationships)) {
                    continue;
                }
                List<DynamicDTO> pkRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(parentRelationships, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, true);
                if (!CollectionUtils.hasValue(pkRelationships)) {
                    continue;
                }
                for (int i = 0; i < pkRelationships.size(); i++) {
                    DynamicDTO pkDTO = pkRelationships.get(i);
                    BusinessObjectInfo toReadBusinessObjectInfo = (i == pkRelationships.size() - 1) ? referenceBusinessObjectInfo : referenceBusinessObjectInfo.clone(executionContext);
                    //Get childBoAttrName(current bo) and parentBoAttrName(reference bo)
                    String childBoAttrName = (String) pkDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                    String parentBoAttrName = (String) pkDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                    //INNER Join current bo (current bo is a child)
                    SimpleJoinClause simpleJoinClause = new SimpleJoinClause(boName, "b", JoinType.INNER);
                    simpleJoinClause.setJoiningConditions(Arrays.asList(new JoiningCondition(
                            new JoiningCondition.JoiningConditionOperand("a", parentBoAttrName)
                            , Operator.EQUALS
                            , new JoiningCondition.JoiningConditionOperand("b", childBoAttrName))));
                    simpleJoinClause.addCriteriaFilter(new CriteriaFilter(childBoAttrName, Operator.NOT_EQUALS, new Object[]{null}));
                    toReadBusinessObjectInfo.addSimpleJoin(simpleJoinClause);
                    //read
                    logger.info("Going to read reference data for filters");
                    referenceDataList = BusinessObjectReadDelegateImpl.getInstance().readSimple(toReadBusinessObjectInfo, executionContext);
                    if (CollectionUtils.hasValue(referenceDataList)) {
                        for (DynamicDTO dynamicDTO : referenceDataList) {
                            // if is_active was not selected then add it now as true
                            if (!(dynamicDTO.containsKey(isActiveBOAttrName))) {
                                dynamicDTO.put(isActiveBOAttrName, true);
                            }
                        }
                        finalMap.put(childBoAttrName, new PagedList<>(referenceDataList.size(), referenceDataList));
                    }
                }
            }
        }
        return finalMap;
    }

    private Map<String, BusinessObjectInfo> buildReadReferenceDataBusinessObjectInfo(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        // read metadata first if not already provided
        if (CollectionUtils.isNullOrEmpty(metadataList)) {
            metadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(boName, executionContext);
        }
        String referenceBOName = null;
        String referenceBOAttrValue = null;
        String referenceBOAttrLabel = null;
        String isActiveBOAttrName = (String) MetadataUtils.getBOAttributeNameForFieldIsActive(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.isActive.name(), executionContext);
        Map<String, BusinessObjectInfo> boNameMapForReading = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DynamicDTO metadataDTO : metadataList) {
            referenceBOName = (String) metadataDTO.get(MetadataUtils.getBOAttributeNameForFieldReferenceTable(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.referenceBOName.name(), executionContext));
            referenceBOAttrValue = (String) metadataDTO.get(MetadataUtils.getBoAttributeNameForFieldReferenceBOAttrValue(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.referenceBOAttrNameForValue.name(), executionContext));
            referenceBOAttrLabel = (String) metadataDTO.get(MetadataUtils.getBoAttributeNameForFieldReferenceBOAttrLabel(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.referenceBOAttrNameForLabel.name(), executionContext));
            if ((StringUtils.hasValue(referenceBOName)) && (StringUtils.hasValue(referenceBOAttrValue)) && (StringUtils.hasValue(referenceBOAttrLabel))) {
                BusinessObjectInfo referenceBusinessObjectInfo = boNameMapForReading.get(referenceBOName);
                if (referenceBusinessObjectInfo != null) {
                    if (!(referenceBusinessObjectInfo.getSelectList().getColumnsToSelect().contains(referenceBOAttrValue))) {
                        referenceBusinessObjectInfo.addColumnsToSelect(referenceBOAttrValue);
                    }
                    if (!(referenceBusinessObjectInfo.getSelectList().getColumnsToSelect().contains(referenceBOAttrLabel))) {
                        referenceBusinessObjectInfo.addColumnsToSelect(referenceBOAttrLabel);
                    }
                } else {
                    referenceBusinessObjectInfo = new BusinessObjectInfo(referenceBOName, executionContext);
                    // read limited columns
                    referenceBusinessObjectInfo.addColumnsToSelect(referenceBOAttrValue, referenceBOAttrLabel);
                    // read distinct records
                    referenceBusinessObjectInfo.setReadWithDistinct(true);
                    // if is_active is a attribute in reference business object then add it otherwise leave it
                    if (MetadataBOAttrReadDelegateImpl.getInstance().readBOAttrNames(referenceBOName, executionContext).contains(isActiveBOAttrName)) {
                        if (!(referenceBusinessObjectInfo.getSelectList().getColumnsToSelect().contains(isActiveBOAttrName))) {
                            referenceBusinessObjectInfo.addColumnsToSelect(isActiveBOAttrName);
                        }
                        referenceBusinessObjectInfo.addFilter(isActiveBOAttrName, true);
                        referenceBusinessObjectInfo.addOrderByDesc(isActiveBOAttrName);
                    }
                    // add all columns from unique constraints
                    List<DynamicDTO> uniqueConstraints = DynamicDAOFactory.getInstance(executionContext)
                            .getDynamicDAO(referenceBOName, executionContext).readActiveMetadataConstraints(executionContext);
                    if (CollectionUtils.hasValue(uniqueConstraints)) {
                        List<String> boAttrNamesFromUniqueConstraints = CollectionUtils.getStringValuesFromList(uniqueConstraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                        if (CollectionUtils.hasValue(boAttrNamesFromUniqueConstraints)) {
                            for (String boAttrNamesFromUniqueConstraint : boAttrNamesFromUniqueConstraints) {
                                if (!(CollectionUtils.containsIgnoreCase(referenceBusinessObjectInfo.getSelectList().getColumnsToSelect(), boAttrNamesFromUniqueConstraint))) {
                                    referenceBusinessObjectInfo.addColumnsToSelect(boAttrNamesFromUniqueConstraint);
                                }
                            }
                        }
                    }
                    // where is_active = true, bring all active and in active o display in combo box to support the view and display of old data
                    // referenceBusinessObjectInfo.addFilter(MetadataUtils.getBOAttributeNameForFieldIsActive(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.isActive.name()), Boolean.TRUE);
                    referenceBusinessObjectInfo.addOrderByAsc(referenceBOAttrLabel);
                    // read first max records of metadata table
                    referenceBusinessObjectInfo.setRecordsPerPage(ConfigProperties.getInstance().max_records_for_reference_data.getIntegerValue(), executionContext);
                    // put in a map to collect other reference columns data for same bo name with different column names then select all reference columns together
                    boNameMapForReading.put(referenceBOName, referenceBusinessObjectInfo);
                }
            }
        }
        return boNameMapForReading;
    }
}
