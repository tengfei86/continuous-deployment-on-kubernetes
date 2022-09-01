package com.lgc.dspdm.repo.delegate.common;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.repo.delegate.common.read.BusinessObjectReadDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.relationships.read.MetadataRelationshipsDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read.MetadataConstraintsDelegateImpl;
import com.lgc.dspdm.repo.delegate.referencedata.read.ReferenceDataReadDelegateImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestApp {
    
    public static void main(String[] args) {
        
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("WELl", executionContext);
        businessObjectInfo.addChildBONamesToRead(Arrays.asList(new BusinessObjectInfo("WELLBORE", executionContext)));
        Map<String, Object> map = new LinkedHashMap<>(5);
        int count = 0;
        List<DynamicDTO> boList = null;
        // If count is required then do count first and if count is greater than zero then read the list otherwise don't
        if (businessObjectInfo.isReadRecordsCount()) {
            count = BusinessObjectReadDelegateImpl.getInstance().count(businessObjectInfo, executionContext);
            if (count > 0) {
                // some data is found
                boList = BusinessObjectReadDelegateImpl.getInstance().readSimple(businessObjectInfo, executionContext);
            } else {
                // no data found
                boList = new ArrayList<>(0);
            }
        } else {
            // count is not required so read the list only and assign the list size to the count
            boList = BusinessObjectReadDelegateImpl.getInstance().readSimple(businessObjectInfo, executionContext);
            count = boList.size();
        }
        
        // 1. ADD PAGED LIST TO DATA
        map.put(businessObjectInfo.getBusinessObjectType(), new PagedList<>(count, boList));
        // 2. READ META DATA
        
        List<DynamicDTO> metadataList = null;
        if (businessObjectInfo.isReadMetadata()) {
            metadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(),
                    executionContext);
            if (CollectionUtils.hasValue(metadataList)) {
                map.put(metadataList.get(0).getType(), new PagedList<>(metadataList.size(), metadataList));
            }
        }
        
        if (businessObjectInfo.isReadMetadataConstraints()) {
            List<DynamicDTO> metadataUniqueConstraintsList =
                    MetadataConstraintsDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(),
                            executionContext);
            if (CollectionUtils.hasValue(metadataUniqueConstraintsList)) {
                map.put(metadataUniqueConstraintsList.get(0).getType(),
                        new PagedList<>(metadataUniqueConstraintsList.size(), metadataUniqueConstraintsList));
            }
        }
        
        // 3. READ REFERENCE DATA
        if (businessObjectInfo.isReadReferenceData()) {
            Map<String, PagedList<DynamicDTO>> referenceDataMap = readReferenceData(businessObjectInfo, metadataList,
                    executionContext);
            if (CollectionUtils.hasValue(referenceDataMap)) {
                // shift all reference data to the main map
                map.put(DSPDMConstants.REFERENCE_DATA, referenceDataMap);
            }
        }
        
        // 4. FILL PARENT BUSINESS OBJECTS TO READ IF ANY ASKED
//        if (CollectionUtils.hasValue(businessObjectInfo.getParentBONamesToRead())) {
        // if metadata is not requested then load it now again by calling the service
        if (metadataList == null) {
            metadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(),
                    executionContext);
        }
        List<DynamicDTO> some =
                MetadataRelationshipsDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(),
                        ExecutionContext.getTestUserExecutionContext());
        
        
        List<DynamicDTO> referenceMetadataList = getReferenceMetadataList(some,
                businessObjectInfo.getParentBONamesToRead(), executionContext, true);
        
        List<DynamicDTO> referenceMetadataList2 = getReferenceMetadataList(some,
                businessObjectInfo.getChildBONamesToRead().stream().filter(child -> child != null).map(
                        child -> child.getBusinessObjectType()).collect(Collectors.toList())
                , executionContext, false);
        fillRelatedDetailsAlt(businessObjectInfo, boList, referenceMetadataList, 1, new ArrayList<String>(),
                executionContext, true);
        fillRelatedDetailsAlt(businessObjectInfo, boList, referenceMetadataList2, 1, new ArrayList<String>(),
                executionContext, false);
        
        
        System.out.println("done");
        
    }
    
    
    /**
     * Read the parent record on the basis of reference columns and sets it inside child record with parent BO name
     *
     * @param businessObjectInfo
     * @param businessObjectsList
     * @param referenceMetadataList
     * @param executionContext
     * @author Muhamad Imran Ansari
     * @since 10-Oct-2019
     */
    private static void fillRelatedDetailsAlt(BusinessObjectInfo businessObjectInfo,
                                              List<DynamicDTO> businessObjectsList,
                                              List<DynamicDTO> referenceMetadataList, int currentHierarchyDepth,
                                              List<String> messages, ExecutionContext executionContext,
                                              Boolean isParent) {
        int maxAllowedDepth = ConfigProperties.getInstance().max_read_parent_hierarchy_level.getIntegerValue();
        if ((maxAllowedDepth > 0) && (currentHierarchyDepth > maxAllowedDepth)) {
            throw new DSPDMException("Fill related details for business object '{}' exceeded its max allowed " +
                    "hierarchy level limit '{}' messages '{}'", executionContext.getExecutorLocale(),
                    businessObjectInfo.getBusinessObjectType(), maxAllowedDepth,
                    CollectionUtils.getCommaSeparated(messages));
        }
        String targetBOName = null;
        String targetPKBOAttrName = null;
        String originFKBOAttrName = null;
        Object targetFKBOAttrValue = null;
        List<DynamicDTO> targetBOList = null;
        BusinessObjectInfo targetBusinessObjectInfo = null;
        for (DynamicDTO businessObjectDTO : businessObjectsList) {
            for (DynamicDTO metadataDTO : referenceMetadataList) {
                targetBOName = (String) (isParent ? metadataDTO.get("PARENT_BO_NAME") : metadataDTO.get(
                        "CHILD_BO_NAME"));
                targetPKBOAttrName = (String) (isParent ? metadataDTO.get("PARENT_BO_ATTR_NAME") : metadataDTO.get(
                        "CHILD_BO_ATTR_NAME"));
                originFKBOAttrName = (String) ((!isParent) ? metadataDTO.get("PARENT_BO_ATTR_NAME") :
                        metadataDTO.get("CHILD_BO_ATTR_NAME"));
                targetFKBOAttrValue = businessObjectDTO.get(originFKBOAttrName);
                if (targetFKBOAttrValue != null) {
                    // if parent and child bo names are same self join
                    if (businessObjectDTO.getType().equalsIgnoreCase(targetBOName)) {
                        if ((businessObjectDTO.getId() != null) && (CollectionUtils.hasValue(businessObjectDTO.getId().getPK()))) {
                            if (targetFKBOAttrValue.equals(businessObjectDTO.getId().getPK()[0])) {
                                // same parent and child detected/ wrong data/ reached till root of hierarchy
                                return;
                            }
                        }
                    }
                    if (isParent) {
                        targetBusinessObjectInfo = new BusinessObjectInfo(targetBOName,
                                executionContext);
                        targetBusinessObjectInfo.addFilter(targetPKBOAttrName, targetFKBOAttrValue);
                        businessObjectInfo.setReadFirst(true);
                        targetBOList = BusinessObjectReadDelegateImpl.getInstance().readSimple(targetBusinessObjectInfo,
                                executionContext);
                        if (CollectionUtils.hasValue(targetBOList)) {
                            String message = StringUtils.formatMessage("Fill related details targetBOName '{}', " +
                                            "targetPKBOAttrName '{}', targetFKBOAttrValue '{}'", targetBOName,
                                    targetPKBOAttrName,
                                    targetFKBOAttrValue);
                            messages.add(message);
                            List<DynamicDTO> targetReferenceMetadataList = null;
                            if (businessObjectInfo.getBusinessObjectType().equalsIgnoreCase(targetBusinessObjectInfo.getBusinessObjectType())) {
                                targetReferenceMetadataList = referenceMetadataList;
                            } else {
                                List<DynamicDTO> targetMetadataList =
                                        MetadataRelationshipsDelegateImpl.getInstance().read(targetBusinessObjectInfo.getBusinessObjectType(), executionContext);
                                targetReferenceMetadataList = getReferenceMetadataList(targetMetadataList,
                                        businessObjectInfo.getParentBONamesToRead(), executionContext, true);
                            }
                            // call recursively same method
                            fillRelatedDetailsAlt(targetBusinessObjectInfo, targetBOList, targetReferenceMetadataList,
                                    currentHierarchyDepth + 1, messages, executionContext, isParent);
                        } else {
                            String finalTargetBOName = targetBOName;
                            targetBusinessObjectInfo = businessObjectInfo
                                    .getChildBONamesToRead().stream()
                                    .filter(child -> child.getBusinessObjectType().equalsIgnoreCase(finalTargetBOName)).findFirst().get();
                            // will do recursively through readComplex method
                            targetBOList =
                                    (List<DynamicDTO>) BusinessObjectReadDelegateImpl.getInstance().readComplex(targetBusinessObjectInfo,
                                            executionContext).get(finalTargetBOName);
                        }
                        businessObjectDTO.put(targetBOName, isParent ? targetBOList.get(0) : targetBOList);
                    }
                }
            }
        }
    }
    
    private static Map<String, PagedList<DynamicDTO>> readReferenceData(BusinessObjectInfo businessObjectInfo,
                                                         List<DynamicDTO> metadataList,
                                                         ExecutionContext executionContext) {
        if (!(businessObjectInfo.isReadMetadata())) {
            throw new DSPDMException("Read metadata is required to read reference data. Please enable metadata " +
                    "reading.", executionContext.getExecutorLocale());
        }
        if (CollectionUtils.isNullOrEmpty(metadataList)) {
            throw new DSPDMException("No metadata found for BO '{}'", executionContext.getExecutorLocale(),
                    businessObjectInfo.getBusinessObjectType());
        }
        return ReferenceDataReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(),
                metadataList, executionContext);
    }
    
    private static List<DynamicDTO> getReferenceMetadataList(List<DynamicDTO> metadataList,
                                                             List<String> filterRefenceBONames,
                                                             ExecutionContext executionContext, Boolean isParent) {
        List<DynamicDTO> referenceList = new ArrayList<>();
        String referenceBOName = null;
        String referenceBOAttrValue = null;
        List<String> alreadyAddedBONames = new ArrayList<>();
        for (DynamicDTO metadataDTO : metadataList) {
            // check is primary key
            if ((boolean) metadataDTO.get("IS_PRIMARY_KEY_RELATIONSHIP")) {
                // check child ref
                referenceBOName = isParent ? (String) metadataDTO.get("PARENT_BO_NAME") : (String) metadataDTO.get(
                        "CHILD_BO_NAME");
                if ((StringUtils.hasValue(referenceBOName)) && (!alreadyAddedBONames.contains(referenceBOName)) && CollectionUtils.containsIgnoreCase(filterRefenceBONames, referenceBOName)) {
                    referenceList.add(metadataDTO);
                    alreadyAddedBONames.add(referenceBOName);
                }
            }
        }
        return referenceList;
    }
    
}
