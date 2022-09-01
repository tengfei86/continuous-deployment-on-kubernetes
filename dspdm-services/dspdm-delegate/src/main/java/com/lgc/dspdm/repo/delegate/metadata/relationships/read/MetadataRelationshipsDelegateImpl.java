package com.lgc.dspdm.repo.delegate.metadata.relationships.read;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetadataRelationshipsDelegateImpl implements IMetadataRelationshipsDelegate {
    private static IMetadataRelationshipsDelegate singleton = null;

    private MetadataRelationshipsDelegateImpl() {

    }

    public static IMetadataRelationshipsDelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataRelationshipsDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext) {
//        BusinessObjectInfo metadataRelationshipBusinessObjectInfo = new BusinessObjectInfo(MetadataUtils.getBONameForDTO(BusinessObjectRelationshipDTO.class, executionContext), executionContext);
//        // read read all records of metadata table
//        metadataRelationshipBusinessObjectInfo.setReadAllRecords(true);
//        return BusinessObjectReadDelegate.getInstance().readSimple(metadataRelationshipBusinessObjectInfo, executionContext);
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readMetadataRelationships(executionContext);
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataChildRelationshipsMap(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext)
                .getDynamicDAO(boName, executionContext).readMetadataChildRelationships(executionContext);
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataParentRelationshipsMap(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext)
                .getDynamicDAO(boName, executionContext).readMetadataParentRelationships(executionContext);
    }

    @Override
    public DynamicDTO readBoHierarchy(DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String boDisplayName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME);
        // fetching R REPORTING ENTITY KIND for adding Product Volume Summary as a child
        Map<String, BusinessObjectDTO> businessObjectsMap = DynamicDAOFactory.getInstance(executionContext).getBusinessObjectsMap(executionContext);
        List<DynamicDTO> reportingEntityKindDTOsList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(
                DSPDMConstants.BoName.R_REPORTING_ENTITY_KIND, executionContext).read(
                new BusinessObjectInfo(DSPDMConstants.BoName.R_REPORTING_ENTITY_KIND, executionContext).setReadAllRecords(true)
                , executionContext);
        // read child relationships
        Map<String, List<DynamicDTO>> childRelationships = readMetadataChildRelationshipsMap(boName, executionContext);
        List<DynamicDTO> childrenBoHierarchyOfCurrentBo = findBoHierarchyOfChildBo(boName, childRelationships, reportingEntityKindDTOsList,
                businessObjectsMap, executionContext);
        // verifying that the bo belongs to R REPORTING ENTITY KIND or not. If yes, adding Product Volume Summary as a child
        DynamicDTO productVolumeSummaryDTO = null;
        if (reportingEntityKindDTOsList.stream().anyMatch(dynamicDTOForProductVolumeSummary ->
                boName.equalsIgnoreCase((String) dynamicDTOForProductVolumeSummary.get(DSPDMConstants.BoAttrName.REPORTING_ENTITY_KIND)))) {
            // adding product volume summary

            productVolumeSummaryDTO = new DynamicDTO(DSPDMConstants.BoName.PRODUCT_VOLUME_SUMMARY, null, executionContext);
            productVolumeSummaryDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoName.PRODUCT_VOLUME_SUMMARY);
            productVolumeSummaryDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME,
                    businessObjectsMap.get(DSPDMConstants.BoName.PRODUCT_VOLUME_SUMMARY).getBoDisplayName());
            productVolumeSummaryDTO.putWithOrder(DSPDMConstants.EXPANDED, Boolean.FALSE);
            childrenBoHierarchyOfCurrentBo.add(productVolumeSummaryDTO);
        }
        DynamicDTO boHierarchy = new DynamicDTO(boName, null, executionContext);
        boHierarchy.putWithOrder(DSPDMConstants.BoAttrName.BO_NAME, boName);
        boHierarchy.putWithOrder(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, boDisplayName);
        boHierarchy.putWithOrder(DSPDMConstants.EXPANDED, Boolean.TRUE);
        boHierarchy.putWithOrder(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, childrenBoHierarchyOfCurrentBo);
        return boHierarchy;
    }

    /**
     * Returns the list of children for the given parent bo name.
     *
     * @param parentBoName
     * @param childRelationships
     * @param reportingEntityKindDTOsList
     * @param businessObjectsMap
     * @param executionContext
     * @return
     */
    private List<DynamicDTO> findBoHierarchyOfChildBo(String parentBoName,
                                                      Map<String, List<DynamicDTO>> childRelationships,
                                                      List<DynamicDTO> reportingEntityKindDTOsList,
                                                      Map<String, BusinessObjectDTO> businessObjectsMap,
                                                      ExecutionContext executionContext) {
        List<DynamicDTO> dynamicDTOList = new ArrayList<>();
        for (Map.Entry<String, List<DynamicDTO>> entry : childRelationships.entrySet()) {
            // do not process relationships having child_order as 0
            if(CollectionUtils.hasValue(entry.getValue())){
                // only checking child_order for DTO at 0th level because for an entry, child_order will be same for all of its DTOs
                if(((Integer)entry.getValue().get(0).get(DSPDMConstants.BoAttrName.CHILD_ORDER)) == 0){
                    continue;
                }
            }
            // do not process recursive relationships
            if (!parentBoName.equalsIgnoreCase(entry.getKey())) {
                if (!(CollectionUtils.hasValue(CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(
                        entry.getValue(), DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.TRUE)))) {
                    // skipping relationship because the List<DynamicDTO> of the child relationship does not possess any primary key relationship in it
                    continue;
                }
                String childBoName = entry.getKey();
                String childBoDisplayName = businessObjectsMap.get(childBoName).getBoDisplayName();

                // calling recursively for grand children
                List<DynamicDTO> childrenBoHierarchyOfChildBo = findBoHierarchyOfChildBo(childBoName, readMetadataChildRelationshipsMap(childBoName, executionContext),
                        reportingEntityKindDTOsList, businessObjectsMap, executionContext);

                // verifying that current child bo name belongs to R REPORTING ENTITY KIND or not. If yes, adding Product Volume Summary as a valid grand child
                if (reportingEntityKindDTOsList.stream().anyMatch(dynamicDTOForProductVolumeSummary ->
                        childBoName.equalsIgnoreCase((String) dynamicDTOForProductVolumeSummary.get(DSPDMConstants.BoAttrName.REPORTING_ENTITY_KIND)))) {
                    // adding product volume summary
                    DynamicDTO productVolumeSummaryDTO = new DynamicDTO(DSPDMConstants.BoName.PRODUCT_VOLUME_SUMMARY, null, executionContext);
                    productVolumeSummaryDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoName.PRODUCT_VOLUME_SUMMARY);
                    productVolumeSummaryDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME,
                            businessObjectsMap.get(DSPDMConstants.BoName.PRODUCT_VOLUME_SUMMARY).getBoDisplayName());
                    productVolumeSummaryDTO.putWithOrder(DSPDMConstants.EXPANDED, Boolean.FALSE);
                    childrenBoHierarchyOfChildBo.add(productVolumeSummaryDTO);
                }
                DynamicDTO childDynamicDTO = new DynamicDTO(childBoName, null, executionContext);
                childDynamicDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_NAME, childBoName);
                childDynamicDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, childBoDisplayName);
                childDynamicDTO.putWithOrder(DSPDMConstants.EXPANDED, Boolean.TRUE);
                childDynamicDTO.putWithOrder(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, childrenBoHierarchyOfChildBo);
                dynamicDTOList.add(childDynamicDTO);
            }
        }
        return dynamicDTOList;
    }
}
