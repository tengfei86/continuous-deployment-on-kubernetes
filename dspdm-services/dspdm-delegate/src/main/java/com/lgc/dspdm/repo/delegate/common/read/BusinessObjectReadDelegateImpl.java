package com.lgc.dspdm.repo.delegate.common.read;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.Pagination;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.common.util.metadata.MetadataRelationshipUtils;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read.MetadataConstraintsDelegateImpl;
import com.lgc.dspdm.repo.delegate.referencedata.read.ReferenceDataReadDelegateImpl;

import java.util.*;

public class BusinessObjectReadDelegateImpl implements IBusinessObjectReadDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(BusinessObjectReadDelegateImpl.class);
    private static IBusinessObjectReadDelegate singleton = null;

    private BusinessObjectReadDelegateImpl() {

    }

    public static IBusinessObjectReadDelegate getInstance() {
        if (singleton == null) {
            singleton = new BusinessObjectReadDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext).count(businessObjectInfo, executionContext);
    }

    @Override
    public DynamicDTO readOne(DynamicPK dynamicPK, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(dynamicPK.getBoName(), executionContext).readOne(dynamicPK, executionContext);
    }

    @Override
    public List<DynamicDTO> readSimple(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        // in case we need to read the grand children then we need to read the
        // primary key of the current children to read grand children on the pk basis
        addPrimaryKeyColumnsToSelectListIfReadChildBOIncluded(businessObjectInfo, executionContext);
        // first check filters are given and there is not filter with more than 256 values for IN or NOT_IN operator.
        CriteriaFilter filterForLongRangeINSQL = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getFilterForLongRangeINOrNotINSQL();
        if (filterForLongRangeINSQL != null) {
            // clone a new business object info and then work on it
            BusinessObjectInfo clonedBusinessObjectInfo = businessObjectInfo.clone(executionContext);
            // remove the IN filter from the clone
            clonedBusinessObjectInfo.getFilterList().getFilters().remove(filterForLongRangeINSQL);
            list = readLongRangeUsingInClauseAndExistingFilters(clonedBusinessObjectInfo, null,
                    filterForLongRangeINSQL.getColumnName(), filterForLongRangeINSQL.getValues(),
                    filterForLongRangeINSQL.getOperator(), executionContext);
        } else {
            logger.info("Going to read business object data from inside read simple.");
            list = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(
                    businessObjectInfo.getBusinessObjectType(), executionContext).read(businessObjectInfo, executionContext);
        }
        if (CollectionUtils.hasValue(list)) {
            fillNamesForIdsFromCache(businessObjectInfo.getBusinessObjectType(), list, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo,
                                                       BaseJoinClause joinClauseToApplyFilter,
                                                       String boAttrNameForInClause,
                                                       Object[] uniqueValuesForInClause,
                                                       Operator operator,
                                                       ExecutionContext executionContext) {
        // in case we need to read the grand children then we need to read the
        // primary key of the current children to read grand children on the pk basis
        addPrimaryKeyColumnsToSelectListIfReadChildBOIncluded(businessObjectInfo, executionContext);
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                .readLongRangeUsingInClause(businessObjectInfo, joinClauseToApplyFilter, boAttrNameForInClause,
                        uniqueValuesForInClause, operator, executionContext);
    }

    @Override()
    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo,
                                                                         BaseJoinClause joinClauseToApplyFilter,
                                                                         String boAttrNameForInClause,
                                                                         Object[] uniqueValuesForInClause,
                                                                         Operator operator,
                                                                         ExecutionContext executionContext) {
        // in case we need to read the grand children then we need to read the
        // primary key of the current children to read grand children on the pk basis
        addPrimaryKeyColumnsToSelectListIfReadChildBOIncluded(businessObjectInfo, executionContext);
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                .readLongRangeUsingInClauseAndExistingFilters(businessObjectInfo, joinClauseToApplyFilter, boAttrNameForInClause,
                        uniqueValuesForInClause, operator, executionContext);
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo,
                                                                BaseJoinClause joinClauseToApplyFilter,
                                                                List<String> uniqueBoAttrNames,
                                                                Collection<DynamicDTO> uniqueBoAttrNamesValues,
                                                                ExecutionContext executionContext) {
        // in case we need to read the grand children then we need to read the
        // primary key of the current children to read grand children on the pk basis
        addPrimaryKeyColumnsToSelectListIfReadChildBOIncluded(businessObjectInfo, executionContext);
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                .readLongRangeUsingCompositeORClause(businessObjectInfo, joinClauseToApplyFilter, uniqueBoAttrNames,
                        uniqueBoAttrNamesValues, executionContext);
    }

    @Override
    public Map<String, Object> readComplex(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        Map<String, Object> map = new LinkedHashMap<>(5);
        int count = 0;
        List<DynamicDTO> boList = null;
        // If count is required then do count first and if count is greater than zero then read the list otherwise don't
        if (businessObjectInfo.isReadRecordsCount()) {
            logger.info("Going to read count on a business object from inside read complex. Because count is requested.");
            count = count(businessObjectInfo, executionContext);
            if (count > 0) {
                // some data is found
                logger.info("Going to read business object data from inside read complex.");
                boList = readSimple(businessObjectInfo, executionContext);
            } else {
                // no data found
                boList = new ArrayList<>(0);
            }
        } else {
            // count is not required so read the list only and assign the list size to the count
            logger.info("Going to read business object data from inside read complex.");
            boList = readSimple(businessObjectInfo, executionContext);
            count = boList.size();
        }
 
        // 1. ADD PAGED LIST TO DATA
        map.put(businessObjectInfo.getBusinessObjectType(), new PagedList<>(count, boList));
        // 2. READ META DATA

        List<DynamicDTO> metadataList = null;
        if (businessObjectInfo.isReadMetadata()) {
            logger.info("Going to read metadata of a business object from inside read complex.");
            metadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(), executionContext);
            if (CollectionUtils.hasValue(metadataList)) {
                map.put(metadataList.get(0).getType(), new PagedList<>(metadataList.size(), metadataList));
            }
        }

        if (businessObjectInfo.isReadMetadataConstraints()) {
            logger.info("Going to read metadata constraints of a business object from inside read complex.");
            List<DynamicDTO> metadataUniqueConstraintsList = MetadataConstraintsDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(), executionContext);
            if (CollectionUtils.hasValue(metadataUniqueConstraintsList)) {
                map.put(metadataUniqueConstraintsList.get(0).getType(), new PagedList<>(metadataUniqueConstraintsList.size(), metadataUniqueConstraintsList));
            }
        }

        // 3. READ REFERENCE DATA
        if (businessObjectInfo.isReadReferenceData()) {
            Map<String, PagedList<DynamicDTO>> referenceDataMap = readReferenceData(businessObjectInfo, metadataList, executionContext);
            if (CollectionUtils.hasValue(referenceDataMap)) {
                // shift all reference data to the main map
                map.put(DSPDMConstants.REFERENCE_DATA, referenceDataMap);
            }
        }
        // 4. ReadReferenceDataForFilters
        if (businessObjectInfo.isReadReferenceDataForFilters()) {
            Map<String, PagedList<DynamicDTO>> referenceDataForFiltersMap = readReferenceDataForFilters(businessObjectInfo, metadataList, executionContext);
            if (CollectionUtils.hasValue(referenceDataForFiltersMap)) {
                // shift all reference data for filters to the main map
                map.put(DSPDMConstants.REFERENCE_DATA_FOR_FILTERS, referenceDataForFiltersMap);
            }
        }
        // 5. ReadReferenceDataConstraints
        if (businessObjectInfo.isReadReferenceDataConstraints()) {
            Map<String, Object> referenceDataConstraintsMap = readReferenceDataConstraints(businessObjectInfo, metadataList, executionContext);
            if (CollectionUtils.hasValue(referenceDataConstraintsMap)) {
                // shift all reference data constraints to the main map
                map.put(DSPDMConstants.REFERENCE_DATA_CONSTRAINTS, referenceDataConstraintsMap);
            }
        }

        // 6. FILL PARENT BUSINESS OBJECTS TO READ IF ANY ASKED
        if (CollectionUtils.hasValue(businessObjectInfo.getParentBONamesToRead())) {
            // if metadata is not requested then load it now again by calling the service
            if (metadataList == null) {
                metadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(), executionContext);
            }
            List<DynamicDTO> referenceMetadataList = getReferenceMetadataList(metadataList, businessObjectInfo.getParentBONamesToRead(), executionContext);
            fillRelatedDetails(businessObjectInfo, boList, referenceMetadataList, 1, new ArrayList<String>(), executionContext);
        }
        // Read Child Business Objects if required
        if (CollectionUtils.hasValue((businessObjectInfo.getChildBONamesToRead()))) {
            // Handle Child results according to parent foreign key
            fillRelatedChildrenData(businessObjectInfo, boList, DynamicDAOFactory.getInstance(executionContext)
                    .getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .readMetadataRelationships(executionContext), 1, new ArrayList<>(), executionContext);
		}
        
        
        return map;
    }

    /**
     * in case we are required to read the children then we must read the pk of the parent
     *
     * @param businessObjectInfo
     * @param executionContext
     */
    private void addPrimaryKeyColumnsToSelectListIfReadChildBOIncluded(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        // in case we need to read the grand children then we need to read the
        // primary key of the current children to read grand children on the pk basis
        if (CollectionUtils.hasValue(businessObjectInfo.getChildBONamesToRead())) {
            if (businessObjectInfo.hasSimpleColumnToSelect() && (!(businessObjectInfo.hasAggregateColumnToSelect()))) {
                List<DynamicDTO> attrMetadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(), executionContext);
                List<DynamicDTO> pkAttrMetadataList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(
                        attrMetadataList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                if (CollectionUtils.hasValue(pkAttrMetadataList)) {
                    List<String> pkBoAttrNames = CollectionUtils.getStringValuesFromList(pkAttrMetadataList, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    for (String pkBoAttrName : pkBoAttrNames) {
                        if (!(CollectionUtils.containsIgnoreCase(businessObjectInfo.getSelectList().getColumnsToSelect(), pkBoAttrName))) {
                            businessObjectInfo.getSelectList().addColumnsToSelect(pkBoAttrName);
                        }
                    }
                }
            }
        }
    }

    /**
     * Read all the parent records on the basis of reference column BO name
     *
     * @param businessObjectInfo
     * @param metadataList
     * @param executionContext
     * @return
     */
    private Map<String, PagedList<DynamicDTO>> readReferenceData(BusinessObjectInfo businessObjectInfo, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        if (!(businessObjectInfo.isReadMetadata())) {
            throw new DSPDMException("Read metadata is required to read reference data. Please enable metadata reading.", executionContext.getExecutorLocale());
        }
        if (CollectionUtils.isNullOrEmpty(metadataList)) {
            throw new DSPDMException("No metadata found for BO '{}'", executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType());
        }
        return ReferenceDataReadDelegateImpl.getInstance().read(businessObjectInfo.getBusinessObjectType(), metadataList, executionContext);
    }

    /**
     * Read only those reference data values which are used in the current table
     *
     * @param businessObjectInfo
     * @param metadataList
     * @param executionContext
     * @return
     */
    private Map<String, PagedList<DynamicDTO>> readReferenceDataForFilters(BusinessObjectInfo businessObjectInfo, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        return ReferenceDataReadDelegateImpl.getInstance().readFromCurrent(businessObjectInfo.getBusinessObjectType(), metadataList, executionContext);
    }

    /**
     * Read all the parent constraints on the basis of reference column BO name
     *
     * @param businessObjectInfo
     * @param metadataList
     * @param executionContext
     * @return
     */
    private Map<String, Object> readReferenceDataConstraints(BusinessObjectInfo businessObjectInfo, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        return MetadataConstraintsDelegateImpl.getInstance().readParentConstraints(businessObjectInfo.getBusinessObjectType(), metadataList, executionContext);
    }

    /**
     * Read the child records on the basis of relationship columns
     *
     * @param parentBusinessObjectInfo
     * @param parentBusinessObjectsList
     * @param executionContext
     * @author rao.alikhan
     * @since 25-Feb-2020
     */
    private void fillRelatedChildrenData(BusinessObjectInfo parentBusinessObjectInfo, List<DynamicDTO> parentBusinessObjectsList, List<DynamicDTO> allParentChildRelationships,
                                         int currentHierarchyDepth, List<String> messages, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(parentBusinessObjectInfo.getChildBONamesToRead())) {
            int maxAllowedDepth = ConfigProperties.getInstance().max_read_parent_hierarchy_level.getIntegerValue();
            if ((maxAllowedDepth > 0) && (currentHierarchyDepth > maxAllowedDepth)) {
                throw new DSPDMException("Fill related children details for business object '{}' exceeded its max allowed hierarchy level limit '{}' messages '{}'",
                        executionContext.getExecutorLocale(), parentBusinessObjectInfo.getBusinessObjectType(), maxAllowedDepth, CollectionUtils.getCommaSeparated(messages));
            }
            List<DynamicDTO> allChildRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(allParentChildRelationships, DSPDMConstants.BoAttrName.PARENT_BO_NAME, parentBusinessObjectInfo.getBusinessObjectType());
            if (CollectionUtils.hasValue(allChildRelationships)) {
                final Map<String, List<DynamicDTO>> allChildRelationshipsGroupsByNameMap = CollectionUtils.groupDynamicDTOByPropertyValue(allChildRelationships, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);

                for (BusinessObjectInfo childBusinessObjectInfo : parentBusinessObjectInfo.getChildBONamesToRead()) {
                    final List<DynamicDTO> currentChildRelationships = MetadataRelationshipUtils.findBestRelationshipWithCurrentChild(
                            allChildRelationshipsGroupsByNameMap, childBusinessObjectInfo.getBusinessObjectType());
                    if (CollectionUtils.hasValue(currentChildRelationships)) {
                        if (currentChildRelationships.size() == 1) {
                            DynamicDTO relationshipDTO = currentChildRelationships.get(0);
                            String childBOAttrName = (String) relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                            String parentBOAttrName = (String) relationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                            if (childBOAttrName == null) {
                                throw new DSPDMException("Incorrect value found for relationship for ParentBoName '{}' and childBoName '{}'",
                                        executionContext.getExecutorLocale(), parentBusinessObjectInfo.getBusinessObjectType(), childBusinessObjectInfo.getBusinessObjectType());
                            }
                            if (parentBOAttrName == null) {
                                throw new DSPDMException("Incorrect value found for relationship for ParentBoName '{}' and childBoName '{}'",
                                        executionContext.getExecutorLocale(), parentBusinessObjectInfo.getBusinessObjectType(), childBusinessObjectInfo.getBusinessObjectType());
                            }
                            // clone business object info before any change to it so that condition is not added again and again to the same business object info
                            childBusinessObjectInfo = ObjectUtils.deepCopy(childBusinessObjectInfo, executionContext);
                            // Single column relationship
                            fillChildrenDataForSimpleRelationships(parentBOAttrName, childBOAttrName, childBusinessObjectInfo, parentBusinessObjectsList, currentHierarchyDepth, messages, executionContext);
                        } else {
                            // clone business object info before any change to it so that condition is not added again and again to the same business object info
                            childBusinessObjectInfo = ObjectUtils.deepCopy(childBusinessObjectInfo, executionContext);
                            // If there is composite relationship with the child table
                            fillChildrenDataForCompositeRelationships(currentChildRelationships, parentBusinessObjectInfo.getBusinessObjectType(),
                                    parentBusinessObjectsList, childBusinessObjectInfo, currentHierarchyDepth, messages, executionContext);
                        }
                    }
                }
            }
        }
    }

    /**
     * Handle child records using composite foreign key relationships
     *
     * @param relationShipDTOs          Relationship list containing given business object
     * @param parentBoName              Parent Business object name
     * @param parentBusinessObjectsList Parent business object Array
     * @param childBusinessObjectInfo   Child business object info
     * @param currentHierarchyDepth     level of hierarchy depth
     * @param messages                  List of messages
     * @param executionContext
     */
    private void fillChildrenDataForCompositeRelationships(List<DynamicDTO> relationShipDTOs, String parentBoName,
                                                           List<DynamicDTO> parentBusinessObjectsList, BusinessObjectInfo childBusinessObjectInfo,
                                                           int currentHierarchyDepth, List<String> messages, ExecutionContext executionContext) {
        for (DynamicDTO parentDynamicDTO : parentBusinessObjectsList) {
            boolean skipThisParent = false;
            // use new business object info every time because parent filters are being added on ech parent.
            BusinessObjectInfo currentParentChildBusinessObjectInfo = ObjectUtils.deepCopy(childBusinessObjectInfo, executionContext);
            for (DynamicDTO relationshipDTO : relationShipDTOs) {
                String message = StringUtils.formatMessage("Fill related details Business Object '{}', Child Business Object '{}', Child Business Object Attribute '{}'",
                        parentBoName, currentParentChildBusinessObjectInfo.getBusinessObjectType(), relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME));
                logger.info(message);
                messages.add(message);
                String childBOAttrName = (String) relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                String parentBOAttrName = (String) relationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                if (childBOAttrName == null) {
                    throw new DSPDMException("Incorrect value found for relationship for ParentBoName '{}' and childBoName '{}'",
                            executionContext.getExecutorLocale(), parentBoName, currentParentChildBusinessObjectInfo.getBusinessObjectType());
                }
                if (parentBOAttrName == null) {
                    throw new DSPDMException("Incorrect value found for relationship for ParentBoName '{}' and childBoName '{}'",
                            executionContext.getExecutorLocale(), parentBoName, currentParentChildBusinessObjectInfo.getBusinessObjectType());
                }
                // in case select list is provided and the child attribute not selected then add child attribute to the select list
                if (currentParentChildBusinessObjectInfo.hasSimpleColumnToSelect() || currentParentChildBusinessObjectInfo.hasAggregateColumnToSelect()) {
                    if ((currentParentChildBusinessObjectInfo.getSelectList().getColumnsToSelect() == null)
                            || (!(CollectionUtils.containsIgnoreCase(currentParentChildBusinessObjectInfo.getSelectList().getColumnsToSelect(), childBOAttrName)))) {
                        currentParentChildBusinessObjectInfo.getSelectList().addColumnsToSelect(childBOAttrName);
                    }
                }

                Object parentId = parentDynamicDTO.get(parentBOAttrName);
                if (parentId != null) {
                    currentParentChildBusinessObjectInfo.addFilter(childBOAttrName, parentId);
                } else {
                    // parent attribute value is missing therefore skip the children reading for this parent
                    skipThisParent = true;
                    break;
                }
            }
            // now all parent attribute related filters have been added
            if (!skipThisParent) {
                List<DynamicDTO> childrenList = readSimple(currentParentChildBusinessObjectInfo, executionContext);
                if (CollectionUtils.hasValue(childrenList)) {
                    // add the children list to the parent business object with the child business object name
                    parentDynamicDTO.put(currentParentChildBusinessObjectInfo.getBusinessObjectType(), childrenList);
                    // check for grand children if asked by the client
                    fillRelatedGrandChildrenData(currentParentChildBusinessObjectInfo, childrenList, currentHierarchyDepth, messages, executionContext);
                }
            }
        }
    }

    /**
     * Handle simple foreign key relationships
     *
     * @param parentBOAttrName          Parent Business object name
     * @param childBOAttrName           Child Business object name
     * @param childBusinessObjectInfo   Child Business object info
     * @param parentBusinessObjectsList Parent Business object Array
     * @param currentHierarchyDepth     Level of hierarchy
     * @param messages                  List of messages
     * @param executionContext
     */
    private void fillChildrenDataForSimpleRelationships(String parentBOAttrName, String childBOAttrName, BusinessObjectInfo childBusinessObjectInfo, List<DynamicDTO> parentBusinessObjectsList, int currentHierarchyDepth, List<String> messages, ExecutionContext executionContext) {
        // Single column relationship
        // Store required column parentRecordIds for childBO
        // make HashSet for unique records
        HashSet<Object> parentRecordIds = new HashSet<>();
        for (DynamicDTO dynamicDTO : parentBusinessObjectsList) {
            parentRecordIds.add(dynamicDTO.get(parentBOAttrName));
        }
        List<DynamicDTO> childrenList = null;
        // in case select list is provided and the child attribute not selected then add child attribute to the select list
        // this foreign key attribute data will be used to associate the child records to the parents
        if (childBusinessObjectInfo.hasSimpleColumnToSelect() && (!(childBusinessObjectInfo.hasAggregateColumnToSelect()))) {
            if (!(CollectionUtils.containsIgnoreCase(childBusinessObjectInfo.getSelectList().getColumnsToSelect(), childBOAttrName))) {
                childBusinessObjectInfo.getSelectList().addColumnsToSelect(childBOAttrName);
            }
        }
        if (parentRecordIds.size() == 1) {
            // case when there is only one parent record
            // add relationship joining filter
            childBusinessObjectInfo.addFilter(childBOAttrName, parentRecordIds.iterator().next());
            // call database
            logger.info("Going to read children business object data");
            childrenList = readSimple(childBusinessObjectInfo, executionContext);
            if (CollectionUtils.hasValue(childrenList)) {
                // add the children list to the parent business object with the child business object name
                parentBusinessObjectsList.get(0).put(childBusinessObjectInfo.getBusinessObjectType(), childrenList);
                // check for grand children if asked by the client
                fillRelatedGrandChildrenData(childBusinessObjectInfo, childrenList, currentHierarchyDepth, messages, executionContext);
            }
        } else {
            // case when there are more than one parent records for which children are to be read
            // records count for each parent. -1 means no limit. All records to be read by default
            int recordsPerEachParent = -1;
            if (!(childBusinessObjectInfo.isReadAllRecords())) {
                final Pagination childRecordsPaginationObject = childBusinessObjectInfo.getPagination();
                recordsPerEachParent = childRecordsPaginationObject.getRecordsPerPage();
                if (CollectionUtils.hasValue(childRecordsPaginationObject.getPagesToRead())) {
                    recordsPerEachParent = recordsPerEachParent * childRecordsPaginationObject.getPagesToRead().size();
                }
                // Need to fetch all records based on the criteria given and additional filters added in order to fetch child records to attach to respective parent
                childBusinessObjectInfo.setReadAllRecords(true);
            }
            List<DynamicDTO> allParentsChildren = readLongRangeUsingInClauseAndExistingFilters(childBusinessObjectInfo, null, childBOAttrName, parentRecordIds.toArray(), Operator.IN, executionContext);
            if (CollectionUtils.hasValue(allParentsChildren)) {
                Map<Object, List<DynamicDTO>> parentChildrenMap = CollectionUtils.groupDynamicDTOByPropertyValue(allParentsChildren, childBOAttrName, recordsPerEachParent);
                for (DynamicDTO parentDTO : parentBusinessObjectsList) {
                    childrenList = parentChildrenMap.get(parentDTO.get(parentBOAttrName));
                    if (CollectionUtils.hasValue(childrenList)) {
                        // add the children list to the parent business object with the child business object name
                        parentDTO.put(childBusinessObjectInfo.getBusinessObjectType(), childrenList);
                        // check for grand children if asked by the client
                        fillRelatedGrandChildrenData(childBusinessObjectInfo, childrenList, currentHierarchyDepth, messages, executionContext);
                    }
                }
            }
        }
    }

    private void fillRelatedGrandChildrenData(BusinessObjectInfo childBusinessObjectInfo, List<DynamicDTO> childrenList, int currentHierarchyDepth, List<String> messages, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(childBusinessObjectInfo.getChildBONamesToRead())) {
            List<DynamicDTO> childBusinessObjectInfoRelationships = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(childBusinessObjectInfo.getBusinessObjectType(), executionContext).readMetadataRelationships(executionContext);
            if (CollectionUtils.hasValue(childBusinessObjectInfoRelationships)) {
                fillRelatedChildrenData(childBusinessObjectInfo, childrenList, childBusinessObjectInfoRelationships, currentHierarchyDepth + 1, messages, executionContext);
            }
        }
    }

    /**
     * Read the parent record on the basis of reference columns and sets it inside child record with parent BO name
     *
     * @param businessObjectInfo
     * @param businessObjectsList
     * @param childReferenceMetadataList
     * @param executionContext
     * @author Muhamad Imran Ansari
     * @since 10-Oct-2019
     */
    private void fillRelatedDetails(BusinessObjectInfo businessObjectInfo, List<DynamicDTO> businessObjectsList, List<DynamicDTO> childReferenceMetadataList, int currentHierarchyDepth, List<String> messages, ExecutionContext executionContext) {
        int maxAllowedDepth = ConfigProperties.getInstance().max_read_parent_hierarchy_level.getIntegerValue();
        if ((maxAllowedDepth > 0) && (currentHierarchyDepth > maxAllowedDepth)) {
            throw new DSPDMException("Fill related details for business object '{}' exceeded its max allowed hierarchy level limit '{}' messages '{}'", executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType(), maxAllowedDepth, CollectionUtils.getCommaSeparated(messages));
        }
        String parentBOName = null;
        String parentPKBOAttrName = null;
        String childFKBOAttrName = null;
        Object childFKBOAttrValue = null;
        List<DynamicDTO> parentBOList = null;
        for (DynamicDTO businessObjectDTO : businessObjectsList) {
            for (DynamicDTO metadataDTO : childReferenceMetadataList) {
                parentBOName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
                parentPKBOAttrName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE);
                childFKBOAttrName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                childFKBOAttrValue = businessObjectDTO.get(childFKBOAttrName);
                if (childFKBOAttrValue != null) {
                    // if parent and child bo names are same self join
                    if (businessObjectDTO.getType().equalsIgnoreCase(parentBOName)) {
                        if ((businessObjectDTO.getId() != null) && (CollectionUtils.hasValue(businessObjectDTO.getId().getPK()))) {
                            if (childFKBOAttrValue.equals(businessObjectDTO.getId().getPK()[0])) {
                                // same parent and child detected/ wrong data/ reached till root of hierarchy
                                return;
                            }
                        }
                    }
                    BusinessObjectInfo parentBusinessObjectInfo = new BusinessObjectInfo(parentBOName, executionContext);
                    parentBusinessObjectInfo.addFilter(parentPKBOAttrName, childFKBOAttrValue);
                    businessObjectInfo.setReadFirst(true);
                    logger.info("Going to read parent business object data");
                    parentBOList = readSimple(parentBusinessObjectInfo, executionContext);
                    if (CollectionUtils.hasValue(parentBOList)) {
                        String message = StringUtils.formatMessage("Fill related details parentBOName '{}', parentPKBOAttrName '{}', childFKBOAttrValue '{}'", parentBOName, parentPKBOAttrName, childFKBOAttrValue);
                        logger.info(message);
                        messages.add(message);
                        List<DynamicDTO> parentReferenceMetadataList = null;
                        if (businessObjectInfo.getBusinessObjectType().equalsIgnoreCase(parentBusinessObjectInfo.getBusinessObjectType())) {
                            parentReferenceMetadataList = childReferenceMetadataList;
                        } else {
                            List<DynamicDTO> parentMetadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(parentBusinessObjectInfo.getBusinessObjectType(), executionContext);
                            parentReferenceMetadataList = getReferenceMetadataList(parentMetadataList, businessObjectInfo.getParentBONamesToRead(), executionContext);
                        }
                        // call recursively same method
                        fillRelatedDetails(parentBusinessObjectInfo, parentBOList, parentReferenceMetadataList, currentHierarchyDepth + 1, messages, executionContext);
                        businessObjectDTO.put(parentBOName, parentBOList.get(0));
                    }
                }
            }
        }
    }

    private static List<DynamicDTO> getReferenceMetadataList(List<DynamicDTO> metadataList, List<String> filterReferenceBONames, ExecutionContext executionContext) {
        List<DynamicDTO> referenceList = new ArrayList<>();
        String referenceBOName = null;
        String referenceBOAttrValue = null;
        String referenceBOAttrDataType = null;
        List<String> alreadyAddedBONames = new ArrayList<>();
        boolean isMSSQLServerDialect = ConnectionProperties.isMSSQLServerDialect();
        for (DynamicDTO metadataDTO : metadataList) {
            referenceBOName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
            if ((StringUtils.hasValue(referenceBOName)) && (!alreadyAddedBONames.contains(referenceBOName)) && CollectionUtils.containsIgnoreCase(filterReferenceBONames, referenceBOName)) {
                referenceBOAttrValue = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE);
                referenceBOAttrDataType = (String) metadataDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                if (StringUtils.hasValue(referenceBOAttrValue) && StringUtils.hasValue(referenceBOAttrDataType) && (referenceBOAttrDataType.startsWith(DSPDMConstants.DataTypes.PRIMARY_KEY_DATA_TYPE.getAttributeDataType(isMSSQLServerDialect)))) {
                    DynamicDTO parentMetadataDTO = MetadataBOAttrReadDelegateImpl.getInstance().readMap(referenceBOName, executionContext).get(referenceBOAttrValue);
                    // the parent key is the primary key in main table
                    if ((parentMetadataDTO != null) && ((Boolean) parentMetadataDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                        referenceList.add(metadataDTO);
                        alreadyAddedBONames.add(referenceBOName);
                    }
                }
            }
        }
        return referenceList;
    }

    /**
     * this is to avoid query another table to get names against ids
     *
     * @param boName
     * @param list
     * @param executionContext
     */
    private static void fillNamesForIdsFromCache(String boName, List<DynamicDTO> list, ExecutionContext executionContext) {
        if ((DSPDMConstants.BoName.USER_PERFORMED_OPR.equalsIgnoreCase(boName)) ||
                (DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY.equalsIgnoreCase(boName))) {
            if (CollectionUtils.hasValue(list)) {
                Integer operationTypeId = null;
                DSPDMConstants.BusinessObjectOperations operationById = null;
                for (DynamicDTO dto : list) {
                    operationTypeId = (Integer) dto.get(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID);
                    if (operationTypeId != null) {
                        operationById = DSPDMConstants.BusinessObjectOperations.getOperationById(operationTypeId);
                        if (operationById != null) {
                            dto.put(DSPDMConstants.BoAttrName.OPERATION_SHORT_NAME, operationById.getOperationShortName());
                            dto.put(DSPDMConstants.BoAttrName.OPERATION_NAME, operationById.getOperationName());
                        } else {
                            throw new DSPDMException("No business object operation found against id '{}' in business object '{}'",
                                    executionContext.getExecutorLocale(), operationTypeId, DSPDMConstants.BoName.R_BUSINESS_OBJECT_OPR);
                        }
                    }
                }
            }
        }
    }

    @Override
    public String getCurrentUserDBSchemaName(ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext).getCurrentUserDBSchemaName(executionContext);
    }

    @Override
    public String getCurrentUserDBName(ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext).getCurrentUserDBName(executionContext);
    }

    @Override
    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext)
                .getAllEntityNamesListOfConnectedDB(dynamicDTO, exclusionList, executionContext);
    }
}
