package com.lgc.dspdm.repo.delegate.metadata.bosearch;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicTable;
import com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegateImpl;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author rao.alikhan
 */
public class BOSearchDelegateImpl implements IBOSearchDelegate {

    private static IBOSearchDelegate singleton = null;
    private static DSPDMLogger logger = new DSPDMLogger(BOSearchDelegateImpl.class);

    private BOSearchDelegateImpl() {

    }

    public static IBOSearchDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new BOSearchDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public SaveResultDTO createSearchIndexForBusinessObjects(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectList)) {
            List<DynamicDTO> mainBOSearchSaveList = new ArrayList<>(businessObjectList.size());
            List<DynamicDTO> businessObjectInsertList;
            List<DynamicDTO> businessObjectListForUpdate = null;
            if (!(executionContext.isIndexAll())) {
                Map<DSPDMConstants.DBOperation, List<DynamicDTO>> separateSaveAndUpdateListsMap = BOSearchHelper.getInstance(executionContext).getSeparateSaveAndUpdateListsMap(businessObjectList);
                businessObjectInsertList = separateSaveAndUpdateListsMap.get(DSPDMConstants.DBOperation.CREATE);
                businessObjectListForUpdate = separateSaveAndUpdateListsMap.get(DSPDMConstants.DBOperation.UPDATE);
            } else {
                businessObjectInsertList = businessObjectList;
            }
            if (businessObjectInsertList != null) {
                List<DynamicDTO> boSearchDataForInsert = BOSearchHelper.getInstance(executionContext).getBoSearchDynamicDTOListForInsertOrUpdateOrDelete(
                        businessObjectInsertList, DSPDMConstants.DBOperation.CREATE, executionContext);
                if (CollectionUtils.hasValue(boSearchDataForInsert)) {
                    mainBOSearchSaveList.addAll(boSearchDataForInsert);
                } else {
                    logger.info("Data is inserted in " + boName + " but there is no entry found to insert in " + DSPDMConstants.BoName.BO_SEARCH);
                }
            }
            if (businessObjectListForUpdate != null) {
                List<DynamicDTO> boSearchDataForUpdate = BOSearchHelper.getInstance(executionContext).getBoSearchDynamicDTOListForInsertOrUpdateOrDelete(
                        businessObjectListForUpdate, DSPDMConstants.DBOperation.UPDATE, executionContext);
                if (CollectionUtils.hasValue(boSearchDataForUpdate)) {
                    mainBOSearchSaveList.addAll(boSearchDataForUpdate);
                } else {
                    logger.info("Data is updated in " + boName + " but there is no entry found to update in " + DSPDMConstants.BoName.BO_SEARCH);
                }
            }
            if (executionContext.isIndexAll()) {
                rootSaveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext)
                        .getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).saveOrUpdate(mainBOSearchSaveList, executionContext));
            } else {
                DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).saveOrUpdate(mainBOSearchSaveList, executionContext);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO createSearchIndexForAllBusinessObjects(ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // 1. Call deleteAll service to clear BO_SEARCH for all the business objects
        rootSaveResultDTO.addResult(this.deleteAllSearchIndexes(executionContext));
        // 2. Read all business object other than metadata and BO_SEARCH.
        List<DynamicDTO> businessObjectsMetadata = DynamicDAOFactory.getInstance(executionContext).getBusinessObjects(executionContext);
        if (CollectionUtils.hasValue(businessObjectsMetadata)) {
            IDynamicDAO currentBoDAO = null;
            List<DynamicDTO> businessObjectList = null;
            BusinessObjectInfo businessObjectInfo = null;
            int maxAllowedReadSize = ConfigProperties.getInstance().max_records_to_read.getIntegerValue();
            for (DynamicDTO businessObjectMetadataDTO : businessObjectsMetadata) {
                String currentBoName = (String) businessObjectMetadataDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                if ((!(CollectionUtils.containsIgnoreCase(DSPDMConstants.NO_CHANGE_TRACK_BO_NAMES, currentBoName)))
                        && (Boolean.FALSE.equals(businessObjectMetadataDTO.get(DSPDMConstants.BoAttrName.IS_METADATA_TABLE)))
                        && (Boolean.TRUE.equals(businessObjectMetadataDTO.get(DSPDMConstants.BoAttrName.IS_ACTIVE)))) {
                    // get dao for current bo name
                    currentBoDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(currentBoName, executionContext);
                    logger.info("Going to index records for business object '{}'", currentBoName);
                    businessObjectInfo = new BusinessObjectInfo(currentBoName, executionContext);
                    // init page number with 1 as first page number
                    int pageNumber = PagedList.FIRST_PAGE_NUMBER;
                    //  start a do while loop
                    do {
                        businessObjectInfo.setRecordsAndPages(executionContext, maxAllowedReadSize, pageNumber);
                        // 3. Read business object from db
                        businessObjectList = currentBoDAO.read(businessObjectInfo, executionContext);
                        if (CollectionUtils.hasValue(businessObjectList)) {
                            // 4. For all the business object read from db create search indexes.
                            rootSaveResultDTO.addResult(this.createSearchIndexForBusinessObjects(businessObjectList, currentBoName, executionContext));
                        }
                        // check if the list size was equal to or greater tah the max allowed size
                        // then it means there are still more records in the db for same business object type
                        // no need to read count for this purpose specially.
                        // increment page number
                        pageNumber++;
                    } while (businessObjectList.size() >= maxAllowedReadSize);
                }
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO createSearchIndexForBoName(String boName, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        BusinessObjectDTO businessObjectDTO = DynamicDAOFactory.getInstance(executionContext).getBusinessObjectDTO(boName, executionContext);
        if ((businessObjectDTO != null)) {
            // fix case
            boName = businessObjectDTO.getBoName();
            if (!(CollectionUtils.containsIgnoreCase(DSPDMConstants.NO_CHANGE_TRACK_BO_NAMES, boName))) {
                // 1. Call deleteAll service to clear BO_SEARCH for the current  business objects
                rootSaveResultDTO.addResult(this.deleteAllSearchIndexesForBoName(boName, executionContext));
                // 2. Read business object other than metadata and BO_SEARCH.

                int maxAllowedReadSize = ConfigProperties.getInstance().max_records_to_read.getIntegerValue();
                if ((Boolean.FALSE.equals(businessObjectDTO.getMetadataTable()))
                        && (Boolean.TRUE.equals(businessObjectDTO.getActive()))) {
                    String currentBoName = businessObjectDTO.getBoName();
                    // 3. read records from data model db
                    IDynamicDAO currentBoDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(currentBoName, executionContext);
                    logger.info("Going to index records for business object '{}'", currentBoName);
                    BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(currentBoName, executionContext);
                    // init page number with 1 as first page number
                    int pageNumber = PagedList.FIRST_PAGE_NUMBER;
                    List<DynamicDTO> businessObjectList = null;
                    //  start a do while loop
                    do {
                        businessObjectInfo.setRecordsAndPages(executionContext, maxAllowedReadSize, pageNumber);
                        // 3. Read business object from db
                        businessObjectList = currentBoDAO.read(businessObjectInfo, executionContext);
                        if (CollectionUtils.hasValue(businessObjectList)) {
                            // 4. For all the business object read from db create search indexes.
                            rootSaveResultDTO.addResult(this.createSearchIndexForBusinessObjects(businessObjectList, currentBoName, executionContext));
                        }
                        // check if the list size was equal to or greater tah the max allowed size
                        // then it means there are still more records in the db for same business object type
                        // no need to read count for this purpose specially.
                        // increment page number
                        pageNumber++;
                    } while (businessObjectList.size() >= maxAllowedReadSize);
                }
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO delete(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectList)) {
            List<DynamicDTO> boSearchListToDelete = BOSearchHelper.getInstance(executionContext).getBoSearchDynamicDTOListForInsertOrUpdateOrDelete(businessObjectList, DSPDMConstants.DBOperation.DELETE, executionContext);
            if (CollectionUtils.hasValue(boSearchListToDelete)) {
                DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).delete(boSearchListToDelete, executionContext);
            } else {
                logger.info("Data is deleted in " + boName + " but unable find any data to delete in " + DSPDMConstants.BoName.BO_SEARCH);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO deleteAllSearchIndexes(ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = DynamicDAOFactory.getInstance(executionContext)
                .getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).deleteAll(executionContext);
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO deleteAllSearchIndexesForBoName(String boName, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        IDynamicDAO searchDAO = DynamicDAOFactory.getInstance(executionContext)
                .getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext);
        int maxAllowedReadSize = ConfigProperties.getInstance().max_records_to_read.getIntegerValue();

        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BO_SEARCH, executionContext);
        businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoAttrName.BO_SEARCH_ID);
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName);
        //  start a do while loop
        List<DynamicDTO> searchIndexes = null;
        do {
            businessObjectInfo.setRecordsAndPages(executionContext, maxAllowedReadSize, PagedList.FIRST_PAGE_NUMBER);
            // 3. Read business object from db
            searchIndexes = searchDAO.read(businessObjectInfo, executionContext);
            if (CollectionUtils.hasValue(searchIndexes)) {
                // 4. delete the search indexes.
                rootSaveResultDTO.addResult(searchDAO.delete(searchIndexes, executionContext));
            }
            // increment page number
            // Data that has been deleted cannot be found , no need :pageNumber++;
        } while (searchIndexes.size() >= maxAllowedReadSize);
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectList)) {
            List<DynamicDTO> boSearchListToDelete = BOSearchHelper.getInstance(executionContext).getBoSearchDynamicDTOListForInsertOrUpdateOrDelete(businessObjectList, DSPDMConstants.DBOperation.DELETE, executionContext);
            if (CollectionUtils.hasValue(boSearchListToDelete)) {
                DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).softDelete(boSearchListToDelete, executionContext);
            } else {
                logger.info("Data is deleted in " + boName + " but unable find any data to delete in " + DSPDMConstants.BoName.BO_SEARCH);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public PagedList<DynamicDTO> search(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        PagedList<DynamicDTO> searchResults = null;
        BusinessObjectInfo boSearchBusinessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BO_SEARCH, executionContext);
        boSearchBusinessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES, DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES, DSPDMConstants.BoAttrName.BO_NAME);
        // find search keywords from original business object info
        Object[] searchKeywordsExact = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getValuesWithOperator(Operator.JSONB_FIND_EXACT);
        Object[] searchKeywordsLike = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getValuesWithOperator(Operator.JSONB_FIND_LIKE);
        Object[] searchKeywords = null;
        Operator jsonbExactOrLikeOperator = null;
        if ((CollectionUtils.hasValue(searchKeywordsExact)) && (CollectionUtils.hasValue(searchKeywordsLike))) {
            throw new DSPDMException("Unable to perform search. Invalid search criteria filter found with operator {}", executionContext.getExecutorLocale(), Operator.JSONB_FIND_LIKE.name() + " and " + Operator.JSONB_FIND_EXACT);
        } else if (CollectionUtils.hasValue(searchKeywordsExact)) {
            // search keywords are already converted to string with lower case
            jsonbExactOrLikeOperator = Operator.JSONB_FIND_EXACT;
            searchKeywords = searchKeywordsExact;
        } else if (CollectionUtils.hasValue(searchKeywordsLike)) {
            // search keywords are already converted to string with lower case
            jsonbExactOrLikeOperator = Operator.JSONB_FIND_LIKE;
            searchKeywords = searchKeywordsLike;
        } else {
            throw new DSPDMException("Unable to perform search. No search criteria filter found with operator {}", executionContext.getExecutorLocale(), Operator.JSONB_FIND_LIKE.name() + " or " + Operator.JSONB_FIND_EXACT);
        }
        boSearchBusinessObjectInfo.addFilter(DSPDMConstants.BoAttrName.SEARCH_JSONB, jsonbExactOrLikeOperator, searchKeywords);
        // remove all jsonb related filters from the actual business object info
        businessObjectInfo.getFilterList().removeFiltersWithOperator(jsonbExactOrLikeOperator);
         
     	List<BusinessObjectInfo> businessObjectInfoClones =new ArrayList<>();
     	// iterate over main and join list to clone the BusinessObjectInfo
     	BusinessObjectInfo boSearchBusinessObjectInfoClone = boSearchBusinessObjectInfo.clone(executionContext);
     	boSearchBusinessObjectInfoClone.addFilter(DSPDMConstants.BoAttrName.BO_NAME, businessObjectInfo.getBusinessObjectType());
        // now propagate all remaining filters to the search business object info
		if (businessObjectInfo.getFilterList() != null) {
			for (CriteriaFilter filter : businessObjectInfo.getFilterList().getFilters()) {
				boSearchBusinessObjectInfoClone.addFilter(DSPDMConstants.BoAttrName.OBJECT_JSONB + DSPDMConstants.SPACE + Operator.JSONB_DOT_FOR_TEXT.getOperator() + " '" + filter.getColumnName() + "' ", filter.getOperator(), filter.getValues());
			}
		}		
		businessObjectInfoClones.add(boSearchBusinessObjectInfoClone);
		// set the boName and entity/aliasName
		Map<String, Object> boNameEntityMap=new HashMap<>();
		if (StringUtils.hasValue(businessObjectInfo.getAlias())) {
			boNameEntityMap.put(businessObjectInfo.getBusinessObjectType(), businessObjectInfo.getAlias());
		} else {
			boNameEntityMap.put(businessObjectInfo.getBusinessObjectType(), MetadataBOAttrReadDelegateImpl.getInstance()
                    .readMetadataBusinessObject(businessObjectInfo.getBusinessObjectType(), executionContext).get(DSPDMConstants.BoAttrName.ENTITY));
		}
		if (businessObjectInfo.hasSimpleJoinsToRead()) {
			for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
				boSearchBusinessObjectInfoClone = boSearchBusinessObjectInfo.clone(executionContext);
				boSearchBusinessObjectInfoClone.addFilter(DSPDMConstants.BoAttrName.BO_NAME, simpleJoinClause.getBoName());
				if (simpleJoinClause.getFilterList() != null) {
					for (CriteriaFilter filter : simpleJoinClause.getFilterList().getFilters()) {
						boSearchBusinessObjectInfoClone.addFilter(DSPDMConstants.BoAttrName.OBJECT_JSONB + DSPDMConstants.SPACE + Operator.JSONB_DOT_FOR_TEXT.getOperator() + " '" + filter.getColumnName() + "' ", filter.getOperator(), filter.getValues());
					}
				}
				businessObjectInfoClones.add(boSearchBusinessObjectInfoClone);
				
				if (StringUtils.hasValue(simpleJoinClause.getJoinAlias())) {
					boNameEntityMap.put(simpleJoinClause.getBoName(), simpleJoinClause.getJoinAlias());
				} else {
					boNameEntityMap.put(simpleJoinClause.getBoName(), MetadataBOAttrReadDelegateImpl.getInstance()
                            .readMetadataBusinessObject(simpleJoinClause.getBoName(), executionContext).get(DSPDMConstants.BoAttrName.ENTITY));
				}
			}
		}
		if (businessObjectInfo.hasDynamicJoinsToRead()) {
			for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
				boSearchBusinessObjectInfoClone = boSearchBusinessObjectInfo.clone(executionContext);
				boSearchBusinessObjectInfoClone.addFilter(DSPDMConstants.BoAttrName.BO_NAME, dynamicJoinClause.getDynamicTables());
				if (dynamicJoinClause.getFilterList() != null) {
					for (CriteriaFilter filter : dynamicJoinClause.getFilterList().getFilters()) {
						boSearchBusinessObjectInfoClone.addFilter(DSPDMConstants.BoAttrName.OBJECT_JSONB + DSPDMConstants.SPACE + Operator.JSONB_DOT_FOR_TEXT.getOperator() + " '" + filter.getColumnName() + "' ", filter.getOperator(), filter.getValues());
					}
				}
				businessObjectInfoClones.add(boSearchBusinessObjectInfoClone);

				for (DynamicTable dynamicTable : dynamicJoinClause.getDynamicTables()) {
					if (StringUtils.hasValue(dynamicJoinClause.getJoinAlias())) {
						boNameEntityMap.put(dynamicTable.getBoName(), dynamicJoinClause.getJoinAlias());
					} else {
						boNameEntityMap.put(dynamicTable.getBoName(), MetadataBOAttrReadDelegateImpl.getInstance()
                                .readMetadataBusinessObject(dynamicTable.getBoName(), executionContext).get(DSPDMConstants.BoAttrName.ENTITY));
					}
				}			
			}
		}
		// go for call the search and then the results
		searchResults = getSearchResultsPagedList(businessObjectInfo, businessObjectInfoClones, boNameEntityMap, executionContext);
		return searchResults;
    }

    private PagedList<DynamicDTO> getSearchResultsPagedList(BusinessObjectInfo businessObjectInfo, List<BusinessObjectInfo> boSearchBusinessObjectInfos, Map<String, Object> boNameEntityMap, ExecutionContext executionContext) {
        int totalRecords = 0;
        List<DynamicDTO> businessObjectList = null;
        if (businessObjectInfo.isReadRecordsCount()) {
        	for(BusinessObjectInfo boSearchBusinessObjectInfo : boSearchBusinessObjectInfos) { 
	            // read BO_SEARCH records count
	            totalRecords += DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).count(boSearchBusinessObjectInfo, executionContext);
        	}
            if (totalRecords > 0) {
                businessObjectList = getSearchResultsList(businessObjectInfo, boSearchBusinessObjectInfos, boNameEntityMap, executionContext);
            } else {
                businessObjectList = new ArrayList<>(0);
            }
        } else {
            businessObjectList = getSearchResultsList(businessObjectInfo, boSearchBusinessObjectInfos, boNameEntityMap, executionContext);
        }
        totalRecords = businessObjectList.size();
        return new PagedList<>(totalRecords, businessObjectList);
    }

    private List<DynamicDTO> getSearchResultsList(BusinessObjectInfo businessObjectInfo, List<BusinessObjectInfo> boSearchBusinessObjectInfos, Map<String, Object> boNameEntityMap, ExecutionContext executionContext) {    	
        List<DynamicDTO> businessObjectList = null;
        for(BusinessObjectInfo boSearchBusinessObjectInfo : boSearchBusinessObjectInfos) { 
        	BusinessObjectInfo businessObjectInfoClone= businessObjectInfo.clone(executionContext);
	        // set pagination parameters to the bo_search business object info
	        if (businessObjectInfoClone.isReadAllRecords()) {
	            boSearchBusinessObjectInfo.setReadAllRecords(true);
	        } else {
	            boSearchBusinessObjectInfo.getPagination().setRecordsPerPage(businessObjectInfoClone.getPagination().getRecordsPerPage(), executionContext);
	            if (CollectionUtils.hasValue(businessObjectInfoClone.getPagination().getPagesToRead())) {
	                boSearchBusinessObjectInfo.getPagination().setPagesToRead(executionContext, businessObjectInfoClone.getPagination().getPagesToRead().toArray(new Integer[0]));
	            }
	            // !important now remove pagination from the main business objects
	            businessObjectInfoClone.setReadAllRecords(true);
	        }
	        // set order by if needed
	        if (businessObjectInfoClone.getOrderBy() != null) {
	            Map<String, Boolean> orderMap = businessObjectInfoClone.getOrderBy().getOrderMap();
	            for (Map.Entry<String, Boolean> e : orderMap.entrySet()) {
	                String boAttrName = e.getKey();
	                if (Boolean.TRUE == e.getValue()) {
	                    boSearchBusinessObjectInfo.addOrderByAsc(boAttrName);
	                } else {
	                    boSearchBusinessObjectInfo.addOrderByDesc(boAttrName);
	                }
	            }
	        }
	        // read BO_SEARCH records
	        List<DynamicDTO> boSearchList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext).read(boSearchBusinessObjectInfo, executionContext);
	        if (CollectionUtils.hasValue(boSearchList)) {
	            DynamicDTO firstBoSearchObject = boSearchList.get(0);
				String primaryKeyColumnNamesInString = (String) firstBoSearchObject.get(DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES);
	            String[] primaryColumnNames =primaryKeyColumnNamesInString.split(DSPDMConstants.Separator.BO_SEARCH_ATTR_SEPARATOR);
                BaseJoinClause joinClauseToApplyFilter = null;
                // find the appropriate join clause place to add the filters
                if ((businessObjectInfoClone.isJoiningRead()) && (boNameEntityMap.get(firstBoSearchObject.get(DSPDMConstants.BoAttrName.BO_NAME)) != null)) {
                    String alias = (String) boNameEntityMap.get(firstBoSearchObject.get(DSPDMConstants.BoAttrName.BO_NAME));
                    joinClauseToApplyFilter = businessObjectInfoClone.findFirstJoinClauseForJoinAlias(alias);
                    if (joinClauseToApplyFilter == null) {
                        joinClauseToApplyFilter = businessObjectInfoClone.findFirstJoinClauseForBoName((String) firstBoSearchObject.get(DSPDMConstants.BoAttrName.BO_NAME));
                    }
                }
				if (businessObjectList == null) {
					businessObjectList = new ArrayList<>(0);
				}
				businessObjectList = Stream.of(businessObjectList,
						BOSearchHelper.getInstance(executionContext).readBusinessObjectListUsingBoSearchResultsList(businessObjectInfoClone, joinClauseToApplyFilter, boSearchList, primaryColumnNames, executionContext))
						.flatMap(Collection::stream).distinct().collect(Collectors.toList());
	        }
        }
        return businessObjectList;
    }
}
