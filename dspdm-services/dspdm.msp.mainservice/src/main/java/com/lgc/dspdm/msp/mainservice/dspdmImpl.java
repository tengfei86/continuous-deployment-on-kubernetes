package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.JoinType;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.LocaleUtils;
import com.lgc.dspdm.msp.mainservice.model.*;
import com.lgc.dspdm.msp.mainservice.model.join.JoiningCondition;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;
import com.lgc.dspdm.msp.mainservice.utils.BusinessObjectBuilder;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import java.util.*;
import java.util.regex.Pattern;

public class dspdmImpl extends mainserviceImpl {
	// implementation moved to mainservicImpl.java

/*
	protected Response businessObjects(String language, Float timezoneOffset, String businessObjectType, String boName,
			Boolean readAttributes, Boolean readConstraints, Boolean readRelationships, Boolean readGroups,
			String select, String filter, String orderby, Boolean readUnique, Boolean readFirst, Boolean readMetadata,
			Boolean readMetadataConstraints, Boolean readReferenceData, Boolean readReferenceDataForFilters, Boolean readReferenceDataConstraints, Boolean readRecordsCount,
			Boolean readWithDistinct, Boolean readAllRecords, Boolean isUploadNeeded, Integer page, Integer size,
			HttpHeaders httpHeaders) {
		if (dynamicReadService == null) {
			dynamicReadService = DynamicReadService.getInstance();
		}
		ExecutionContext executionContext = getCurrentUserExecutionContext(httpHeaders,
				DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.VIEW);
		Response response = null;
		try {
			// SET BOQUERY
			BOQuery entity = initBoQuery(language, timezoneOffset, page, size);
			entity.setBoName(businessObjectType);
			// SET SELECT
			if (select != null && !select.isEmpty()) {
				List<String> selects = parseSelect(select);
				if (selects.size() > 0) {
					entity.setSelectList(selects);
				}
			}
			// SET CRITERIA FILTERS
			List<CriteriaFilter> criteriaFilters = new ArrayList<CriteriaFilter>();
			if (boName != null && !boName.isEmpty()) {
				criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS,
						new Object[] { boName.trim() }));
			}
			if (filter != null) {
				String filterReplace = Pattern.compile(DSPDMConstants.AND, Pattern.CASE_INSENSITIVE | Pattern.LITERAL)
						.matcher(filter).replaceAll(DSPDMConstants.AND);
				criteriaFilters.addAll(parseFilter(filterReplace));
			}
			if (criteriaFilters.size() > 0) {
				entity.setCriteriaFilters(criteriaFilters);
			}
			// SET SIMPLEJOINS
			List<SimpleJoinClause> simpleJoins = new ArrayList<SimpleJoinClause>();
			// SET READATTRIBUTES SIMPLEJOIN
			if (readAttributes != null && readAttributes) {
				simpleJoins.add(parserSimpleJoin(entity, businessObjectType, JoinType.INNER.name(),
						DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS,
						DSPDMConstants.BoAttrName.BO_NAME));
			}
			// SET READCONSTRAINTS SIMPLEJOIN
			if (readConstraints != null && readConstraints) {
				simpleJoins.add(parserSimpleJoin(entity, businessObjectType, JoinType.INNER.name(),
						DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, DSPDMConstants.BoAttrName.BO_NAME,
						Operator.EQUALS, DSPDMConstants.BoAttrName.BO_NAME));
			}
			// SET READRELATIONSHIPS SIMPLEJOIN
			if (readRelationships != null && readRelationships) {
				simpleJoins.add(parserSimpleJoin(entity, businessObjectType, JoinType.INNER.name(),
						DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS,
						DSPDMConstants.BoAttrName.CHILD_BO_NAME));
			}
			// SET READGROUPS SIMPLEJOIN
			if (readGroups != null && readGroups) {
				simpleJoins.add(parserSimpleJoin(entity, businessObjectType, JoinType.INNER.name(),
						DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS,
						DSPDMConstants.BoAttrName.BO_NAME));
			}
			if (simpleJoins.size() > 0) {
				entity.setSimpleJoins(simpleJoins);
			}
			// SET ORDER BY
			if (orderby != null) {
				List<OrderBy> orderBys = parseOrderBy(orderby);
				if (orderBys.size() > 0) {
					entity.setOrderBy(orderBys);
				}
			}
			if (readUnique != null) {
				entity.setReadUnique(readUnique);
			}
			if (readFirst != null) {
				entity.setReadFirst(readFirst);
			}
			if (readMetadata != null) {
				entity.setReadMetadata(readMetadata);
			}
			if (readMetadataConstraints != null) {
				entity.setReadMetadataConstraints(readMetadataConstraints);
			}
			if (readReferenceData != null) {
				entity.setReadReferenceData(readReferenceData);
			}
			if (readReferenceDataForFilters != null) {
				entity.setReadReferenceDataForFilters(readReferenceDataForFilters);
			}
			if (readReferenceDataConstraints != null) {
				entity.setReadReferenceDataConstraints(readReferenceDataConstraints);
			}
			if (readRecordsCount != null) {
				entity.setReadRecordsCount(readRecordsCount);
			}
			if (readWithDistinct != null) {
				entity.setReadWithDistinct(readWithDistinct);
			}
			if (readAllRecords != null) {
				entity.setReadAllRecords(readAllRecords);
			}
			if (isUploadNeeded != null) {
				entity.setUploadNeeded(isUploadNeeded);
			}
			// SET BusinessObjectInfo
			BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(entity, executionContext);
			// FETCH LIST FROM DATA STORE
			Map<String, Object> map = dynamicReadService.readWithDetails(businessObjectInfo, executionContext);
			// CREATE SUCCESS RESPONSE
			DSPDMResponse dspdmResponse = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext);
			response = Response.ok().entity(dspdmResponse).build();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			DSPDMResponse dspdmResponse = new DSPDMResponse(e, executionContext);
			response = Response.serverError().entity(dspdmResponse).build();
		}
		// log time taken by whole request and the time taken by db
		executionContext.logTotalProcessingTime();
		return response;
	}

	*//**
	 * initialize the BOQuery from language,timezoneOffset,page and size
	 *
	 * @param language
	 *            LOCALE/LANGUAGE
	 * @param timezoneOffset
	 *            a digital representation of time zone (-8.25timezoneOffset means
	 *            GMT -8:15timezone)
	 * @param page
	 *            the page number
	 * @param size
	 *            the records number of per page
	 * @return BOQuery
	 *
	 * @author Qinghua Ma
	 * @since 14-Sep-2020
	 *//*
	private BOQuery initBoQuery(String language, Float timezoneOffset, Integer page, Integer size) {
		// SET BOQUERY
		BOQuery entity = new BOQuery();
		// SET LOCALE/LANGUAGE
		if (language != null && language.trim().length() > 0) {
			entity.setLanguage(language);
		} else {
			entity.setLanguage(Locale.getDefault().getLanguage());
		}
		// SET TIMEZONE
		if (timezoneOffset != null) {
			entity.setTimezoneOffset(timezoneOffset);
		} else {
			entity.setTimezone(LocaleUtils.getTimezone(TimeZone.getDefault()));
		}
		// SET ReadAllRecords
		entity.setReadAllRecords(false);
		// SET pagination(default 20)(default 20)(default 20)(default 20)(default
		// 20)(default 20)(default 20)
		if (page != null && page > 0 && size != null && size > 0) {
			entity.setPagination(new Pagination(size, Arrays.asList(page)));
		}
		return entity;
	}

	*//**
	 * converts select from String type to String List
	 *
	 * @param strSelect
	 *            string of select
	 * @return list of String
	 *
	 * @author Qinghua Ma
	 * @since 16-Sep-2020
	 *//*
	private List<String> parseSelect(String strSelect) {
		String[] selects = strSelect.split(Character.toString(DSPDMConstants.COMMA));
		List<String> strSelects = new ArrayList<String>();
		for (String select : selects) {
			strSelects.add(select.trim());
		}
		return strSelects;
	}

	*//**
	 * converts filter criteria from String type to CriterialFilter List
	 *
	 * @param strFilter
	 *            string of filter
	 * @return list of CriteriaFilter
	 *
	 * @author Qinghua Ma
	 * @since 14-Sep-2020
	 *//*
	private List<CriteriaFilter> parseFilter(String strFilter) {
		String[] filters = strFilter.split(DSPDMConstants.AND);
		List<CriteriaFilter> criteriaFilters = new ArrayList<CriteriaFilter>();
		for (String filter : filters) {
			for (Operator operator : Operator.values()) {
				String[] splitFilters = filter.split(operator.getRestOperator());
				if (splitFilters != null && splitFilters.length == 2) {
					criteriaFilters.add(new CriteriaFilter(splitFilters[0].trim(), operator,
							new Object[] { splitFilters[1].trim() }));
					break;
				}
			}
		}
		return criteriaFilters;
	}

	*//**
	 * converts order by from String type to OrderBy List
	 *
	 * @param strOrderby string of order by
	 * @return list of OrderBy
	 * @author Qinghua Ma
	 * @since 14-Sep-2020
	 *//*
	private List<OrderBy> parseOrderBy(String strOrderby) {
		String[] orderBys = strOrderby.split(Character.toString(DSPDMConstants.COMMA));
		List<OrderBy> orderByLst = new ArrayList<OrderBy>();
		for (String orderBy : orderBys) {
			String[] splitOrderBys = orderBy.split(Character.toString(DSPDMConstants.SPACE));
			if (splitOrderBys != null && splitOrderBys[0] != null) {
				orderByLst.add(new OrderBy(splitOrderBys[0].trim(),
						splitOrderBys.length == 2 ? Order.valueOf(splitOrderBys[1]) : Order.ASC));
			}
		}
		return orderByLst;
	}

	*//**
	 * compose the SimpleJoinClause
	 *
	 * @param entity
	 *            BOQuery
	 * @param boNameLeft
	 *            left boname of join
	 * @param joinType
	 *            joinType
	 * @param boNameRight
	 *            right boname of join
	 * @param boAttrNameLeft
	 *            left boattrname of join
	 * @param operator
	 *            operator of left and right boattrname
	 * @param boAttrNameRight
	 *            right boattrname of join
	 * @return a SimpleJoinClause
	 *
	 * @author Qinghua Ma
	 * @since 14-Sep-2020
	 *//*
	private SimpleJoinClause parserSimpleJoin(BOQuery entity, String boNameLeft, String joinType, String boNameRight,
			String boAttrNameLeft, Operator operator, String boAttrNameRight) {
		String leftJoinAlias = boNameLeft.replace(" ", DSPDMConstants.UNDERSCORE);
		entity.setJoinAlias(leftJoinAlias);
		String rightJoinAlias = boNameRight.replace(" ", DSPDMConstants.UNDERSCORE);
		SimpleJoinClause simpleJoinClause = new SimpleJoinClause();
		simpleJoinClause.setBoName(boNameRight);
		simpleJoinClause.setJoinType(joinType);
		simpleJoinClause.setJoinAlias(rightJoinAlias);
		JoiningCondition joiningCondition = new JoiningCondition();
		JoiningCondition.JoiningConditionOperand leftSide = new JoiningCondition.JoiningConditionOperand();
		leftSide.setJoinAlias(leftJoinAlias);
		leftSide.setBoAttrName(boAttrNameLeft);
		joiningCondition.setLeftSide(leftSide);
		joiningCondition.setOperator(operator);
		JoiningCondition.JoiningConditionOperand rightSide = new JoiningCondition.JoiningConditionOperand();
		rightSide.setJoinAlias(rightJoinAlias);
		rightSide.setBoAttrName(boAttrNameRight);
		joiningCondition.setRightSide(rightSide);
		simpleJoinClause.setJoiningConditions(Arrays.asList(joiningCondition));
		return simpleJoinClause;
	}*/
}
