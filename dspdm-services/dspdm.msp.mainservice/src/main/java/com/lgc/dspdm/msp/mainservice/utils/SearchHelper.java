package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.data.criteria.BaseFilter;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;

/**
 * Class for search utility method related to the rest controller class
 *
 * @author muhammad.tanveer
 */
public abstract class SearchHelper {

    private static final DSPDMLogger logger = new DSPDMLogger(SearchHelper.class);

    public static void validateCriteriaFiltersValues(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        if (businessObjectInfo.getFilterList() != null) {
            // validating criteria filter
            List<CriteriaFilter> criteriaFilterList = businessObjectInfo.getFilterList().getFilters();
            if (CollectionUtils.hasValue(criteriaFilterList)) {
                for (CriteriaFilter criteriaFilter : criteriaFilterList) {
                    validateFilter(criteriaFilter, executionContext);
                }
            }
            // validating having filter
            List<HavingFilter> havingFilterList = businessObjectInfo.getFilterList().getHavingFilters();
            if (CollectionUtils.hasValue(havingFilterList)) {
                for (HavingFilter havingFilter : havingFilterList) {
                    validateFilter(havingFilter, executionContext);
                }
            }
            // BusinessObjectInfo can have simple joins and dynamic joins and those joins can also have criteria filters and having filters
            if(CollectionUtils.hasValue(businessObjectInfo.getDynamicJoins())){
                for(DynamicJoinClause dynamicJoinClause: businessObjectInfo.getDynamicJoins()){
                    if(dynamicJoinClause.getFilterList() != null){
                        List<CriteriaFilter> criteriaFilterListFromDynamicJoin = dynamicJoinClause.getFilterList().getFilters();
                        if (CollectionUtils.hasValue(criteriaFilterListFromDynamicJoin)) {
                            for (CriteriaFilter criteriaFilter : criteriaFilterListFromDynamicJoin) {
                                validateFilter(criteriaFilter, executionContext);
                            }
                        }
                        List<HavingFilter> havingFilterListFromDynamicJoin = dynamicJoinClause.getFilterList().getHavingFilters();
                        if (CollectionUtils.hasValue(havingFilterListFromDynamicJoin)) {
                            for (HavingFilter havingFilter : havingFilterListFromDynamicJoin) {
                                validateFilter(havingFilter, executionContext);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void validateFilter(BaseFilter filter, ExecutionContext executionContext) {
        if (filter.getOperator().equals(Operator.JSONB_FIND_EXACT)
                || filter.getOperator().equals(Operator.JSONB_FIND_LIKE)) {
            Object[] values = filter.getValues();
            for (Object value : values) {
                if (value.equals(DSPDMConstants.SpecialCharactersSearchAPI.SINGLE_QUOTES)
                        || value.equals(DSPDMConstants.SpecialCharactersSearchAPI.DOUBLE_QUOTES)
                        || value.equals(DSPDMConstants.SpecialCharactersSearchAPI.HYPHEN)
                        || value.equals(DSPDMConstants.SpecialCharactersSearchAPI.UNDERSCORE)) {
                    throw new DSPDMException("Invalid character(s) are not allowed in search string '{}'", executionContext.getExecutorLocale(),
                            value);
                }
                if (value instanceof String) {
                    if (((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.ROUND_BRACKET_OPEN)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.ROUND_BRACKET_CLOSE)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.SQUARE_BRACKET_OPEN)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.SQUARE_BRACKET_CLOSE)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.DOUBLE_QUOTES)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.QUESTION_MARK)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.PLUS)
                            || ((String) value).contains(DSPDMConstants.SpecialCharactersSearchAPI.ASTERISK)) {
                        throw new DSPDMException("Invalid character(s) are not allowed in search string '{}'", executionContext.getExecutorLocale(),
                                value);
                    }
                }
            }
        }
    }
}