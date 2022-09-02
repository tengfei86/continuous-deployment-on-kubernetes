package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.utils.security.AuthTokenUtils;
import com.lgc.dspdm.msp.mainservice.utils.security.AuthTokenInfo;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.Locale;
import java.util.TimeZone;

public class BaseController {
    protected static DSPDMLogger logger = new DSPDMLogger(BaseController.class);
    private static Throwable initException = null;

    static {
        ExecutionContext executionContext = null;
        try {
            executionContext = ExecutionContext.getSystemUserExecutionContext();
            try {
                // initialize application configurations
                AppConfigInit.init(PropertySource.MAP_PROVIDED, executionContext);
            } catch (Exception e) {
                initException = e;
            }
        } catch (Exception e) {
            logger.error("Unable to get execution context for system user");
            initException = e;
        }
    }

    protected ExecutionContext getCurrentUserExecutionContext(HttpHeaders httpHeaders, String defaultUserName, String apiRequestType) {
        if (initException != null) {
            DSPDMException.throwException(initException, Locale.getDefault());
        }
        Locale locale = getLocale(httpHeaders);
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        AuthTokenUtils.validateAccessTokenFromHttpHeaders(httpHeaders, executionContext);
        AuthTokenInfo authTokenInfo = AuthTokenUtils.getTokenInfoFromToken(httpHeaders, executionContext.getExecutorLocale());
        String userName = authTokenInfo.getUserName();
        if (StringUtils.isNullOrEmpty(userName)) {
            userName = defaultUserName;
        }
        String userId = authTokenInfo.getSubject();
        TimeZone timeZone = getTimezone(httpHeaders);
        executionContext.setExecutorLocale(locale);
        executionContext.setExecutorName(userName);
        executionContext.setExecutorTimeZone(timeZone.toZoneId());

        if ((!apiRequestType.equalsIgnoreCase(DSPDMConstants.ApiPermission.OPEN))) {
            AuthTokenUtils.validateApiPermissions(userId, apiRequestType, executionContext);
        }

        if (StringUtils.hasValue(httpHeaders.getHeaderString("unit_test_case_context"))) {
            if (Boolean.valueOf(httpHeaders.getHeaderString("unit_test_case_context"))) {
                executionContext.setUnitTestCall(true);
            }
        }
        Thread.currentThread().setName(userName + "_" + StringUtils.getNextRandomNumber(5));
        return executionContext;
    }

    private Locale getLocale(HttpHeaders httpHeaders) {
        Locale locale = (CollectionUtils.hasValue(httpHeaders.getAcceptableLanguages())) ? httpHeaders.getAcceptableLanguages().get(0) : Locale.getDefault();
        if (locale.getLanguage().equals("*")) {
            locale = locale.getDefault();
        }
        return locale;
    }

    private TimeZone getTimezone(HttpHeaders httpHeaders) {
        return TimeZone.getDefault();
    }

    protected void setLanguage(String language, ExecutionContext executionContext) {
        if (language != null) {
            Locale locale = LocaleUtils.getLocale(language.trim().toLowerCase());
            if (locale != null) {
                // change locale in execution context too
                executionContext.setExecutorLocale(locale);
            } else {
                throw new DSPDMException("Value for parameter language is not recognized : {}", executionContext.getExecutorLocale(), language);
            }
        } else {
            throw new DSPDMException("Parameter language (case sensitive) is required", executionContext.getExecutorLocale());
        }
    }

    protected void setTimezone(String timezoneStr, ExecutionContext executionContext) {
        if (timezoneStr != null) {
            TimeZone timezone = LocaleUtils.getTimezone(timezoneStr.trim());
            if (timezone != null) {
                // change timezone in execution context too
                executionContext.setExecutorTimeZone(timezone.toZoneId());
            } else {
                throw new DSPDMException("Value for parameter timezone is not recognized : {}", executionContext.getExecutorLocale(), timezoneStr);
            }
        } else {
            throw new DSPDMException("Parameter timezone (case sensitive) is required", executionContext.getExecutorLocale());
        }
    }

    protected Response handleSaveException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForSave((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleDeleteException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForDelete((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleAddCustomAttributeException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForAddCustomAttribute((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleDeleteCustomAttributeException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForDeleteCustomAttribute((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleAddBusinessObjectException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForAddBusinessObject((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleDropBusinessObjectException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForDropBusinessObject((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleDeleteBusinessObjectMetadataException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForDeleteBusinessObjectMetadata((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleAddUniqueConstraintException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForAddUniqueConstraint((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleAddSearchIndexException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForAddSearchIndex((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }

    protected Response handleAddRelationshipException(Throwable e, ExecutionContext executionContext) {
        logger.error(e.getMessage(), e);
        if ((e instanceof DSPDMException) && (StringUtils.hasValue(((DSPDMException) e).getSqlState())) && ((e.getCause() != null) && (e.getCause() != e))) {
            e = SQLState.getActualExceptionForAddRelationship((DSPDMException) e, executionContext);
        }
        // CREATE ERROR RESPONSE
        return new DSPDMResponse(e, executionContext).getResponse();
    }
}
