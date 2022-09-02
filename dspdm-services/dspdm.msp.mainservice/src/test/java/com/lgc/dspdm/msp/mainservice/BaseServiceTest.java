package com.lgc.dspdm.msp.mainservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
/*import com.lgc.dist.core.msp.service.ServiceUtils;
import com.lgc.dist.core.msp.test.ServerTestRule;*/
import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.config.security.SecurityServerTokenUtils;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.client.mainservice;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.utils.HttpFileUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

public class BaseServiceTest {
    /*@ClassRule
    public static ServerTestRule<com.lgc.dspdm.msp.mainservice.client.mainservice> rule = new ServerTestRule<mainservice>(mainservice.class);
    @Mock
    static mainservice mainservice;
    private static DSPDMLogger logger = new DSPDMLogger(BaseServiceTest.class);
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    private String MAIN_SERVICE_NAME = "mainservice";
    private String ENDPOINT_PATH_CUSTOM_BOE = "/secure/custom/boe";

    @BeforeClass
    public static void init() {
        // specify a higher ranking (10) to bind the Mocked instance over the real one (ranking is 0)
        ServiceUtils.getInjector().bind(mainservice, mainservice.class, 10);

        ExecutionContext executionContext = null;
        try {
            executionContext = ExecutionContext.getTestUserExecutionContext();
        } catch (Exception e) {
            logger.error("Unable to get execution context for test user");
            DSPDMException.throwException(e, Locale.getDefault());
        }
        // initialize application configurations
        AppConfigInit.init(PropertySource.MAP_PROVIDED, executionContext);
    }

    protected String getServiceName() {
        return MAIN_SERVICE_NAME;
    }

    protected String getEndpointCustomBOE() {
        return ENDPOINT_PATH_CUSTOM_BOE;
    }

    /**
     * Reads dynamic token from security server using the unit test user name and password defined in the connection properties file
     * It will invoke the key cloak rest service internally to verify user name and password and then get the new token
     *
     * @return
     * @throws IOException
     * @author Rao.Ali khan
     * @since 10-Jul-2020
     */
   /* protected String getTestUserToken() {
        String token = null;
        if (ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
            token = SecurityServerTokenUtils.getBearerTokenFromSecurityServer(ExecutionContext.getTestUserExecutionContext());
        }
        return token;
    }

    //controller: read===============================
    protected DSPDMResponse commonGet(String boName) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/common/" + boName, null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    protected DSPDMResponse commonCountGet(String boName) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/common/count/" + boName, null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    protected DSPDMResponse commonPost(BOQuery boQuery) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/common", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<BOQuery> entity = Entity.entity(boQuery, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse commonCountPost(BOQuery boQuery) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/common/count", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<BOQuery> entity = Entity.entity(boQuery, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected String parseImportPost(String url, String fileParam, List<File> uploadFileList, Map<String, String> paramMap) {
        return HttpFileUtil.upload(url, fileParam, uploadFileList, paramMap, getHeadersMap());
    }

    *//*
    protected DSPDMResponse customPost(BOQuery boQuery) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/custom", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<BOQuery> entity = Entity.entity(boQuery, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }*//*

    protected DSPDMResponse customPostBoe(BOQuery boQuery) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/custom/boe", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<BOQuery> entity = Entity.entity(boQuery, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    //controller: write===============================
    protected DSPDMResponse savePost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/save", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    //controller: delete===============================
    protected DSPDMResponse deleteDelete(String boName, Integer id) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/delete/" + boName + "/" + id, null, MediaType.APPLICATION_JSON);
        return invokeDeleteRequest(requestBuilder);
    }

    protected DSPDMResponse deletePost(Map<String, Object> formParams) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/delete", null, MediaType.APPLICATION_JSON);
        Entity<Map<String, Object>> entity = Entity.entity(formParams, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    //controller: download===============================
    protected Response exportGet(String boName) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/export/" + boName, MediaType.APPLICATION_OCTET_STREAM_TYPE, MediaType.APPLICATION_OCTET_STREAM);
        return invokeBaseHttpRequest(() -> {
            return requestBuilder.buildGet();
        });
    }

    protected Response exportPost(BOQuery... entitiesObject) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/export", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_OCTET_STREAM);
        Entity<Object> entitys = Entity.entity(entitiesObject, MediaType.APPLICATION_JSON_TYPE);
        return invokeBaseHttpRequest(() -> {
            return requestBuilder.buildPost(entitys);
        });
    }

    // controller: config user roles cache
    protected DSPDMResponse clearAuthorizationCache() {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/config/clearAuthorizationCache", null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    // controller: config user roles cache
    protected DSPDMResponse refreshAuthorizationCacheForUser(String userId) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/config/refreshAuthorizationCacheForUser/" + userId, null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    //controller: metadata.config===============================
    protected DSPDMResponse deleteUserAuthorizationCache(String userId) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/config/deleteUserAuthorizationCache/" + userId, null, MediaType.APPLICATION_JSON);
        return invokeDeleteRequest(requestBuilder);
    }

    //controller: metadata.config===============================
    protected DSPDMResponse refreshMetadataGet() {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/config/refreshMetadata", null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    //controller: metadata.config===============================
    protected DSPDMResponse validateMetadataGet(String boName) {
        String url = "/secure/config/validateMetadata" + ((StringUtils.hasValue(boName)) ? "/" + boName : "");
        Invocation.Builder requestBuilder = getBaseRequestBuilder(url, null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    //controller: metadata.config===============================
    protected DSPDMResponse boHierarchyGet(String boName) {
        String url = "/secure/boHierarchy" + ((StringUtils.hasValue(boName)) ? "/" + boName : "");
        Invocation.Builder requestBuilder = getBaseRequestBuilder(url, null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    //controller: metadata.config===============================
    protected DSPDMResponse validateAndRefreshMetadataGet() {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/config/validateAndRefreshMetadata", null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    //controller: metadata.read===============================
    protected DSPDMResponse readAllBusinessObjectsGet() {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/metadata/read/bo/list", null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    //controller: metadata.write===============================
    protected DSPDMResponse introduceNewBusinessObjectsPost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/introduceNewBusinessObjects", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse dropBusinessObjects(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/dropBusinessObjects", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse addRelationships(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/addRelationships", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse dropRelationships(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/dropRelationships", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse addUniqueConstraints(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/addUniqueConstraints", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse addSearchIndexes(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/addSearchIndexes", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse dropUniqueConstraints(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/dropUniqueConstraints", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse dropSearchIndexes(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/dropSearchIndexes", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse addBusinessObjectGroups(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/addBusinessObjectGroups", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse addCustomAttributesPost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/addCustomAttributes", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse updateCustomAttributesPost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/updateCustomAttributes", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse deleteCustomAttributesPost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/deleteCustomAttributes", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse generateMetadataForExistingTablePost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/generateMetadataForExistingTable", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse generateMetadataForAllPost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/generateMetadataForAll", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse deleteMetadataForAllPost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/deleteMetadataForAll", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse deleteMetadataForExistingTablePost(String body) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/metadata/write/deleteMetadata", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<String> entity = Entity.entity(body, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    //controller: upload.import===============================
    protected DSPDMResponse importPost(FileInputStream fileInputStream, String contentType) {
        //TODO No implementation
//        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/import", null, contentType);
//        Entity<FileInputStream> entity = Entity.entity(fileInputStream, MediaType.MULTIPART_FORM_DATA_TYPE);
//        return invokePostRequest(requestBuilder, entity);
        return null;
    }

    //controller: upload.parse===============================
    protected DSPDMResponse parseImportPost(FileInputStream fileInputStream, String contentType) {
        //TODO No implementation
//        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/parse", null, contentType);
//        Entity<FileInputStream> entity = Entity.entity(fileInputStream, MediaType.MULTIPART_FORM_DATA_TYPE);
//        return invokePostRequest(requestBuilder, entity);
        return null;
    }

    protected DSPDMResponse parseBoNamesPost(FileInputStream fileInputStream, String contentType) {
        //TODO No implementation
//        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/parse/boNames",null, contentType);
//        Entity<FileInputStream> entity = Entity.entity(fileInputStream, MediaType.MULTIPART_FORM_DATA_TYPE);
//        return invokePostRequest(requestBuilder, entity);
        return null;
    }

    protected DSPDMResponse parseBulkPost(FileInputStream fileInputStream, String contentType) {
        //TODO No implementation
//        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/parse/bulk", null, contentType);
//        Entity<FileInputStream> entity = Entity.entity(fileInputStream, MediaType.MULTIPART_FORM_DATA_TYPE);
//        return invokePostRequest(requestBuilder, entity);
        return null;
    }

    //search
    protected DSPDMResponse searchPost(BOQuery boQuery) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/search", MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        Entity<BOQuery> entity = Entity.entity(boQuery, MediaType.APPLICATION_JSON_TYPE);
        return invokePostRequest(requestBuilder, entity);
    }

    protected DSPDMResponse searchPostIndexAllForBoName(String boName) {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/search/indexAll/" + boName, MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON);
        return invokePostRequest(requestBuilder, null);
    }

    //base method
    protected String getWebBaseUrl() {
        return rule.getWebTarget().getUri().toString().replace("0.0.0.0", "localhost");
    }

    //data types controller: reveal all supported data types 
    protected DSPDMResponse revealSupportedDataTypesGet() {
        Invocation.Builder requestBuilder = getBaseRequestBuilder("/secure/supportedDataTypes", null, MediaType.APPLICATION_JSON);
        return invokeGetRequest(requestBuilder);
    }

    private Invocation.Builder getBaseRequestBuilder(String path, MediaType consumes, String produces) {
        String token = null;
        try {
            token = getTestUserToken();
        } catch (Throwable e) {
            logger.error(e);
            DSPDMException.throwException(e, ExecutionContext.getEmptyExecutionContext());
        }
        Invocation.Builder requestBuilder = rule.getWebTarget().path(path).request();
        if (consumes != null) {
            requestBuilder.accept(consumes);
        }
        if (produces != null) {
            requestBuilder.header("content-type", produces);
        }
        requestBuilder.header("Authorization", token);
        // add header for unit test call
        requestBuilder.header("unit_test_case_context", true);
        return requestBuilder;
    }

    private Map<String, Object> getHeadersMap() {
        String token = null;
        Map<String, Object> headersMap = new HashMap<>(2);
        try {
            token = getTestUserToken();
        } catch (Throwable e) {
            logger.error(e);
            DSPDMException.throwException(e, ExecutionContext.getEmptyExecutionContext());
        }

        headersMap.put("Authorization", token);
        // add header for unit test call
        headersMap.put("unit_test_case_context", true);
        return headersMap;
    }

    private Response invokeBaseHttpRequest(Supplier<Invocation> invocationSupplier) {
        Response response = null;
        try {
            Invocation invocation = invocationSupplier.get();
            response = invocation.invoke(); // synchronous with wait for response
        } catch (Throwable e) {
            logger.error(e);
            DSPDMException.throwException(e, ExecutionContext.getEmptyExecutionContext());
        }
        return response;
    }

    private DSPDMResponse invokeBaseHttpRequestEx(Supplier<Invocation> invocationSupplier) {
        DSPDMResponse dspdmResponse = null;
        ExecutionContext executionContext = null;
        try {
            Invocation invocation = invocationSupplier.get();
            Response response = invocation.invoke(); // synchronous with wait for response
            try {
                dspdmResponse = response.readEntity(DSPDMResponse.class);
            } catch (Exception e) {
            }
            if ((dspdmResponse != null) && (dspdmResponse.getStatus() != null) && (dspdmResponse.getExecutionContext() != null)) {
                // first get execution context from response
                executionContext = dspdmResponse.getExecutionContext();
                if (dspdmResponse.getException() != null) {
                    throw dspdmResponse.getException();
                }
            } else {
                throw new DSPDMException(response.getStatus(), response.getStatusInfo().getReasonPhrase(), (dspdmResponse == null) ? null : dspdmResponse.getException(), Locale.ENGLISH);
            }
        } catch (Throwable e) {
            logger.error(e);
            DSPDMException.throwException(e, (executionContext != null) ? executionContext : ExecutionContext.getEmptyExecutionContext());
        }
        return dspdmResponse;
    }

    private DSPDMResponse invokeGetRequest(Invocation.Builder requestBuilder) {
        return invokeBaseHttpRequestEx(() -> {
            return requestBuilder.buildGet();
        });
    }

    private DSPDMResponse invokePostRequest(Invocation.Builder requestBuilder, Entity<?> entity) {
        return invokeBaseHttpRequestEx(() -> {
            return requestBuilder.buildPost(entity);
        });
    }

    private DSPDMResponse invokePutRequest(Invocation.Builder requestBuilder, Entity<?> entity) {
        return invokeBaseHttpRequestEx(() -> {
            return requestBuilder.buildPut(entity);
        });
    }

    private DSPDMResponse invokeDeleteRequest(Invocation.Builder requestBuilder) {
        return invokeBaseHttpRequestEx(() -> {
            return requestBuilder.buildDelete();
        });
    }*/
}
