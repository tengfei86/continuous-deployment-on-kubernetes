package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.data.common.DSPDMMessage;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.utils.DTOHelper;
import com.lgc.dspdm.msp.mainservice.utils.metadata.*;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.service.common.fixed.metadata.write.IMetadataWriteService;
import com.lgc.dspdm.service.common.fixed.metadata.write.MetadataWriteService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/")
@PermitAll
public class MetadataChangeRestServiceImpl extends mainserviceImpl {
    protected static DSPDMLogger logger = new DSPDMLogger(MetadataChangeRestServiceImpl.class);

    protected IDynamicReadService dynamicReadService;
    protected IMetadataWriteService metadataWriteService;

    //controller: metadata.write===============================
    @POST
    @Path("/metadata/write/introduceNewBusinessObjects")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Creates a new business object",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response introduceNewBusinessObjects(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_BUSINESS_OBJECT_ADD);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToSaveOrUpdate = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DEFINE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToSaveOrUpdate)) {
                throw new DSPDMException("No business object found to be defined.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToSaveOrUpdate.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT))) {
                throw new DSPDMException("No business object found to be defined with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
            List<DynamicDTO> boListToSave = boNameAndBOListMapToSaveOrUpdate.get(DSPDMConstants.BoName.BUSINESS_OBJECT);
            //validate
            MetadataChangeRestServiceHelper.validateIntroduceNewBusinessObjects(boListToSave, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call save service
            SaveResultDTO saveResultDTO = metadataWriteService.introduceNewBusinessObjects(boListToSave, executionContext);

            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show inserted records message if any record is inserted
            if (saveResultDTO.getCreatedDBTablesCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business objects(s) defined successfully.",
                        saveResultDTO.getCreatedDBTablesCount()));
            }
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isInsertExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Insert executed but no business object(s) added" +
                            ". Please make sure that data is correct."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddBusinessObjectException(e, executionContext);
        }
        // log time taken by whole request and the time taken by
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/dropBusinessObjects")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Deletes existing business object definition",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response dropBusinessObjects(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_BUSINESS_OBJECT_DELETE);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeDropped = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DROP, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeDropped)) {
                throw new DSPDMException("No business object found to be dropped.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeDropped.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT))) {
                throw new DSPDMException("No business object found to be dropped with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
            List<DynamicDTO> boListToBeDropped = boNameAndBOListMapToBeDropped.get(DSPDMConstants.BoName.BUSINESS_OBJECT);
            //validate
            MetadataChangeRestServiceHelper.validateDropBusinessObjects(boListToBeDropped, dynamicReadService, executionContext);
            // verify that we are not going to delete any metadata from here
            DTOHelper.verifyNoMetadataBusinessObjectExists("drop", CollectionUtils.prepareMapOfListFromValuesOfKey(boListToBeDropped, DSPDMConstants.BoAttrName.BO_NAME), dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call drop service
            SaveResultDTO saveResultDTO = metadataWriteService.dropBusinessObjects(boListToBeDropped, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show dropped records message if any business object is dropped
            if (saveResultDTO.getDroppedDBTablesCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business objects(s) dropped successfully.",
                        saveResultDTO.getDroppedDBTablesCount()));
            }

            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDropBusinessObjectException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/dropRelationships")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Deletes existing business object relationships definition",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response dropRelationships(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_RELATIONSHIP);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeDropped = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DROP, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeDropped)) {
                throw new DSPDMException("No business object relationship found to be dropped.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeDropped.containsKey(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP))) {
                throw new DSPDMException("No business object relationship found to be dropped with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            }
            List<DynamicDTO> busObjRelationshipsToDrop = boNameAndBOListMapToBeDropped.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            //validate
            MetadataRelationshipChangeRestServiceHelper.validateDropBusinessObjectRelationships(busObjRelationshipsToDrop, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call drop service
            SaveResultDTO saveResultDTO = metadataWriteService.dropBusinessObjectRelationships(busObjRelationshipsToDrop, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show dropped records message if any business object is dropped
            if (saveResultDTO.getDroppedDBRelationshipsCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business object relationship(s) dropped successfully.",
                        saveResultDTO.getDroppedDBRelationshipsCount()));
            }

            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            // CREATE ERROR RESPONSE
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/addRelationships")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Add relationships between existing business objects",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response addRelationships(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_RELATIONSHIP);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeCreated = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.CREATE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeCreated)) {
                throw new DSPDMException("No business object relationship found to be created.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeCreated.containsKey(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP))) {
                throw new DSPDMException("No business object relationship found to be created with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            }
            List<DynamicDTO> busObjRelationshipsToCreate = boNameAndBOListMapToBeCreated.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            //validate
            MetadataRelationshipChangeRestServiceHelper.validateAddBusinessObjectRelationships(busObjRelationshipsToCreate, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call add service
            SaveResultDTO saveResultDTO = metadataWriteService.addBusinessObjectRelationships(busObjRelationshipsToCreate, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show dropped records message if any business object is dropped
            if (saveResultDTO.getCreatedDBRelationshipsCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business object relationship(s) added successfully.",
                        saveResultDTO.getCreatedDBRelationshipsCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddRelationshipException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/addUniqueConstraints")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Add unique constraints on attributes of an existing business objects",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response addUniqueConstraints(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_UNIQUE_CONSTRAINT);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeCreated = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.CREATE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeCreated)) {
                throw new DSPDMException("No unique constraint found to be created.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeCreated.containsKey(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS))) {
                throw new DSPDMException("No unique constraint found to be created with type '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            }
            List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated = boNameAndBOListMapToBeCreated.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            //validate
            MetadataUniqueConstraintChangeRestServiceHelper.validateAddUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call add service
            SaveResultDTO saveResultDTO = metadataWriteService.addUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show added records message if any business object is added
            if (saveResultDTO.getCreatedDBUniqueConstraintsCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} unique constraints(s) added successfully.",
                        saveResultDTO.getCreatedDBUniqueConstraintsCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddUniqueConstraintException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/addSearchIndexes")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Add search index on attributes of an existing business objects",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response addSearchIndexes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_SEARCH_INDEX);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeCreated = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.CREATE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeCreated)) {
                throw new DSPDMException("No search index found to be created.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeCreated.containsKey(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES))) {
                throw new DSPDMException("No search index found to be created with type '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES);
            }
            List<DynamicDTO> busObjAttrSearchIndexToBeCreated = boNameAndBOListMapToBeCreated.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES);
            //validate
            MetadataSearchIndexChangeRestServiceHelper.validateAddSearchIndexes(busObjAttrSearchIndexToBeCreated, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call add service
            SaveResultDTO saveResultDTO = metadataWriteService.addSearchIndexes(busObjAttrSearchIndexToBeCreated, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show added records message if any business object is added
            if (saveResultDTO.getCreatedDBSearchIndexesCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} search index(es) added successfully.",
                        saveResultDTO.getCreatedDBSearchIndexesCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddSearchIndexException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/dropUniqueConstraints")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Drop unique constraints from attributes of an existing business objects",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response dropUniqueConstraints(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_UNIQUE_CONSTRAINT);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeDropped = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DROP, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeDropped)) {
                throw new DSPDMException("No unique constraint found to be dropped.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeDropped.containsKey(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS))) {
                throw new DSPDMException("No unique constraint found to be dropped with type '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            }
            List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped = boNameAndBOListMapToBeDropped.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            //validate
            MetadataUniqueConstraintChangeRestServiceHelper.validateDropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call drop service
            SaveResultDTO saveResultDTO = metadataWriteService.dropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show dropped records message if any business object unique constraint has been dropped
            if (saveResultDTO.getDroppedDBUniqueConstraintsCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} unique constraint(s) dropped successfully.",
                        saveResultDTO.getDroppedDBUniqueConstraintsCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            // CREATE ERROR RESPONSE
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/dropSearchIndexes")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Drop search indexes from attributes of an existing business objects",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response dropSearchIndexes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_SEARCH_INDEX);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeDropped = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DROP, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeDropped)) {
                throw new DSPDMException("No search indexes found to be dropped.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeDropped.containsKey(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES))) {
                throw new DSPDMException("No search indexes found to be dropped with type '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES);
            }
            List<DynamicDTO> busObjAttrSearchIndexesToBeDropped = boNameAndBOListMapToBeDropped.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES);
            //validate
            MetadataSearchIndexChangeRestServiceHelper.validateDropSearchIndexes(busObjAttrSearchIndexesToBeDropped, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call drop service
            SaveResultDTO saveResultDTO = metadataWriteService.dropSearchIndexes(busObjAttrSearchIndexesToBeDropped, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show dropped records message if any business object search indexes has been dropped
            if (saveResultDTO.getDroppedDBSearchIndexesCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} search index(es) dropped successfully.",
                        saveResultDTO.getDroppedDBSearchIndexesCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            // CREATE ERROR RESPONSE
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/generateMetadataForExistingTable")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Generates metadata for an existing table or view",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response generateMetadataForExistingTable(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER,
                    DSPDMConstants.ApiPermission.GENERATE_METADATA_FOR_EXISTING_TABLE_VIEW);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToSaveOrUpdate = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DEFINE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToSaveOrUpdate)) {
                throw new DSPDMException("No business object found to generate metadata for existing table or view.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToSaveOrUpdate.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT))) {
                throw new DSPDMException("No business object found to generate metadata for existing table or view with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
            List<DynamicDTO> boListToGenerateMetadata = boNameAndBOListMapToSaveOrUpdate.get(DSPDMConstants.BoName.BUSINESS_OBJECT);
            //validate
            MetadataChangeRestServiceHelper.validateGenerateMetadataForExistingTable(boListToGenerateMetadata, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // set sequence names
            MetadataChangeRestServiceHelper.setSequenceNamesForGenerateMetadata(boListToGenerateMetadata, dynamicReadService, executionContext);
            // Call save service
            SaveResultDTO saveResultDTO = metadataWriteService.generateMetadataForExistingTable(boListToGenerateMetadata,  null, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show inserted records message if any record is inserted
            if (saveResultDTO.isAnyRecordInserted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business objects(s) metadata generated successfully.",
                        saveResultDTO.getInsertedRecordsCount()));
            }
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isInsertExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Insert executed but no business object metadata generated" +
                            ". Please make sure that request is correct."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddBusinessObjectException(e, executionContext);
        }
        // log time taken by whole request and the time taken by
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/deleteMetadata")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Deletes metadata for an existing table or view",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response deleteMetadataForExistingTable(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER,
                    DSPDMConstants.ApiPermission.DELETE_METADATA_FOR_EXISTING_TABLE_VIEW);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToDelete = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DROP, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToDelete)) {
                throw new DSPDMException("No business object found to delete metadata.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToDelete.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT))) {
                throw new DSPDMException("No business object found to delete metadata with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
            List<DynamicDTO> boListToDeleteMetadata = boNameAndBOListMapToDelete.get(DSPDMConstants.BoName.BUSINESS_OBJECT);
            //validate
            MetadataChangeRestServiceHelper.validateDeleteMetadataForExistingTable(boListToDeleteMetadata, dynamicReadService, executionContext);

            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call save service
            SaveResultDTO saveResultDTO = metadataWriteService.deleteMetadataForExistingTable(boListToDeleteMetadata, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show deleted records message if any record is deleted
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business objects(s) metadata deleted successfully.",
                        saveResultDTO.getDeletedRecordsCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();


        } catch (Throwable e) {
            response = handleDeleteBusinessObjectMetadataException(e, executionContext);
        }
        // log time taken by whole request and the time taken by
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/addBusinessObjectGroups")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Add business object group",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response addBusinessObjectGroups(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            //TO-DO: business object group permission
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, DSPDMConstants.ApiPermission.CUSTOM_BUSINESS_OBJECT_ADD);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToBeCreated = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.CREATE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToBeCreated)) {
                throw new DSPDMException("No business object group found to be created.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToBeCreated.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP))) {
                throw new DSPDMException("No business object group found to be created with type '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP);
            }
            List<DynamicDTO> busObjGroupsToBeCreated = boNameAndBOListMapToBeCreated.get(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP);
            //validate
            MetadataGroupChangeRestServiceHelper.validateAddBusinessObjectGroups(busObjGroupsToBeCreated, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call add service
            SaveResultDTO saveResultDTO = metadataWriteService.addBusinessObjectGroups(busObjGroupsToBeCreated, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show added records message if any business object is added
            setOperationMessageByBoName(DSPDMConstants.API_Operation.SAVE, messages, saveResultDTO);
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddUniqueConstraintException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/generateMetadataForAll")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Generates metadata for all tables/views of connected database",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response generateMetadataForAll(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER,
                    DSPDMConstants.ApiPermission.GENERATE_METADATA_FOR_ALL);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToSaveOrUpdate = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DEFINE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToSaveOrUpdate)) {
                throw new DSPDMException("No business object found to generate metadata for existing table or view.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToSaveOrUpdate.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT))) {
                throw new DSPDMException("No business object found to generate metadata for existing table or view with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
            // we'll be getting only one bo
            DynamicDTO boToGenerateMetadataForAll = boNameAndBOListMapToSaveOrUpdate.get(DSPDMConstants.BoName.BUSINESS_OBJECT).get(0);
            //validate
            MetadataChangeRestServiceHelper.validateGenerateMetadataForAll(boToGenerateMetadataForAll, dynamicReadService, executionContext);
            // generate exclusion list that will entail metadata tables and user provided tables that should be ignored
            // while generating metadata and add it to execution context
            Set<String> exclusionList = MetadataChangeRestServiceHelper.generateExclusionList(boToGenerateMetadataForAll, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // get all entity names in relationship order (i.e., on the basis of parent/child relationship like parent will always come
            // before child). This list will be used to generate dynamic dto list and will be passed to
            // our already created server i.e., generateMetadataForExistingTable to generate metadata for all
            List<String> entityNamesList =dynamicReadService.getAllEntityNamesListOfConnectedDB(boToGenerateMetadataForAll, exclusionList, executionContext);
            if (!CollectionUtils.hasValue(entityNamesList)) {
                throw new DSPDMException("No table/view found to generate metadata for database name '{}' and schema name '{}'", executionContext.getExecutorLocale(),
                        boToGenerateMetadataForAll.get(DSPDMConstants.DATABASE_NAME), boToGenerateMetadataForAll.get(DSPDMConstants.SCHEMA_NAME));
            }
            // now checking overwrite case
            String overwritePolicy = (String) boToGenerateMetadataForAll.get(DSPDMConstants.OVERWRITE_POLICY);
            // with overwrite policy, we can make decision what to do if we got metadata for a table already generated.
            MetadataChangeRestServiceHelper.verifyOverwritePolicyForGenerateMetadataForAll(entityNamesList, overwritePolicy, dynamicReadService, executionContext);
            logger.info("Final list of table names for generate metadata {}", CollectionUtils.getCommaSeparated(entityNamesList));
            List<DynamicDTO> dynamicDTOList = new ArrayList<>(entityNamesList.size());
            // now creating dynamicDTOList against which metadata will be generated
            for (String tableName : entityNamesList) {
                DynamicDTO dynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUSINESS_OBJECT, null, executionContext);
                dynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, tableName.replaceAll("_", " ").toUpperCase());
                dynamicDTO.put(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, StringUtils.toCamelCase(tableName.replaceAll("_", " ")));
                dynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, tableName);
                dynamicDTO.put(DSPDMConstants.DATABASE_NAME, boToGenerateMetadataForAll.get(DSPDMConstants.DATABASE_NAME));
                dynamicDTO.put(DSPDMConstants.SCHEMA_NAME, boToGenerateMetadataForAll.get(DSPDMConstants.SCHEMA_NAME));
                dynamicDTOList.add(dynamicDTO);
            }
            // set sequence names
            MetadataChangeRestServiceHelper.setSequenceNamesForGenerateMetadata(dynamicDTOList, dynamicReadService, executionContext);
            // the following flag will help us to identify that this request is for creating metadataForAll. And on the basis of
            // below flag, we'll make some decisions on already available service i.e., generateMetadataForExistingTable
            executionContext.setGenerateMetadataForAll(true);
            // Call generate metadata service
            SaveResultDTO saveResultDTO = metadataWriteService.generateMetadataForExistingTable(dynamicDTOList, exclusionList, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                // Since sending whole response might cause client application to hang, what we'll be
                // doing is sending only BO Names. For doing this with minimal changes, we've extracted
                // bo names from the readBack data and set to the main response map.
                List<Object> boList = CollectionUtils.getValuesFromList(saveResultDTO.getDataFromReadBack().get(DSPDMConstants.BoName.BUSINESS_OBJECT), DSPDMConstants.BoAttrName.BO_NAME);
                Map<String, List<Object>> tempResponseMap = new HashMap<>();
                tempResponseMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT, boList);
                map = new HashMap<>(tempResponseMap);
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show inserted records message if any record is inserted
            if (saveResultDTO.isAnyRecordInserted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business objects(s) metadata generated successfully.",
                        saveResultDTO.getInsertedRecordsCount()));
            }
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isInsertExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Insert executed but no business object metadata generated" +
                            ". Please make sure that request is correct."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddBusinessObjectException(e, executionContext);
        }
        // log time taken by whole request and the time taken by
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/deleteMetadataForAll")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Delete metadata for all tables/views of connected database",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response deleteMetadataForAll(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER,
                    DSPDMConstants.ApiPermission.DELETE_METADATA_FOR_ALL);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToDelete = DTOHelper.buildBONameAndBOListMapFromRequestJSON(
                    formParams, DSPDMConstants.CRAWLER_OPERATION.DEFINE, true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToDelete)) {
                throw new DSPDMException("No business object found to delete metadata for existing table or view.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToDelete.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT))) {
                throw new DSPDMException("No business object found to delete metadata for existing table or view with type '{}'.", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
            // we'll be getting only one bo
            DynamicDTO boToDeleteMetadataForAll = boNameAndBOListMapToDelete.get(DSPDMConstants.BoName.BUSINESS_OBJECT).get(0);
            //validate
            MetadataChangeRestServiceHelper.validateDeleteMetadataForAll(boToDeleteMetadataForAll, dynamicReadService, executionContext);
            // we should not be deleting metadata of metadata tables. so adding them in exclusion list.
            Set<String> exclusionList = MetadataChangeRestServiceHelper.generateExclusionList(boToDeleteMetadataForAll, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Get all entity names in order that parent will come always before child. This is same used for generateMetadataForALl
            // without transaction opening and should come from DAO LAYER
            List<String> entityNamesList = dynamicReadService.getAllEntityNamesListOfConnectedDB(boToDeleteMetadataForAll, exclusionList, executionContext);
            // reverse needed because above list has parent first and then child. So for deleting, we gotta delete child first
            Collections.reverse(entityNamesList);
            List<DynamicDTO> dynamicDTOList = new ArrayList<>(entityNamesList.size());
            for (String tableName : entityNamesList) {
                DynamicDTO dynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUSINESS_OBJECT, null, executionContext);
                dynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, tableName.replaceAll("_", " ").toUpperCase());
                dynamicDTOList.add(dynamicDTO);
            }
            // now checking overwrite case. If there's no available metadata to delete, remove from the list
            MetadataChangeRestServiceHelper.checkOverwriteForDeleteAllMetadata(dynamicDTOList, dynamicReadService, executionContext);
            executionContext.setDeleteMetadataForAll(true);
            // Call delete metadata service
            SaveResultDTO saveResultDTO = metadataWriteService.deleteMetadataForExistingTable(dynamicDTOList, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                // Since sending whole response might cause client application to hang, what we'll be
                // doing is sending only BO Names. For doing this with minimal changes, we've extracted
                // bo names from the readBack data and set to the main response map.
                List<Object> boList = CollectionUtils.getValuesFromList(saveResultDTO.getDataFromReadBack().get(DSPDMConstants.BoName.BUSINESS_OBJECT), DSPDMConstants.BoAttrName.BO_NAME);
                Map<String, List<Object>> tempResponseMap = new HashMap<>();
                tempResponseMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT, boList);
                map = new HashMap<>(tempResponseMap);
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show deleted records message if any record is deleted
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} business objects(s) metadata deleted successfully.",
                        saveResultDTO.getDeletedRecordsCount()));
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();

        } catch (Throwable e) {
            response = handleAddBusinessObjectException(e, executionContext);
        }
        // log time taken by whole request and the time taken by
        executionContext.logTotalProcessingTime();
        return response;
    }
}
