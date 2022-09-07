package com.lgc.dspdm.msp.mainservice;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.config.WafByPassRulesProperties;
import com.lgc.dspdm.core.common.data.common.BulkImportResponseDTO;
import com.lgc.dspdm.core.common.data.common.DSPDMMessage;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.DSPDMUnit;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.JoinType;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.msp.mainservice.client.mainservice;
import com.lgc.dspdm.msp.mainservice.model.*;
import com.lgc.dspdm.msp.mainservice.model.join.JoiningCondition;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;
import com.lgc.dspdm.msp.mainservice.utils.*;
import com.lgc.dspdm.msp.mainservice.utils.metadata.MetadataAttributeChangeRestServiceHelper;
import com.lgc.dspdm.msp.mainservice.utils.security.AuthTokenUtils;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.service.common.dynamic.write.DynamicWriteService;
import com.lgc.dspdm.service.common.dynamic.write.IDynamicWriteService;
import com.lgc.dspdm.service.common.fixed.metadata.write.IMetadataWriteService;
import com.lgc.dspdm.service.common.fixed.metadata.write.MetadataWriteService;
import com.lgc.dspdm.service.utils.excel.ExcelExportUtil;
import com.lgc.dspdm.service.utils.excel.ExcelImportJoinUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.poi.ss.usermodel.Workbook;
import org.glassfish.jersey.media.multipart.FormDataParam;


import java.io.*;
import java.util.*;
import java.util.regex.Pattern;


/**
 * @author service Developer
 * @label Template for microservices implementation
 * @documentationExample An example simple microservices implementations. To support resource auto scanning,
 * resource annotations (such as @path) need to be presented in this implementation; annotations
 * in the superclass/interface are ignored.
 */
@Path("/secure")
public class mainserviceImpl extends BaseController implements mainservice {

    protected static DSPDMLogger logger = new DSPDMLogger(mainserviceImpl.class);

    //@Inject
    protected IDynamicReadService dynamicReadService;
    protected IDynamicWriteService dynamicWriteService;
    protected IMetadataWriteService metadataWriteService;

    @GET
    @Path("/hierarchy/{hierarchyName}")
    @Produces("application/json")
    @Operation(summary = "Get total number of records for the given hierarchic name",
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
    public Response getHierarchyGet(
            @Parameter(description = "name of hierarchy object", required = true)
            @PathParam("hierarchyName") String hierarchyName, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            BOQuery entity = new BOQuery();
            entity.setBoName(hierarchyName);
            List<HierarchyObjectInfo> hierarchyObjectInfo = HierarchyBuilder.build(entity, dynamicReadService, executionContext);
            Map<String, Object> map = null;
            List<HierarchyResult> hierarchyList = null;
            if (hierarchyObjectInfo.size() > 0) {
                map = new HashMap<>(hierarchyObjectInfo.size());
                for (HierarchyObjectInfo hierarchyObject : hierarchyObjectInfo) {
                    hierarchyList = HierarchyHelper.convertHierarchyDataToHierarchyStruct(hierarchyObject, executionContext);
                    map.put(hierarchyObject.getHierarchyName(), new PagedList<>(hierarchyList.size(), hierarchyList));
                }
            } else {
                map = new HashMap<>();
            }
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/Hierarchy")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(summary = "Get list of dynamicDTO resources",
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
    public Response getHierarchyPost(
            @Parameter(description = "query entity", required = true)
                    BOQuery entity, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            List<HierarchyObjectInfo> hierarchyObjectInfo = HierarchyBuilder.build(entity, dynamicReadService, executionContext);
            Map<String, Object> map = null;
            if (hierarchyObjectInfo.size() > 0) {
                List<HierarchyResult> hierarchyList = HierarchyHelper.convertBODataToHierarchyStruct(hierarchyObjectInfo.get(0), dynamicReadService, executionContext);
                map = new HashMap<>(1);
                map.put(hierarchyObjectInfo.get(0).getHierarchyName(), new PagedList<>(hierarchyList.size(), hierarchyList));
            } else {
                map = new HashMap<>();
            }
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }


    @POST
    @Path("/custom/boe")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(
            summary = "Get list of dynamicDTO for BOE",
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
    public Response getCustomBOEResponsePost(
            @Parameter(description = "query entity", required = true)
                    BOQuery entity, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(entity, executionContext);
            // read max volume date from whole table
            PagedList<DynamicDTO> pagedList = dynamicReadService.readBOEVolumeData(businessObjectInfo, executionContext);
            // CREATE MAP TO ADD DATA TO RESPONSE
            Map<String, Object> map = new HashMap<>(1);
            // ADD PAGED LIST TO DATA
            map.put(DSPDMConstants.BoName.WELL_VOL_DAILY, pagedList);
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: read===============================
    @GET
    @Path("/common/{boName}")
    @Produces("application/json")
    @Operation(
            summary = "Get list of dynamicDTO resources",
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
    public Response getCustomResponseGet(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            // FETCH LIST FROM DATA STORE
            List<DynamicDTO> result = dynamicReadService.read(boName, executionContext);
            // CREATE MAP TO ADD DATA TO RESPONSE
            Map<String, Object> map = new HashMap<>(1);
            // ADD PAGED LIST TO DATA
            map.put(boName, new PagedList<>(result.size(), result));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @GET
    @Path("/common/count/{boName}")
    @Produces("application/json")
    @Operation(summary = "Get total number of records for the given BO name",
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
    public Response count(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName,
            @Parameter(description = "list of business object body", required = true)
                    List<DynamicDTO> list,
            @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(boName, executionContext);
            // FETCH COUNT FROM DATA STORE
            int count = dynamicReadService.count(businessObjectInfo, executionContext);
            Map<String, Object> map = new HashMap<>(1);
            map.put("count", count);
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/common")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(summary = "Get list of dynamicDTO resources",
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
    public Response getCustomResponsePost(
            @Parameter(description = "query entity", required = true)
                    BOQuery entity, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);

            BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(entity, executionContext);
            Map<String, DynamicDTO> metadataMap =
                    dynamicReadService.readMetadataMapForBOName(businessObjectInfo.getBusinessObjectType(),
                            executionContext);
            DTOHelper.convertDataTypeForFilters(businessObjectInfo, metadataMap, "read", executionContext,
                    executionContext.getExecutorName(), dynamicReadService);
            // FETCH LIST FROM DATA STORE
            Map<String, Object> map = dynamicReadService.readWithDetails(businessObjectInfo, executionContext);
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/common/count")
    @Consumes("application/json")
    @Produces("application/json")
    @Operation(summary = "Get number of total records",
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
    public Response count(
            @Parameter(description = "query entity", required = true)
                    BOQuery entity, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(entity, executionContext);
            // FETCH COUNT FROM DATA STORE
            int count = dynamicReadService.count(businessObjectInfo, executionContext);
            Map<String, Object> map = new HashMap<>(1);
            map.put("count", count);
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: write===============================
    @POST
    @Path("/save")
    @Consumes("application/json")
    @Produces("application/json")
    public Response saveOrUpdate(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            // check that the save request has only unsecure bo names to be saved
            boolean onlyUnsecureBonames = true;
            for (Map.Entry<String, Object> e : formParams.entrySet()) {
                String boName = e.getKey();
                if (!(DSPDMConstants.UNSECURE_BO_NAMES.isUnSecureBoName(boName))) {
                    onlyUnsecureBonames = false;
                    break;
                }
            }
            if (onlyUnsecureBonames) {
                // just validate the token and no need to validate the permissions
                executionContext = getCurrentUserExecutionContext(httpHeaders,
                        DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.OPEN);
            } else {
                // validate the token and also validate the permissions
                executionContext = getCurrentUserExecutionContext(httpHeaders,
                        DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EDIT);
            }
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToSaveOrUpdate =
                    DTOHelper.buildBONameAndBOListMapFromRequestJSON(formParams, "save/update", true, dynamicReadService, executionContext);
            // verify that we are not going to save or update any metadata from here
            DTOHelper.verifyNoMetadataBusinessObjectExists("save/update", boNameAndBOListMapToSaveOrUpdate, dynamicReadService, executionContext);
            // save r_reporting_entity_kind in execution context which will be used for sync data between REPORTING ENTITY & Associated Object
            executionContext.setrReportingEntityKindDTOList(dynamicReadService.readSimple(
                    (new BusinessObjectInfo(DSPDMConstants.BoName.R_REPORTING_ENTITY_KIND, executionContext).setReadAllRecords(true)), executionContext));
            // Call save service
            SaveResultDTO saveResultDTO = dynamicWriteService.saveOrUpdate(boNameAndBOListMapToSaveOrUpdate,
                    executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>();
            setOperationMessageByBoName(DSPDMConstants.API_Operation.SAVE, messages, saveResultDTO);
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isUpdateExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Update executed but no records updated" +
                            ". Please make sure primary key values exist.", saveResultDTO.getIgnoredRecordsCount()));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
                if (saveResultDTO.isDeleteExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Delete executed but no records deleted" +
                            ". Please make sure primary key values exist.", saveResultDTO.getIgnoredRecordsCount()));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }

            if (!(executionContext.isReadBack())) {
                map.clear();
            }
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleSaveException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @DELETE
    @Path("/delete/{boName}/{id}")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "remove a specific business object by ID",
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
    public Response removeById(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName,
            @Parameter(description = "id of business object", required = true) @PathParam("id") Long id,
            @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.DELETE);
            // CREATE DYNAMICPK FOR QUERY
            DynamicPK pk = new DynamicPK(boName, id);
            // verify that we are not going to delete any metadata from here
            DynamicDTO businessObject = dynamicReadService.readMetadataBusinessObject(boName, executionContext);
            if (Boolean.TRUE.equals(businessObject.get(DSPDMConstants.BoAttrName.IS_METADATA_TABLE))) {
                throw new DSPDMException("Cannot perform delete on metadata business object '{}' directly. Please use dedicated metadata service.", executionContext.getExecutorLocale(), boName);
            }
            // FETCH DYNAMIC DTO FROM DATA STORE
            DynamicDTO result = dynamicReadService.readOne(pk, executionContext);
            if (result == null) {
                DSPDMResponse dspdmResponse = new DSPDMResponse(new DSPDMException("Business object '{}' with id '{}' does not exist.", executionContext.getExecutorLocale(), boName, id), executionContext);
                response = Response.status(404).entity(dspdmResponse).build();
                return response;
            }

            // CONSTRUCT MAP TO DELETE
            List<DynamicDTO> list = new ArrayList<>();
            list.add(result);
            Map<String, List<DynamicDTO>> boNameAndBOListMapToDelete = new HashMap<>();
            boNameAndBOListMapToDelete.put(boName, list);
            // COMMIT CHANGE
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DELETE_OPR.getId());
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call delete service
            SaveResultDTO saveResultDTO = dynamicWriteService.delete(boNameAndBOListMapToDelete,
                    executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(2);
            // show delete message if any deleted is updated
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} deleted successfully.",
                        saveResultDTO.getDeletedRecordsCount(), boName, id));
            }
            // show ignored records message if any record is ignored
            if (saveResultDTO.getIgnoredRecordsCount() > 0) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} ignored from delete because it is already deleted.", saveResultDTO.getIgnoredRecordsCount(), boName));
            }
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isDeleteExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Delete operation executed but no {} was deleted. Please make sure primary key values exist and the record is not already deleted.", boName));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }

            if (!(executionContext.isReadBack())) {
                map.clear();
            }
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Exception e) {
            response = handleDeleteException(e, executionContext);
        }
        return response;
    }

    @POST
    @Path("/delete")
    @Consumes("application/json")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "remove business objects provided",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "500", description = "Internal Server Error!"),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response removeByJson(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.DELETE);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToDelete =
                    DTOHelper.buildBONameAndBOListMapFromRequestJSON(formParams, "delete", false, dynamicReadService, executionContext);

            // verify that we are not going to delete any metadata from here
            DTOHelper.verifyNoMetadataBusinessObjectExists("delete", boNameAndBOListMapToDelete, dynamicReadService, executionContext);
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DELETE_OPR.getId());
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call delete service
            SaveResultDTO saveResultDTO = dynamicWriteService.delete(boNameAndBOListMapToDelete,
                    executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>();
            setOperationMessageByBoName(DSPDMConstants.API_Operation.DELETE, messages, saveResultDTO);
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isDeleteExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Delete operation executed but no record was deleted. Please make sure primary key values exist and the record(s) are not already deleted."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }

            if (!(executionContext.isReadBack())) {
                map.clear();
            }
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDeleteException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: download===============================
    @GET
    @Path("/export/{boName}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    //@GZIP
    @Operation(
            summary = "Get list of dynamicDTO resources",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/octet-stream",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response export(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EXPORT);
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(boName, executionContext);
            // SET READ METADATA ALONG WITH DATA
            businessObjectInfo.setReadMetadata(true);
            businessObjectInfo.setReadAllRecords(false);
            // FETCH LIST FROM DATA STORE
            Map<String, Object> businessObjectsAndMetadataMap = dynamicReadService.readWithDetails(businessObjectInfo
                    , executionContext);
            List<DynamicDTO> businessObjects =
                    (List<DynamicDTO>) businessObjectsAndMetadataMap.get(businessObjectInfo.getBusinessObjectType());
            List<DynamicDTO> metadata = (List<DynamicDTO>) businessObjectsAndMetadataMap.get("BUSINESS OBJECT ATTR");

            // read metadata map for the BO name to be exported
            Map<String, DynamicDTO> metadataMap = dynamicReadService
                    .readMetadataMapForBOName(businessObjectInfo.getBusinessObjectType(), executionContext);

            // CREATE SUCCESS RESPONSE
            Workbook workbook = null;
            byte[] excelSingleSheetBytes;
            try {
                workbook = ExcelExportUtil.createWorkBook(executionContext);
                ExcelExportUtil.exportToExcelSingleSheetStream(workbook, businessObjectInfo.getBusinessObjectType(),
                        metadata, businessObjects,  executionContext);
                excelSingleSheetBytes = ExcelExportUtil.convertExcelWorkbookToByteArray(workbook, executionContext);
            } finally {
                ExcelExportUtil.disposeWorkBook(workbook);
            }
            String excelFileName = businessObjectInfo.getBusinessObjectType() + ".xlsx";
            response = Response.ok(excelSingleSheetBytes, MediaType.APPLICATION_OCTET_STREAM_TYPE).header("content" +
                    "-disposition", "attachment; filename=\"" + excelFileName + "\"").build();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMResponse dspdmResponse = new DSPDMResponse(e, executionContext);
            if (e instanceof DSPDMException) {
                DSPDMException exception = (DSPDMException) e;
                response = Response.status(exception.getHttpStatusCode()).type(MediaType.APPLICATION_JSON_TYPE).entity(dspdmResponse).build();
            } else {
                response = Response.serverError().type(MediaType.APPLICATION_JSON_TYPE).entity(dspdmResponse).build();
            }
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/export")
    @Consumes("application/json")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    //@GZIP
    @Operation(summary = "Get list of dynamicDTO resources",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/octet-stream",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            }
    )
    public Response export(
            @Parameter(description = "query entity", required = true)
                    Object entitysObject, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        Workbook workbook = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EXPORT);

            byte[] excelSingleSheetBytes = null;
            String excelFileName = "";
            //To an array of BOQuerys
            List<BOQuery> entitys = new ArrayList();
            ObjectMapper objectMapper = new ObjectMapper();
            String entitysString = objectMapper.writeValueAsString(entitysObject);
            if (entitysObject instanceof List) {
                entitys = JSON.parseArray(entitysString, BOQuery.class);
            } else {
                entitys.add(JSON.parseObject(entitysString, BOQuery.class));
            }
            workbook = ExcelExportUtil.createWorkBook(executionContext);
            for (BOQuery entity : entitys) {
                // create BusinessObjectInfo instance from request
                BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(entity, executionContext);
                // read metadata map for the BO name to be exported
                Map<String, DynamicDTO> metadataMap = dynamicReadService.readMetadataMapForBOName(businessObjectInfo.getBusinessObjectType(), executionContext);
                // Convert data types of filter values to actual from String. Actual data type is in metadata
                DTOHelper.convertDataTypeForFilters(businessObjectInfo, metadataMap, "read", executionContext,
                        executionContext.getExecutorName(), dynamicReadService);
                Object[] searchKeywordsExact = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getValuesWithOperator(Operator.JSONB_FIND_EXACT);
                Object[] searchKeywordsLike = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getValuesWithOperator(Operator.JSONB_FIND_LIKE);
                boolean searchApplied = false;
                if (CollectionUtils.hasValue(searchKeywordsExact) || CollectionUtils.hasValue(searchKeywordsLike)) {
                    businessObjectInfo.setReadRecordsCount(false);
                    searchApplied = true;
                }
                // Get custom sheet name
                String sheetName = entity.getSheetName();
                // Going to call the service, pass same workbook for all the iterations of the loop
                ExportHelper.readAndConvertToExcelBinary(businessObjectInfo, workbook, sheetName, new ArrayList<>(metadataMap.values()), searchApplied, dynamicReadService, executionContext);
                // file name
                if (StringUtils.hasValue(excelFileName)) {
                    excelFileName += DSPDMConstants.UNDERSCORE;
                }
                excelFileName += StringUtils.hasValue(sheetName) ? sheetName : businessObjectInfo.getBusinessObjectType();
            }
            excelSingleSheetBytes = ExcelExportUtil.convertExcelWorkbookToByteArray(workbook, executionContext);
            excelFileName += DSPDMConstants.DOT + DSPDMConstants.FileExtentions.XLSX;
            // CREATE SUCCESS RESPONSE
            response = Response.ok(excelSingleSheetBytes, MediaType.APPLICATION_OCTET_STREAM_TYPE).header("content" +
                    "-disposition", "attachment; filename=\"" + excelFileName + "\"").build();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMResponse dspdmResponse = new DSPDMResponse(e, executionContext);
            if (e instanceof DSPDMException) {
                DSPDMException exception = (DSPDMException) e;
                response = Response.status(exception.getHttpStatusCode()).type(MediaType.APPLICATION_JSON_TYPE).entity(dspdmResponse).build();
            } else {
                response = Response.serverError().type(MediaType.APPLICATION_JSON_TYPE).entity(dspdmResponse).build();
            }
        } finally {
            // close workbook if created
            ExcelExportUtil.disposeWorkBook(workbook);
            // free memory of jvm
            long freeMemoryNow = Runtime.getRuntime().freeMemory();
            // close stream immediately to avoid out of memory error
            // very important, it is used and we do not need it anymore
            // release reference and make byte array it available for GC
            // request garbage collector to perform gc work as per its convenient
            System.gc();
            logger.info("Total memory freed after cleaning file buffer : {} MB", (Runtime.getRuntime().freeMemory() - freeMemoryNow) / (1024 * 1024));
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    private List<DynamicDTO> getUploadAttributes(Map<String, DynamicDTO> metadataMap, Boolean isUploadNeeded) {
        List<DynamicDTO> list = new ArrayList<>(metadataMap.values());
        if (isUploadNeeded == null) {
            return list;
        }
        // Filter only required attributes
        list.removeIf(dynamicDTO -> (!isUploadNeeded.equals(dynamicDTO.get(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED))));
        return list;
    }

    //controller: metadata.config===============================
    @GET
    @Path("/config/refreshMetadata")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "Reloads the metadata and refreshes its cache",
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
    public Response refreshMetadata(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.SYSTEM_ADMIN);
            dynamicReadService.refreshMetadata(executionContext);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Metadata reloaded successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, null,
                    executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: metadata.config===============================
    @GET
    @Path("/config/validateMetadata")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "validate metadata",
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
    public Response validateMetadata(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            Map<String, List<String>> resultList = dynamicReadService.validateMetadata(null, executionContext);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Metadata validate successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, new TreeMap<>(resultList),
                    executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: metadata.config===============================
    @GET
    @Path("/config/validateMetadata/{boName}")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "validate metadata for a single business object",
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
    public Response validateMetadataForBusinessObject(@Parameter(description = "name of business object", required = true)
                                                      @PathParam("boName") String boName, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            DynamicDTO businessObject = dynamicReadService.readMetadataBusinessObject(boName, executionContext);
            Map<String, List<String>> resultList = dynamicReadService.validateMetadata(businessObject, executionContext);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Metadata validate successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, new TreeMap<>(resultList),
                    executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: metadata.config===============================
    @GET
    @Path("/config/validateAndRefreshMetadata")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "validate metadata and Reloads the metadata and refreshes its cache",
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
    public Response validateAndRefreshMetadata(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            dynamicReadService.refreshMetadata(executionContext);
            Map<String, List<String>> resultList = dynamicReadService.validateMetadata(null, executionContext);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Metadata reloaded and validate successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, new TreeMap<>(resultList),
                    executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @GET
    @Path("/boHierarchy/{boName}")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "finds hierarchy for a business object",
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
    public Response boHierarchyForBusinessObject(@Parameter(description = "name of business object", required = true)
                                                 @PathParam("boName") String boName, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            // the below line of code will not only work for case correction but also work for a validation purpose.
            // If bo not found, we'll return from here.
            DynamicDTO dynamicDTO = dynamicReadService.readMetadataBusinessObject(boName, executionContext);
            DynamicDTO boHierarchy = dynamicReadService.getBoHierarchyForBusinessObject(dynamicDTO, executionContext);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "BO hierarchy generated successfully for " +
                    "business object '{}'", dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME)));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, new HashMap<>(boHierarchy),
                    executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    /*
    @GET
    @Path("/config/bo")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "Only retrieves all the business objects which have attributes metadata",
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
    public Response readAllBusinessObjects(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = getCurrentUserExecutionContext(httpHeaders,
                DSPDMConstants.DEFAULT_USERS.SYSTEM_USER);
        Response response = null;
        try {
            List<DynamicDTO> list = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
            // CREATE MAP TO ADD DATA TO RESPONE
            Map<String, Object> map = new HashMap<>(1);
            // ADD PAGED LIST TO DATA
            map.put(DSPDMConstants.BoName.BUSINESS_OBJECT, new PagedList<>(list.size(), list));
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
    */
    //controller: metadata.read===============================
    @GET
    @Path("/metadata/read/bo/list")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "Only retrieves all the business objects which have attributes metadata",
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
    public Response readAllBusinessObjects(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            List<DynamicDTO> list = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
            // CREATE MAP TO ADD DATA TO RESPONSE
            Map<String, Object> map = new HashMap<>(1);
            // ADD PAGED LIST TO DATA
            map.put(DSPDMConstants.BoName.BUSINESS_OBJECT, new PagedList<>(list.size(), list));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }
    //controller: upload.import===============================

    @POST
    @Path("/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    //@GZIP
    @Operation(
            summary = "Import data of given excel file to the data store",
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
    public Response uploadBinary(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;

        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EDIT);
            if (CollectionUtils.isNullOrEmpty(params)) {
                throw new DSPDMException("Invalid input for parse : Business object params not found", executionContext.getExecutorLocale());
            }
            if (fileInputStream == null) {
                throw new DSPDMException("No excel file found with param name '{}' and multi-part type '{}'", executionContext.getExecutorLocale(), "uploadedFile", "application/octet-stream");
            }

            String boName = params.keySet().iterator().next();
            Map<String, Object> bo = null;
            try {
                bo = (Map<String, Object>) params.get(boName);
            } catch (Exception e) {
                throw new DSPDMException("Invalid input for parse : Business object '{}' must be an object", executionContext.getExecutorLocale(), boName);
            }
            // initialize execution context
            initExecutionContextSettingsForImport(bo, executionContext);

            InputStream targetStream = fileInputStream;
            Map<String, BulkImportResponseDTO> parsedData = null;
            try {
                parsedData = ExcelImportJoinUtil.read(params, targetStream, false, dynamicReadService, executionContext);
            } finally {
                long freeMemoryNow = Runtime.getRuntime().freeMemory();
                // close stream immediately to avoid out of memory error
                // byte array input stream close has no effect
                targetStream.close();
                // release reference and make byte array it available for GC
                targetStream = null;
                // very important, it is used and we do not need it anymore
                // release reference and make byte array it available for GC
                // request garbage collector to perform gc work as per its convenient
                System.gc();
                logger.info("Total memory freed after cleaning file buffer : {} MB", (Runtime.getRuntime().freeMemory() - freeMemoryNow) / (1024 * 1024));
            }
            // transform the parsed data to the format acceptable to save api
            Map<String, List<DynamicDTO>> mapToSave = new HashMap<>();
            Map<String, String> convertResultMaps = new HashMap<String, String>();
            for (Map.Entry<String, BulkImportResponseDTO> entry : parsedData.entrySet()) {
                if (CollectionUtils.hasValue(entry.getValue().getParsedValues())) {
                    mapToSave.put(entry.getKey(), entry.getValue().getParsedValues());
                }
            }
            if (CollectionUtils.hasValue(mapToSave)) {
                DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
                executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.SIMPLE_IMPORT_OPR.getId());
                executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
                // Call save service
                SaveResultDTO saveResultDTO = dynamicWriteService.saveOrUpdate(mapToSave, executionContext);
                // Prepare response
                Map<String, Object> map = null;
                if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                    map = new HashMap<>(saveResultDTO.getDataFromReadBack());
                } else {
                    map = new HashMap<>(2);
                }
                List<DSPDMMessage> messages = new ArrayList<>();
                setOperationMessageByBoName(DSPDMConstants.API_Operation.IMPORT, messages, saveResultDTO);

                if ((saveResultDTO.getInsertedRecordsCount() == 0) && (saveResultDTO.getUpdatedRecordsCount() == 0)
                        && (saveResultDTO.getDeletedRecordsCount() == 0) && (saveResultDTO.getIgnoredRecordsCount() == 0)) {
                    throw new DSPDMException("No valid records found to import. Please user parse function to analyze the data.", executionContext.getExecutorLocale());
                }

                if (!(executionContext.isReadBack())) {
                    map.clear();
                }
                response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map,
                        executionContext).getResponse();
            } else {
                throw new DSPDMException("No valid records found to import. Please user parse function to analyze the data.", executionContext.getExecutorLocale());
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            // CREATE ERROR RESPONSE
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    //controller: upload.parse===============================
    @POST
    @Path("/parse")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    //@GZIP
    @Operation(
            summary = "Parses data of given excel file and the returns it in json format",
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
    public Response parseUploadBinary(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        Response response = null;
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EDIT);
            if (CollectionUtils.isNullOrEmpty(params)) {
                throw new DSPDMException("Invalid input for parse : Business object params not found", executionContext.getExecutorLocale());
            }
            if (fileInputStream == null) {
                throw new DSPDMException("No excel file found with param name '{}' and multi-part type '{}'", executionContext.getExecutorLocale(), "uploadedFile", "application/octet-stream");
            }

            String boName = params.keySet().iterator().next();
            Map<String, Object> bo = null;
            try {
                bo = (Map<String, Object>) params.get(boName);
            } catch (Exception e) {
                throw new DSPDMException("Invalid input for parse : Business object '{}' must be an object", executionContext.getExecutorLocale(), boName);
            }

            // initialize execution context settings from params
            initExecutionContextSettingsForImport(bo, executionContext);

            InputStream targetStream = fileInputStream;
            Map<String, BulkImportResponseDTO> map = null;
            try {
                map = ExcelImportJoinUtil.read(params, targetStream, false, dynamicReadService, executionContext);
            } finally {
                long freeMemoryNow = Runtime.getRuntime().freeMemory();
                // close stream immediately to avoid out of memory error
                // byte array input stream close has no effect
                targetStream.close();
                // release reference and make byte array it available for GC
                targetStream = null;
                // very important, it is used and we do not need it anymore
                // release reference and make byte array it available for GC

                // request garbage collector to perform gc work as per its convenient
                System.gc();
                logger.info("Total memory freed after cleaning file buffer : {} MB", (Runtime.getRuntime().freeMemory() - freeMemoryNow) / (1024 * 1024));
            }
            Map<String, Object> data = new HashMap<>(map);
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, data, executionContext).getResponse();
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
    @Path("/parse/boNames")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    //@GZIP
    @Operation(
            summary = "sends the names of the sheets along with the number of rows and returns them in a array if they matches the name of any business object.",
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
    public Response getBONamesSheetNames(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        Response response = null;
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EDIT);
            if (CollectionUtils.isNullOrEmpty(params)) {
                throw new DSPDMException("Invalid input for parse : Business object params not found", executionContext.getExecutorLocale());
            }
            if (fileInputStream == null) {
                throw new DSPDMException("No excel file found with param name '{}' and multi-part type '{}'", executionContext.getExecutorLocale(), "uploadedFile", "application/octet-stream");
            }
            // GET LOCALE/LANGUAGE from params and set into execution context
            String language = (String) params.get(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY);
            setLanguage(language, executionContext);

            // GET TIMEZONE from params and set into execution context
            String timezoneStr = (String) params.get(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY);
            setTimezone(timezoneStr, executionContext);

            // create a response reference to be returner
            InputStream targetStream = fileInputStream;
            Map<String, Integer> sheetNamesAndRecordsCount = null;
            try {
                sheetNamesAndRecordsCount = ExcelImportJoinUtil.getSheetNamesAndRecordsCount(targetStream, executionContext);
            } finally {
                long freeMemoryNow = Runtime.getRuntime().freeMemory();
                // close stream immediately to avoid out of memory error
                // byte array input stream close has no effect
                targetStream.close();
                // release reference and make byte array it available for GC
                targetStream = null;
                // very important, it is used and we do not need it anymore
                // release reference and make byte array it available for GC
                // request garbage collector to perform gc work as per its convenient
                System.gc();
                logger.info("Total memory freed after cleaning file buffer : {} MB", (Runtime.getRuntime().freeMemory() - freeMemoryNow) / (1024 * 1024));
            }

            // bo names list to be returned
            List<DynamicDTO> boNames = new ArrayList<>();
            // messages list
            List<DSPDMMessage> messages = new ArrayList<>();
            // read all business objects from data base
            List<DynamicDTO> metadataAllBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
            // iterate over sheet names in outer loop as we want to return the bo names in the excel sheets name definition order
            // first sheet should name come first in response if it is a bo name too
            for (Map.Entry<String, Integer> entry : sheetNamesAndRecordsCount.entrySet()) {
                boolean found = false;
                for (DynamicDTO businessObjectTDO : metadataAllBusinessObjects) {
                    String boName = ((String) businessObjectTDO.get(DSPDMConstants.BoAttrName.BO_NAME)).trim();
                    if (entry.getKey().equalsIgnoreCase(boName)) {
                        // bo name found as a sheet name
                        // clone and add total records and then add to list
                        DynamicDTO dynamicDTO = new DynamicDTO(businessObjectTDO, executionContext);
                        dynamicDTO.put(DSPDMConstants.DSPDM_RESPONSE.TOTAL_RECORDS_KEY, entry.getValue());
                        dynamicDTO.put(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY, new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} records to be processed in the next step", entry.getValue()));
                        boNames.add(dynamicDTO);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Sheet name '{}' has '{}' rows and it is not recognized as a business object", entry.getKey(), entry.getValue()));
                    DynamicDTO dynamicDTO = new DynamicDTO(entry.getKey(), null, executionContext);
                    // use putWithOrder do not use simple put.
                    // putWithOrder does not need metadata to convert the object to the json
                    // simple put need metadata to convert the object to the json
                    // simple put prints keys in metadata sequence number order when converting to json
                    dynamicDTO.putWithOrder(DSPDMConstants.BoAttrName.BO_NAME, entry.getKey());
                    dynamicDTO.putWithOrder(DSPDMConstants.DSPDM_RESPONSE.TOTAL_RECORDS_KEY, entry.getValue());
                    dynamicDTO.putWithOrder(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY, new DSPDMMessage(DSPDMConstants.Status.ERROR, "Sheet name '{}' has '{}' rows and it is not recognized as a business object", entry.getKey(), entry.getValue()));
                    boNames.add(dynamicDTO);
                }
            }
            // prepare data map to be returned
            Map<String, Object> map = new LinkedHashMap<>();
            for (DynamicDTO businessObject : boNames) {
                // ADD PAGED LIST TO DATA
                map.put((String) businessObject.get(DSPDMConstants.BoAttrName.BO_NAME), businessObject);
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
    @Path("/parse/bulk")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    //@GZIP
    @Operation(
            summary = "Parses bulk data of given excel file and the returns it in json format in a goup of duplicate, invalid and new",
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
    public Response bulkImport(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        Response response = null;
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.EDIT);
            if (CollectionUtils.isNullOrEmpty(params)) {
                throw new DSPDMException("Invalid input for parse : Business object params not found", executionContext.getExecutorLocale());
            }
            if (fileInputStream == null) {
                throw new DSPDMException("No excel file found with param name '{}' and multi-part type '{}'", executionContext.getExecutorLocale(), "uploadedFile", "application/octet-stream");
            }

            // Verify that all types of params values are Map
            StringBuilder boNameException = new StringBuilder();
            for (String boName : params.keySet()) {
                if (!(params.get(boName) instanceof Map)) {
                    boNameException.append(boName + ",");
                }
            }
            if (boNameException.length() > 0) {
                throw new DSPDMException("Invalid input for bulk import : Business object(s) '{}' must be of Map", executionContext.getExecutorLocale(), boNameException.substring(0, boNameException.length() - 1));
            }

            String boNameFirst = params.keySet().iterator().next();
            Map<String, Object> bo = null;
            try {
                bo = (Map<String, Object>) params.get(boNameFirst);
            } catch (Exception e) {
                throw new DSPDMException("Invalid input for parse : Business object '{}' must be an object", executionContext.getExecutorLocale(), boNameFirst);
            }
            initExecutionContextSettingsForImport(bo, executionContext);
            InputStream targetStream = fileInputStream;
            Map<String, BulkImportResponseDTO> map = null;
            try {
                map = ExcelImportJoinUtil.read(params, targetStream, true, dynamicReadService, executionContext);
            } finally {
                long freeMemoryNow = Runtime.getRuntime().freeMemory();
                // close stream immediately to avoid out of memory error
                // byte array input stream close has no effect
                targetStream.close();
                // release reference and make byte array it available for GC
                targetStream = null;
                // very important, it is used and we do not need it anymore
                // release reference and make byte array it available for GC
                // request garbage collector to perform gc work as per its convenient
                System.gc();
                logger.info("Total memory freed after cleaning file buffer : {} MB", (Runtime.getRuntime().freeMemory() - freeMemoryNow) / (1024 * 1024));
            }

            Map<String, Object> data = new HashMap<>(map);
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, data,
                    executionContext).getResponse();
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
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get list of dynamicDTO resources",
            responses = {
                    @ApiResponse(responseCode = "200",
                            description = "Success",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = DSPDMResponse.class))),
                    @ApiResponse(responseCode = "401", description = "not authorized!"),
                    @ApiResponse(responseCode = "403", description = "forbidden!!!"),
                    @ApiResponse(responseCode = "404", description = "not found!!!")
            })
    public Response search(@Parameter(description = "query entity", required = true)
                                   BOQuery boQuery, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.VIEW);
            BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(boQuery, executionContext);
            // validate search request
            SearchHelper.validateCriteriaFiltersValues(businessObjectInfo, executionContext);
            Map<String, DynamicDTO> metadataMap =
                    dynamicReadService.readMetadataMapForBOName(businessObjectInfo.getBusinessObjectType(),
                            executionContext);
            DTOHelper.convertDataTypeForFilters(businessObjectInfo, metadataMap, "read", executionContext,
                    executionContext.getExecutorName(), dynamicReadService);
            businessObjectInfo.setReadRecordsCount(true);
            final PagedList<DynamicDTO> searchResults = dynamicReadService.search(businessObjectInfo, executionContext);
            // FETCH LIST FROM DATA STORE
            Map<String, Object> map = new HashMap<>(2);
            map.put(boQuery.getBoName(), searchResults);
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, null, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @DELETE
    @Path("/search/indexAll/{boName}")
    @Produces("application/json")
    // @GZIP
    @Operation(summary = "delete all the data of bo_search table from search db for the given bo name", responses = {
            @ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = DSPDMResponse.class))),
            @ApiResponse(responseCode = "401", description = "not authorized!"),
            @ApiResponse(responseCode = "403", description = "forbidden!!!"),
            @ApiResponse(responseCode = "404", description = "not found!!!")})
    public Response clearBoSearchForBoName(@PathParam("boName") String boName, @Context HttpHeaders httpHeaders) {
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER,
                    DSPDMConstants.ApiPermission.SYSTEM_ADMIN);
            // Call save service
            SaveResultDTO saveResultDTO = dynamicWriteService.deleteAllSearchIndexesForBoName(boName, executionContext);

            List<DSPDMMessage> messages = new ArrayList<>(2);
            // show delete message if any deleted is updated
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} search indexes deleted successfully for business object '{}'.",
                        saveResultDTO.getDeletedRecordsCount(), boName));
            } else {
                if (saveResultDTO.isDeleteExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO,
                            "Delete operation executed but no search index was deleted. Please make sure some records exist."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            // Prepare response
            Map<String, Object> map = new HashMap<>(0);
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDeleteException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @DELETE
    @Path("/search/indexAll")
    @Produces("application/json")
    // @GZIP
    @Operation(summary = "delete all the data of bo_search table from search db", responses = {
            @ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = DSPDMResponse.class))),
            @ApiResponse(responseCode = "401", description = "not authorized!"),
            @ApiResponse(responseCode = "403", description = "forbidden!!!"),
            @ApiResponse(responseCode = "404", description = "not found!!!")})
    public Response clearBoSearch(@Context HttpHeaders httpHeaders) {
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER,
                    DSPDMConstants.ApiPermission.SYSTEM_ADMIN);
            // Call save service
            SaveResultDTO saveResultDTO = dynamicWriteService.deleteAllSearchIndexes(executionContext);

            List<DSPDMMessage> messages = new ArrayList<>(2);
            // show delete message if any deleted is updated
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} search indexes deleted successfully.",
                        saveResultDTO.getDeletedRecordsCount()));
            } else {
                if (saveResultDTO.isDeleteExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO,
                            "Delete operation executed but no search index was deleted. Please make sure some records exist."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            // Prepare response
            Map<String, Object> map = new HashMap<>(0);
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDeleteException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/search/indexAll/{boName}")
    @Produces("application/json")
    // @GZIP
    @Operation(summary = "refill all the data of bo_search table in search db for the given bo name", responses = {
            @ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = DSPDMResponse.class))),
            @ApiResponse(responseCode = "401", description = "not authorized!"),
            @ApiResponse(responseCode = "403", description = "forbidden!!!"),
            @ApiResponse(responseCode = "404", description = "not found!!!")})
    public Response indexAllSearchForBoName(@PathParam("boName") String boName, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER,
                    DSPDMConstants.ApiPermission.SYSTEM_ADMIN);

            executionContext.setIndexAll(true);
            SaveResultDTO saveResultDTO = dynamicWriteService.createSearchIndexForBoName(boName, executionContext);
            List<DSPDMMessage> messages = new ArrayList<>(2);
            // show delete message if any deleted is updated
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} record(s) deleted from {} successfully for business object '{}'.",
                        saveResultDTO.getDeletedRecordsCount(), DSPDMConstants.BoName.BO_SEARCH, boName));
            }
            if (saveResultDTO.isAnyRecordInserted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} record(s) inserted in {} successfully for business object '{}'.",
                        saveResultDTO.getInsertedRecordsCount(), DSPDMConstants.BoName.BO_SEARCH, boName));
                responseStatus = DSPDMConstants.Status.SUCCESS;
            }
            if (!saveResultDTO.isAnyRecordDeleted() && !saveResultDTO.isAnyRecordInserted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO,
                        "Delete {} operation executed but no record was deleted. Please make sure some records exist.",
                        DSPDMConstants.BoName.BO_SEARCH));
                responseStatus = DSPDMConstants.Status.INFO;
            }
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, null, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDeleteException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/search/indexAll")
    @Produces("application/json")
    // @GZIP
    @Operation(summary = "refill all the data of bo_search table in search db", responses = {
            @ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = DSPDMResponse.class))),
            @ApiResponse(responseCode = "401", description = "not authorized!"),
            @ApiResponse(responseCode = "403", description = "forbidden!!!"),
            @ApiResponse(responseCode = "404", description = "not found!!!")})
    public Response indexAllSearch(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (dynamicWriteService == null) {
            dynamicWriteService = DynamicWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER,
                    DSPDMConstants.ApiPermission.SYSTEM_ADMIN);

            executionContext.setIndexAll(true);
            SaveResultDTO saveResultDTO = dynamicWriteService.createSearchIndexForAllBusinessObjects(executionContext);
            List<DSPDMMessage> messages = new ArrayList<>(2);
            // show delete message if any deleted is updated
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (saveResultDTO.isAnyRecordDeleted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} record(s) deleted from {} successfully.",
                        saveResultDTO.getDeletedRecordsCount(), DSPDMConstants.BoName.BO_SEARCH));
            }
            if (saveResultDTO.isAnyRecordInserted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} record(s) inserted in {} successfully.",
                        saveResultDTO.getInsertedRecordsCount(), DSPDMConstants.BoName.BO_SEARCH));
                responseStatus = DSPDMConstants.Status.SUCCESS;
            }
            if (!saveResultDTO.isAnyRecordDeleted() && !saveResultDTO.isAnyRecordInserted()) {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO,
                        "Delete {} operation executed but no record was deleted. Please make sure some records exist.",
                        DSPDMConstants.BoName.BO_SEARCH));
                responseStatus = DSPDMConstants.Status.INFO;
            }
            // prepare response
            response = new DSPDMResponse(responseStatus, messages, null, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDeleteException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @GET
    @Path("/config/refreshAuthorizationCacheForUser/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Reloads the auth information and refreshes its cache for the given user id",
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
    public Response refreshAuthorizationCacheForUser(@Context HttpHeaders httpHeaders, @PathParam("userId") String userId) {
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.SYSTEM_ADMIN);

            List<DSPDMMessage> messages = new ArrayList<>();
            if (ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
                AuthTokenUtils.refreshUserAuthorizationCache(userId, executionContext);
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "'{}' updated in auth cache successfully!.", userId));
            } else {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Security not enabled on the current instance of this service."));
            }
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, null, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @GET
    @Path("/config/clearAuthorizationCache")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Reloads the auth information and refreshes its cache",
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
    public Response clearAuthorizationCache(@Context HttpHeaders httpHeaders) {
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.OPEN);
            AuthTokenUtils.clearAuthorizationCache();
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Authorization cache cleared successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, null, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @DELETE
    @Path("/config/deleteUserAuthorizationCache/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Removes the user auth cache",
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
    public Response deleteUserAuthorizationCache(@Context HttpHeaders httpHeaders, @PathParam("userId") String userId) {
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.SYSTEM_ADMIN);

            List<DSPDMMessage> messages = new ArrayList<>();
            if (ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
                AuthTokenUtils.removeUserFromAuthorizationCache(userId, executionContext);
                messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "'{}' removed from auth cache successfully.", userId));
            } else {
                messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Security not enabled on the current instance of this service."));
            }
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, null, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @GET
    @Path("/supportedDataTypes")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = " Reveal All the Supported Data Types Details",
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
    public Response revealSupportedDataTypes(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            List<Map<String, Object>> supportDataTypes = DSPDMConstants.DataTypes.getSupportedDataTypes(ConnectionProperties.isMSSQLServerDialect());
            // CREATE MAP TO ADD DATA TO RESPONSE
            Map<String, Object> map = new HashMap<>(1);
            // ADD PAGED LIST TO DATA
            map.put(DSPDMConstants.DSPDM_RESPONSE.SUPPORTED_DATATPES_KEY, supportDataTypes);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Reveal all the supported Data Types details successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    private void initExecutionContextSettingsForImport(Map<String, Object> bo, ExecutionContext executionContext) {
        // GET LOCALE/LANGUAGE from params and set into execution context
        String language = (String) bo.get(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY);
        setLanguage(language, executionContext);

        // GET TIMEZONE from params and set into execution context
        String timezoneStr = (String) bo.get(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY);
        setTimezone(timezoneStr, executionContext);

        // SET SHOW SQL STATS And SCRIPT
        if (ConfigProperties.getInstance().allow_sql_stats_collection.getBooleanValue()) {
            // stats
            Object obj = bo.get(DSPDMConstants.DSPDM_REQUEST.SHOW_SQL_STATS_KEY);
            if (obj != null) {
                boolean showSQLStats = false;
                if (obj instanceof String) {
                    showSQLStats = Boolean.valueOf(((String) obj).trim());
                } else if (obj instanceof Boolean) {
                    showSQLStats = ((Boolean) obj).booleanValue();
                }
                executionContext.setCollectSQLStats(showSQLStats);
            }
            // script
            obj = bo.get(DSPDMConstants.DSPDM_REQUEST.COLLECT_SQL_SCRIPT_KEY);
            if (obj != null) {
                int collectSQLScript = 0;
                CollectSQLScriptOptions option = null;
                if (obj instanceof String) {
                    collectSQLScript = Integer.valueOf(((String) obj).trim());
                    option = CollectSQLScriptOptions.getByValue(collectSQLScript);
                } else if (obj instanceof Number) {
                    collectSQLScript = NumberUtils.convertToInteger(obj, executionContext);
                    option = CollectSQLScriptOptions.getByValue(collectSQLScript);
                }
                if (option != null) {
                    executionContext.setCollectSQLScriptOptions(option);
                } else {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST, "Invalid value '{}' given for param '{}'. {}",
                            executionContext.getExecutorLocale(), obj, DSPDMConstants.DSPDM_REQUEST.COLLECT_SQL_SCRIPT_KEY, CollectSQLScriptOptions.description);
                }
            }
        }
        // SET WRITE NULL VALUES
        Object writeNullValuesObj = bo.get(DSPDMConstants.DSPDM_REQUEST.WRITE_NULL_VALUES_KEY);
        if (writeNullValuesObj != null) {
            boolean writeNullValues = false;
            if (writeNullValuesObj instanceof String) {
                writeNullValues = Boolean.valueOf(((String) writeNullValuesObj).trim());
            } else if (writeNullValuesObj instanceof Boolean) {
                writeNullValues = ((Boolean) writeNullValuesObj).booleanValue();
            }
            executionContext.setWriteNullValues(writeNullValues);
        }

        // SET READ BACK VALUES
        Object readBackObj = bo.get(DSPDMConstants.DSPDM_REQUEST.READ_BACK_KEY);
        if (readBackObj != null) {
            boolean readBackFlag = false;
            if (readBackObj instanceof String) {
                readBackFlag = Boolean.valueOf(((String) readBackObj).trim());
            } else if (readBackObj instanceof Boolean) {
                readBackFlag = ((Boolean) readBackObj).booleanValue();
            }
            executionContext.setReadBack(readBackFlag);
        }
    }

    protected void setOperationMessageByBoName(String operation, List<DSPDMMessage> messages, SaveResultDTO saveResultDTO) {
        Iterator<Map.Entry<String, Integer>> iter = null;
        if (DSPDMConstants.API_Operation.SAVE.equals(operation) || DSPDMConstants.API_Operation.IMPORT.equals(operation)) {
            // show inserted records message if any record is inserted
            if (saveResultDTO.isAnyRecordInserted()) {
                iter = saveResultDTO.getInsertedRecordsCountByBoName().entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Integer> entry = iter.next();
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} record(s) inserted successfully.",
                            entry.getValue(), entry.getKey()));
                }
            }
            // show update message if any record is updated
            if (saveResultDTO.isAnyRecordUpdated()) {
                iter = saveResultDTO.getUpdatedRecordsCountByBoName().entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Integer> entry = iter.next();
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} record(s) updated successfully.",
                            entry.getValue(), entry.getKey()));
                }
            }
            // show ignored records message if any record is ignored
            if (saveResultDTO.isAnyRecordIgnored()) {
                iter = saveResultDTO.getIgnoredRecordsCountByBoName().entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Integer> entry = iter.next();
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} record(s) ignored from update due to no change detected in data.",
                            entry.getValue(), entry.getKey()));
                }
            }
        } else if (DSPDMConstants.API_Operation.DELETE.equals(operation)) {
            // show delete message if any deleted is updated
            if (saveResultDTO.isAnyRecordDeleted()) {
                iter = saveResultDTO.getDeletedRecordsCountByBoName().entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Integer> entry = iter.next();
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} record(s) deleted successfully.",
                            entry.getValue(), entry.getKey()));
                }
            }
            // show ignored records message if any record is ignored
            if (saveResultDTO.isAnyRecordIgnored()) {
                iter = saveResultDTO.getIgnoredRecordsCountByBoName().entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Integer> entry = iter.next();
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} {} record(s) ignored from delete because either they do not exist or already deleted.",
                            entry.getValue(), entry.getKey()));
                }
            }
        }
    }

    @POST
    @Path("/metadata/write/addCustomAttributes")
    @Consumes("application/json")
    @Produces("application/json")
    public Response addCustomAttributes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.CUSTOM_ATTRIBUTE);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToSaveOrUpdate =
                    DTOHelper.buildBONameAndBOListMapFromRequestJSON(formParams, "save/update", true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToSaveOrUpdate)) {
                throw new DSPDMException("No custom attribute found to be saved.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToSaveOrUpdate.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR))) {
                throw new DSPDMException("No custom attribute found to be saved.", executionContext.getExecutorLocale());
            }
            List<DynamicDTO> boListToSave = boNameAndBOListMapToSaveOrUpdate.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            //validate
            MetadataAttributeChangeRestServiceHelper.validateAddCustomAttributes(boListToSave, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call save service
            SaveResultDTO saveResultDTO = metadataWriteService.addCustomAttributes(boListToSave, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show inserted records message if any record is inserted
            if (saveResultDTO.getAddedDBColumnsCount() > 0) {
                if (saveResultDTO.getAddedDBColumnsCount() == 1) {
                    DynamicDTO attrDynamicDTO = boListToSave.get(0);
                    String attributeDisplayName = (String) attrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                    String boName = (String) attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Custom attribute '{}' added successfully to business object '{}'.",
                            attributeDisplayName, boName));
                } else {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} custom attributes added successfully.",
                            saveResultDTO.getAddedDBColumnsCount()));
                }
            }
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isInsertExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Insert executed but no custom attribute has been added" +
                            ". Please make sure that data is correct."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddCustomAttributeException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/updateCustomAttributes")
    @Consumes("application/json")
    @Produces("application/json")
    public Response updateCustomAttributes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.CUSTOM_ATTRIBUTE);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToSaveOrUpdate =
                    DTOHelper.buildBONameAndBOListMapFromRequestJSON(formParams, "save/update", true, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToSaveOrUpdate)) {
                throw new DSPDMException("No attribute found to be updated.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToSaveOrUpdate.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR))) {
                throw new DSPDMException("No attribute found to be updated.", executionContext.getExecutorLocale());
            }
            List<DynamicDTO> boListToSave = boNameAndBOListMapToSaveOrUpdate.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            //validate
            MetadataAttributeChangeRestServiceHelper.validateUpdateAttributes(boListToSave, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call save service
            SaveResultDTO saveResultDTO = metadataWriteService.updateCustomAttributes(boListToSave, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show inserted records message if any record is inserted
            if (saveResultDTO.getUpdatedRecordsCount() > 0) { // means metadata updated
                if (saveResultDTO.getUpdatedRecordsCount() == 1) {
                    DynamicDTO attrDynamicDTO = boListToSave.get(0);
                    String attributeDisplayName = (String) attrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                    if (StringUtils.isNullOrEmpty(attributeDisplayName)) {
                        attributeDisplayName = (String) attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    }
                    //now checking if column has been updated or not
                    if (saveResultDTO.getUpdatedDBColumnsCount() > 0) {
                        messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Attribute '{}' updated successfully "
                                + "with metadata records and '{}' physical columns updated"
                                , attributeDisplayName, saveResultDTO.getUpdatedDBColumnsCount()));
                    } else {
                        messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Attribute '{}' updated successfully "
                                + "with metadata records and no physical columns updated"
                                , attributeDisplayName));
                    }
                } else {
                    //now checking if column has been updated or not
                    if (saveResultDTO.getUpdatedDBColumnsCount() > 0) {
                        messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Attribute(s) updated successfully with '{}' metadata records updated and '{}' physical" +
                                " db column updated", saveResultDTO.getUpdatedRecordsCount(), saveResultDTO.getUpdatedDBColumnsCount()));
                    } else {
                        messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Attribute(s) updated successfully with '{}' metadata records updated with no physical" +
                                " db column updated", saveResultDTO.getUpdatedRecordsCount()));
                    }
                }
            }
            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isAnyRecordIgnored()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Update executed but no attribute has been updated" +
                            ". Please make sure that data is correct."));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleAddCustomAttributeException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @POST
    @Path("/metadata/write/deleteCustomAttributes")
    @Consumes("application/json")
    @Produces("application/json")
    public Response deleteCustomAttributes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        if (metadataWriteService == null) {
            metadataWriteService = MetadataWriteService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getUnknownUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders, DSPDMConstants.DEFAULT_USERS.OSDU_TEST_USER, DSPDMConstants.ApiPermission.CUSTOM_ATTRIBUTE);
            // prepare objects
            Map<String, List<DynamicDTO>> boNameAndBOListMapToDelete =
                    DTOHelper.buildBONameAndBOListMapFromRequestJSON(formParams, "delete", false, dynamicReadService, executionContext);
            if (CollectionUtils.isNullOrEmpty(boNameAndBOListMapToDelete)) {
                throw new DSPDMException("No custom attribute found to be deleted.", executionContext.getExecutorLocale());
            }
            if (!(boNameAndBOListMapToDelete.containsKey(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR))) {
                throw new DSPDMException("No custom attribute found to be deleted.", executionContext.getExecutorLocale());
            }
            List<DynamicDTO> boListToDelete = boNameAndBOListMapToDelete.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            //validate
            MetadataAttributeChangeRestServiceHelper.validateDeleteCustomAttributes(boListToDelete, dynamicReadService, executionContext);
            // creating dynamic dto and adding it to the executionContext for logging user performed operation.
            DynamicDTO userPerformOprDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.USER_PERFORMED_OPR, null, executionContext);
            executionContext.addUserOperationDTO(userPerformOprDynamicDTO);
            // Call save service
            SaveResultDTO saveResultDTO = metadataWriteService.deleteCustomAttributes(boListToDelete, executionContext);
            // Prepare response
            Map<String, Object> map = null;
            if (CollectionUtils.hasValue(saveResultDTO.getDataFromReadBack())) {
                map = new HashMap<>(saveResultDTO.getDataFromReadBack());
            } else {
                map = new HashMap<>(2);
            }
            List<DSPDMMessage> messages = new ArrayList<>(3);
            // show deleted records message if any record is deleted
            if (saveResultDTO.getDroppedDBColumnsCount() > 0) {
                if (saveResultDTO.getDroppedDBColumnsCount() == 1) {
                    DynamicDTO attrDynamicDTO = boListToDelete.get(0);
                    String attributeDisplayName = (String) attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    String boName = (String) attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Custom attribute '{}' deleted successfully from business object '{}'.",
                            attributeDisplayName, boName));
                } else {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "{} custom attributes deleted successfully.",
                            saveResultDTO.getDroppedDBColumnsCount()));
                }
            }

            DSPDMConstants.Status responseStatus = DSPDMConstants.Status.SUCCESS;
            if (messages.size() == 0) {
                if (saveResultDTO.isDeleteExecuted()) {
                    messages.add(new DSPDMMessage(DSPDMConstants.Status.INFO, "Delete custom attribute executed but no attribute has been deleted" +
                            ". Please make sure bo name and bo attr name values exist.", saveResultDTO.getIgnoredRecordsCount()));
                    responseStatus = DSPDMConstants.Status.INFO;
                }
            }
            response = new DSPDMResponse(responseStatus, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            response = handleDeleteCustomAttributeException(e, executionContext);
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    @GET
    @Path("/common/wafByPassRules")
    @Produces("application/json")
    @Operation(
            summary = " Reveal All the Azure WAF ByPass Rules",
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
    public Response revealWafByPassRules(@Context HttpHeaders httpHeaders) {
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            executionContext = getCurrentUserExecutionContext(httpHeaders,
                    DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, DSPDMConstants.ApiPermission.VIEW);
            Map<String, String> wafBypassRules = WafByPassRulesProperties.getInstance().getWafByPassRulesWhiteListMap();
            // CREATE MAP TO ADD DATA TO RESPONSE
            Map<String, Object> map = new HashMap<>(1);
            // ADD PAGED LIST TO DATA
            map.put(DSPDMConstants.DSPDM_RESPONSE.WAF_BYPASS_RULES, wafBypassRules);
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "Revealed all the implemented Azure WAF bypass rules successfully."));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, map, executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        // log time taken by whole request and the time taken by db
        executionContext.logTotalProcessingTime();
        return response;
    }

    // dspdmImpl.java

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

    @GET
    @Path("/")
    @Produces("application/json")
    //@GZIP
    @Operation(
            summary = "validate probe just for temporary",
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
    public Response validate() {
        ExecutionContext executionContext = ExecutionContext.getSystemUserExecutionContext();
        Response response = null;
        try {
            List<DSPDMMessage> messages = new ArrayList<>();
            messages.add(new DSPDMMessage(DSPDMConstants.Status.SUCCESS, "validate successuflly"));
            // CREATE SUCCESS RESPONSE
            response = new DSPDMResponse(DSPDMConstants.Status.SUCCESS, messages, new TreeMap<>(),
                    executionContext).getResponse();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            response = new DSPDMResponse(e, executionContext).getResponse();
        }
        return response;
    }

    /**
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
     */
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

    /**
     * converts select from String type to String List
     *
     * @param strSelect
     *            string of select
     * @return list of String
     *
     * @author Qinghua Ma
     * @since 16-Sep-2020
     */
    private List<String> parseSelect(String strSelect) {
        String[] selects = strSelect.split(Character.toString(DSPDMConstants.COMMA));
        List<String> strSelects = new ArrayList<String>();
        for (String select : selects) {
            strSelects.add(select.trim());
        }
        return strSelects;
    }

    /**
     * converts filter criteria from String type to CriterialFilter List
     *
     * @param strFilter
     *            string of filter
     * @return list of CriteriaFilter
     *
     * @author Qinghua Ma
     * @since 14-Sep-2020
     */
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

    /**
     * converts order by from String type to OrderBy List
     *
     * @param strOrderby string of order by
     * @return list of OrderBy
     * @author Qinghua Ma
     * @since 14-Sep-2020
     */
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

    /**
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
     */
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
    }
}
