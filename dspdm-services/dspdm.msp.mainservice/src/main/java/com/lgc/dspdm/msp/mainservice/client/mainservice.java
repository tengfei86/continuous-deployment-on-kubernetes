package com.lgc.dspdm.msp.mainservice.client;


import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import io.swagger.v3.oas.annotations.Parameter;
import org.glassfish.jersey.media.multipart.FormDataParam;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.util.List;
import java.util.Map;


/**
 * @author service Developer
 * @documentationExample An example simple microservices implementations. To support resource auto scanning,
 * resource annotations (such as @path) need to be presented in this implementation; annotations
 * in the superclass/interface are ignored.
 */
@Path("/secure")
@PermitAll
//@ServiceProxy
public interface mainservice {

    //controller: read===============================
    @GET
    @Path("/common/{boName}")
    @Produces("application/json")
    Response getCustomResponseGet(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName, @Context HttpHeaders httpHeaders);

    @GET
    @Path("/common/count/{boName}")
    @Produces("application/json")
    Response count(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName,
            @Parameter(description = "list of business object body", required = true)
                    List<DynamicDTO> list,
            @Context HttpHeaders httpHeaders);

    @POST
    @Path("/common")
    @Consumes("application/json")
    @Produces("application/json")
    Response getCustomResponsePost(
            @Parameter(description = "query entity", required = true)
                    BOQuery entity, @Context HttpHeaders httpHeaders);

    @POST
    @Path("/common/count")
    @Consumes("application/json")
    @Produces("application/json")
    Response count(
            @Parameter(description = "query entity", required = true)
                    BOQuery entity, @Context HttpHeaders httpHeaders);

    //controller: write===============================
    @POST
    @Path("/save")
    @Consumes("application/json")
    @Produces("application/json")
    Response saveOrUpdate(Map<String, Object> formParams, @Context HttpHeaders httpHeaders);

    //controller: delete===============================
    @DELETE
    @Path("/delete/{boName}/{id}")
    @Produces("application/json")
    Response removeById(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName,
            @Parameter(description = "id of business object", required = true) @PathParam("id") Long id,
            @Context HttpHeaders httpHeaders);

    @POST
    @Path("/delete")
    @Consumes("application/json")
    @Produces("application/json")
    Response removeByJson(Map<String, Object> formParams, @Context HttpHeaders httpHeaders);

    //controller: download===============================
    @GET
    @Path("/export/{boName}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    Response export(
            @Parameter(description = "name of business object", required = true)
            @PathParam("boName") String boName, @Context HttpHeaders httpHeaders);

    @POST
    @Path("/export")
    @Consumes("application/json")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    Response export(
            @Parameter(description = "query entity", required = true)
            Object entitysObject, @Context HttpHeaders httpHeaders);

    //controller: metadata.config===============================
    @GET
    @Path("/config/refreshMetadata")
    @Produces("application/json")
    Response refreshMetadata(@Context HttpHeaders httpHeaders);

    @GET
    @Path("/config/validateMetadata")
    @Produces("application/json")
    Response validateMetadata(@Context HttpHeaders httpHeaders);

    @GET
    @Path("/config/validateAndRefreshMetadata")
    @Produces("application/json")
    Response validateAndRefreshMetadata(@Context HttpHeaders httpHeaders);

    //controller: metadata.read===============================
    @GET
    @Path("/metadata/read/bo/list")
    @Produces("application/json")
    Response readAllBusinessObjects(@Context HttpHeaders httpHeaders);

    //controller: metadata.write===============================
    @POST
    @Path("/metadata/write/addCustomAttributes")
    @Consumes("application/json")
    @Produces("application/json")
    Response addCustomAttributes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders);

    @POST
    @Path("/metadata/write/deleteCustomAttributes")
    @Consumes("application/json")
    @Produces("application/json")
    Response deleteCustomAttributes(Map<String, Object> formParams, @Context HttpHeaders httpHeaders);

    //controller: upload.import===============================
    @POST
    @Path("/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    Response uploadBinary(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params);

    //controller: upload.parse===============================
    @POST
    @Path("/parse")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    Response parseUploadBinary(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params);

    @POST
    @Path("/parse/boNames")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    Response getBONamesSheetNames(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params);

    @POST
    @Path("/parse/bulk")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    Response bulkImport(@Context HttpHeaders httpHeaders, @FormDataParam("uploadedFile") InputStream fileInputStream
            , @FormDataParam("params") Map<String, Object> params);


    @POST
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Response search(@Parameter(description = "query entity", required = true)
                            BOQuery boQuery, @Context HttpHeaders httpHeaders);

    @GET
    @Path("/config/refreshAuthorizationCacheForUser/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    Response refreshAuthorizationCacheForUser(@Context HttpHeaders httpHeaders, @PathParam("userId") String userId);

    @GET
    @Path("/config/clearAuthorizationCache")
    @Produces(MediaType.APPLICATION_JSON)
    Response clearAuthorizationCache(@Context HttpHeaders httpHeaders);

    @DELETE
    @Path("/config/deleteUserAuthorizationCache/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    Response deleteUserAuthorizationCache(@Context HttpHeaders httpHeaders, @PathParam("userId") String userId);

}
