package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

@Path("secure")
@PermitAll
public class dspdmRestApi extends dspdmImpl {

	@GET
	@Path("/metadata")
	@Produces("application/json")
	@Operation(summary = "Get list of metadata business object", responses = {
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = DSPDMResponse.class))),
			@ApiResponse(responseCode = "401", description = "not authorized!"),
			@ApiResponse(responseCode = "403", description = "forbidden!!!"),
			@ApiResponse(responseCode = "404", description = "not found!!!") })
	public Response metadata(
			@Parameter(description = "language", required = true) @QueryParam("language") String language,
			@Parameter(description = "timezone", required = true) @QueryParam("timezone") Float timezoneOffset,
			@Parameter(description = "read_attributes", required = true) @QueryParam("read_attributes") Boolean readAttributes,
			@Parameter(description = "read_constraints", required = true) @QueryParam("read_constraints") Boolean readConstraints,
			@Parameter(description = "read_relationships", required = true) @QueryParam("read_relationships") Boolean readRelationships,
			@Parameter(description = "read_groups", required = true) @QueryParam("read_groups") Boolean readGroups,
			@Parameter(description = "select", required = true) @QueryParam("select") String select,
			@Parameter(description = "filter", required = true) @QueryParam("filter") String filter,
			@Parameter(description = "sort", required = true) @QueryParam("sort") String sort,
			@Parameter(description = "read_uniques", required = true) @QueryParam("read_uniques") Boolean readUnique,
			@Parameter(description = "read_first", required = true) @QueryParam("read_first") Boolean readFirst,
			@Parameter(description = "read_metadata", required = true) @QueryParam("read_metadata") Boolean readMetadata,
			@Parameter(description = "read_metadata_constraints", required = true) @QueryParam("read_metadata_constraints") Boolean readMetadataConstraints,
			@Parameter(description = "read_reference_data", required = true) @QueryParam("read_reference_data") Boolean readReferenceData,
			@Parameter(description = "read_reference_data_for_filters", required = true) @QueryParam("read_reference_data_for_filters") Boolean readReferenceDataForFilters,
			@Parameter(description = "read_reference_data_constraints", required = true) @QueryParam("read_reference_data_constraints") Boolean readReferenceDataConstraints,
			@Parameter(description = "read_records_count", required = true) @QueryParam("read_records_count") Boolean readRecordsCount,
			@Parameter(description = "read_with_distinct", required = true) @QueryParam("read_with_distinct") Boolean readWithDistinct,
			@Parameter(description = "read_all_records", required = true) @QueryParam("read_all_records") Boolean readAllRecords,
			@Parameter(description = "is_upload_needed", required = true) @QueryParam("is_upload_needed") Boolean isUploadNeeded,
			@Parameter(description = "page", required = true) @QueryParam("page") Integer page,
			@Parameter(description = "size", required = true) @QueryParam("size") Integer size,
			@Context HttpHeaders httpHeaders) {
		return businessObjects(language, timezoneOffset, DSPDMConstants.BoName.BUSINESS_OBJECT, null, readAttributes,
				readConstraints, readRelationships, readGroups, select, filter, sort, readUnique, readFirst,
				readMetadata, readMetadataConstraints, readReferenceData, readReferenceDataForFilters, readReferenceDataConstraints, readRecordsCount,
				readWithDistinct, readAllRecords, isUploadNeeded, page, size, httpHeaders);
	}

	@GET
	@Path("/metadata/{bo_name}")
	@Produces("application/json")
	@Operation(summary = "Get list of metadata business object by name", responses = {
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = DSPDMResponse.class))),
			@ApiResponse(responseCode = "401", description = "not authorized!"),
			@ApiResponse(responseCode = "403", description = "forbidden!!!"),
			@ApiResponse(responseCode = "404", description = "not found!!!") })
	public Response metadata(
			@Parameter(description = "name of business object", required = true) @PathParam("bo_name") String boName,
			@Parameter(description = "language", required = true) @QueryParam("language") String language,
			@Parameter(description = "timezone", required = true) @QueryParam("timezone") Float timezoneOffset,
			@Parameter(description = "read_attributes", required = true) @QueryParam("read_attributes") Boolean readAttributes,
			@Parameter(description = "read_constraints", required = true) @QueryParam("read_constraints") Boolean readConstraints,
			@Parameter(description = "read_relationships", required = true) @QueryParam("read_relationships") Boolean readRelationships,
			@Parameter(description = "read_groups", required = true) @QueryParam("read_groups") Boolean readGroups,
			@Parameter(description = "select", required = true) @QueryParam("select") String select,
			@Parameter(description = "filter", required = true) @QueryParam("filter") String filter,
			@Parameter(description = "sort", required = true) @QueryParam("sort") String sort,
			@Parameter(description = "read_uniques", required = true) @QueryParam("read_uniques") Boolean readUnique,
			@Parameter(description = "read_first", required = true) @QueryParam("read_first") Boolean readFirst,
			@Parameter(description = "read_metadata", required = true) @QueryParam("read_metadata") Boolean readMetadata,
			@Parameter(description = "read_metadata_constraints", required = true) @QueryParam("read_metadata_constraints") Boolean readMetadataConstraints,
			@Parameter(description = "read_reference_data", required = true) @QueryParam("read_reference_data") Boolean readReferenceData,
			@Parameter(description = "read_reference_data_for_filters", required = true) @QueryParam("read_reference_data_for_filters") Boolean readReferenceDataForFilters,
			@Parameter(description = "read_reference_data_constraints", required = true) @QueryParam("read_reference_data_constraints") Boolean readReferenceDataConstraints,
			@Parameter(description = "read_records_count", required = true) @QueryParam("read_records_count") Boolean readRecordsCount,
			@Parameter(description = "read_with_distinct", required = true) @QueryParam("read_with_distinct") Boolean readWithDistinct,
			@Parameter(description = "read_all_records", required = true) @QueryParam("read_all_records") Boolean readAllRecords,
			@Parameter(description = "is_upload_needed", required = true) @QueryParam("is_upload_needed") Boolean isUploadNeeded,
			@Parameter(description = "page", required = true) @QueryParam("page") Integer page,
			@Parameter(description = "size", required = true) @QueryParam("size") Integer size,
			@Context HttpHeaders httpHeaders) {
		return businessObjects(language, timezoneOffset, DSPDMConstants.BoName.BUSINESS_OBJECT, boName, readAttributes,
				readConstraints, readRelationships, readGroups, select, filter, sort, readUnique, readFirst,
				readMetadata, readMetadataConstraints, readReferenceData, readReferenceDataForFilters, readReferenceDataConstraints,
				readRecordsCount, readWithDistinct, readAllRecords, isUploadNeeded, page, size, httpHeaders);
	}
}
