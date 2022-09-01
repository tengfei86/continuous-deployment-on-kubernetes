package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.join.DynamicJoinClause;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@XmlRootElement
@Schema(name = "BOQuery", description = "query entity for BO")
public class BOQuery implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("boName")
    @Schema(name = "boName", description = "WELL", required = true)
    private String boName;

    @JsonProperty("language")
    @Schema(name = "language", description = "browser language for localization")
    private String language;

    @JsonProperty("timezone")
    @Schema(name = "timezone", description = "browser timezone")
    private String timezone;
    
    @JsonProperty("timezoneOffset")
    @Schema(name = "timezoneOffset", description = "browser timezone offset")
    private Float timezoneOffset;

    @JsonProperty("joinAlias")
    @Schema(name = "joinAlias", description = "Alias to be used in joins with other business objects")
    private String joinAlias;

    @JsonProperty("unitPolicy")
    @Schema(name = "unitPolicy", description = "unit conversion policy")
    private String unitPolicy;
    
	@JsonProperty("dspdmUnits")
    @Schema(name = "dspdmUnits", description = "unit conversion dspdmUnits")
    private List<DSPDMUnit> dspdmUnits;

    @JsonProperty("selectList")
    @Schema(name = "selectList", description = "fields to show")
    private List<String> selectList;

    @JsonProperty("aggregateSelectList")
    @Schema(name = "aggregateSelectList", description = "aggregate fields to show")
    private List<AggregateColumn> aggregateSelectList;

    @JsonProperty("criteriaFilters")
    @Schema(name = "criteriaFilters", description = "where condition")
    private List<CriteriaFilter> criteriaFilters;
    
    @JsonProperty("filterGroups")
    @Schema(name = "filterGroups", description = "where condition groups")
    private List<FilterGroup> filterGroups;

    @JsonProperty("havingFilters")
    @Schema(name = "havingFilters", description = "having condition")
    private List<HavingFilter> havingFilters;

    @JsonProperty("hav_ingFilters")
    @Schema(name = "hav_ingFilters", description = "having condition with Azure WAF rules bypassing")
    private List<HavingFilter> hav_ingFilters;

    @JsonProperty("orderBy")
    @Schema(name = "orderBy", description = "result order")
    private List<OrderBy> orderBy;

    @JsonProperty("pagination")
    @Schema(name = "pagination", description = "pagination option")
    private Pagination pagination;

    @JsonProperty("simpleJoins")
    @Schema(name = "simpleJoins", description = "Array of Comma separated existing tables join sql information objects")
    private List<SimpleJoinClause> simpleJoins;

    @JsonProperty("dynamicJoins")
    @Schema(name = "dynamicJoins", description = "Array of Comma separated dynamic tables join sql information objects")
    private List<DynamicJoinClause> dynamicJoins;

    @JsonProperty("readParentBO")
    @Schema(name = "readParentBO", description = "Comma separated names of business objects of foreign keys to be read along with the main object")
    private List<String> readParentBO;

    @JsonProperty("readChildBO")
    @Schema(name = "readChildBO", description = "List of BOQuery to fetch children data ")
    private List<BOQuery> readChildBO;

    @JsonProperty("readBack")
    @Schema(name = "readBack", description = "read back the updated/inserted records ")
    private Boolean readBack;

    @JsonProperty("readUnique")
    @Schema(name = "readUnique", description = "database has more than one record then service will throw error")
    private Boolean readUnique;

    @JsonProperty("readFirst")
    @Schema(name = "readFirst", description = "return first result only")
    private Boolean readFirst;

    @JsonProperty("readMetadata")
    @Schema(name = "readMetadata", description = "Read metadata for the given bo name")
    private Boolean readMetadata;

    @JsonProperty("readMetadataConstraints")
    @Schema(name = "readMetadataConstraints", description = "Read metadata unique constraints for the given bo name")
    private Boolean readMetadataConstraints;

    @JsonProperty("readReferenceData")
    @Schema(name = "readReferenceData", description = "Read all the reference data for the given bo name")
    private Boolean readReferenceData;

    @JsonProperty("readReferenceDataForFilters")
    @Schema(name = "readReferenceDataForFilters", description = "Read only those reference data values which are used in the current table")
    private Boolean readReferenceDataForFilters;

    @JsonProperty("readReferenceDataConstraints")
    @Schema(name = "readReferenceDataConstraints", description = "Read all the reference data constraints for the given bo name")
    private Boolean readReferenceDataConstraints;

    @JsonProperty("readRecordsCount")
    @Schema(name = "readRecordsCount", description = "Read the count of records in database for the given bo name and provided filters. To be used in pagination")
    private Boolean readRecordsCount;

    @JsonProperty("readWithDistinct")
    @Schema(name = "readWithDistinct", description = "Optional and defaults to false. If set to true then distinct will be applied to the SQL SELECT LIST")
    private Boolean readWithDistinct;

    @JsonProperty("readAllRecords")
    @Schema(name = "readAllRecords", description = "Optional and defaults to false. If set to true then no limit on the records will be applied")
    private Boolean readAllRecords;

    @JsonProperty("isUploadNeeded")
    @Schema(name = "isUploadNeeded", description = "Optional and defaults to false. If set to true then only uploadAttributes required")
    private Boolean isUploadNeeded;

    @JsonProperty("prependAlias")
    @Schema(name = "prependAlias", description = "Optional and defaults to false. If set to true then all the attributes will be prepended with their respective business object alias")
    private Boolean prependAlias;

    @JsonProperty("showSQLStats")
    @Schema(name = "showSQLStats", description = "Optional and defaults to false. If set to true then the response will include the SQL statements executed on the system and time taken by them.")
    private Boolean showSQLStats;

    @JsonProperty("collectSQLScript")
    @Schema(name = "collectSQLScript", description = "Optional and defaults to zero means no script collection")
    private Integer collectSQLScript = 0;

    @JsonProperty("writeNullValues")
    @Schema(name = "writeNullValues", description = "Optional and defaults to false. If set to true then the printed json response will include the properties having null values.")
    private Boolean writeNullValues;

    @JsonProperty("sheetName")
    @Schema(name = "sheetName", description = "Custom sheet name for export API")
    private String sheetName;

    public String getBoName() {
        return boName;
    }

    public void setBoName(String boName) {
        this.boName = boName;
    }

    public String getUnitPolicy() {
		return unitPolicy;
	}

	public void setUnitPolicy(String unitPolicy) {
		this.unitPolicy = unitPolicy;
	}

	public List<DSPDMUnit> getDSPDMUnits() {
		return dspdmUnits;
	}

	public void setDSPDMUnits(List<DSPDMUnit> dspdmUnits) {
		this.dspdmUnits = dspdmUnits;
	}

    public List<String> getSelectList() {
        return selectList;
    }

    public void setSelectList(List<String> selectList) {
        if (selectList == null) {
            this.selectList = null;
        } else {
            this.selectList = new ArrayList<>(selectList);
        }
    }

    public List<AggregateColumn> getAggregateSelectList() {
        return aggregateSelectList;
    }

    public void setAggregateSelectList(List<AggregateColumn> aggregateSelectList) {
        if (aggregateSelectList == null) {
            this.aggregateSelectList = null;
        } else {
            this.aggregateSelectList = new ArrayList<>(aggregateSelectList);
        }
    }

    public List<String> getReadParentBO() {
        return readParentBO;
    }

    public void setReadParentBO(List<String> readParentBO) {
        if (readParentBO == null) {
            this.readParentBO = null;
        } else {
            this.readParentBO = new ArrayList<>(readParentBO);
        }
    }

    public List<BOQuery> getReadChildBO() {
        return readChildBO;
    }

    public void setReadChildBO(List<BOQuery> readChildBO) {
        if (readChildBO == null) {
            this.readChildBO = null;
        } else {
            this.readChildBO = new ArrayList<>(readChildBO);
        }
    }

    public void addColumnsToSelect(String... columnsToSelect) {
        if (columnsToSelect != null) {
            if (this.selectList == null) {
                this.selectList = new ArrayList<>(Arrays.asList(columnsToSelect));
            } else {
                this.selectList.addAll(Arrays.asList(columnsToSelect));
            }
        }
    }

    public void addAggregateColumnsToSelect(AggregateColumn... aggregateColumnsToSelect) {
        if (aggregateColumnsToSelect != null) {
            if (this.aggregateSelectList == null) {
                this.aggregateSelectList = new ArrayList<>(Arrays.asList(aggregateColumnsToSelect));
            } else {
                this.aggregateSelectList.addAll(Arrays.asList(aggregateColumnsToSelect));
            }
        }
    }

    public List<CriteriaFilter> getCriteriaFilters() {
        return criteriaFilters;
    }

    public void setCriteriaFilters(List<CriteriaFilter> criteriaFilters) {
        this.criteriaFilters = criteriaFilters;
    }

    public List<FilterGroup> getFilterGroups() {
		return filterGroups;
	}

	public void setFilterGroups(List<FilterGroup> filterGroups) {
		this.filterGroups = filterGroups;
	}

	public List<HavingFilter> getHavingFilters() {
        return havingFilters;
    }

    public void setHavingFilters(List<HavingFilter> havingFilters) {
        this.havingFilters = havingFilters;
    }

    public List<OrderBy> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<OrderBy> orderBy) {
        this.orderBy = orderBy;
    }

    public List<SimpleJoinClause> getSimpleJoins() {
        return simpleJoins;
    }

    public void setSimpleJoins(List<SimpleJoinClause> simpleJoins) {
        this.simpleJoins = simpleJoins;
    }

    public List<DynamicJoinClause> getDynamicJoins() {
        return dynamicJoins;
    }

    public void setDynamicJoins(List<DynamicJoinClause> dynamicJoins) {
        this.dynamicJoins = dynamicJoins;
    }

    public Pagination getPagination() {
        return pagination;
    }

    public void setPagination(Pagination pagination) {
        this.pagination = pagination;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public Float getTimezoneOffset() {
        return timezoneOffset;
    }

    public void setTimezoneOffset(Float timezoneOffset) {
        this.timezoneOffset = timezoneOffset;
    }

    public String getJoinAlias() {
        return joinAlias;
    }

    public void setJoinAlias(String joinAlias) {
        this.joinAlias = joinAlias;
    }

    public Boolean getReadBack() {
        return readBack;
    }

    public void setReadBack(Boolean readBack) {
        this.readBack = readBack;
    }

    public Boolean getReadUnique() {
        return readUnique;
    }

    public void setReadUnique(Boolean readUnique) {
        this.readUnique = readUnique;
    }

    public Boolean getReadFirst() {
        return readFirst;
    }

    public void setReadFirst(Boolean readFirst) {
        this.readFirst = readFirst;
    }

    public Boolean getReadMetadata() {
        return readMetadata;
    }

    public void setReadMetadata(Boolean readMetadata) {
        this.readMetadata = readMetadata;
    }

    public Boolean getReadReferenceData() {
        return readReferenceData;
    }

    public void setReadReferenceData(Boolean readReferenceData) {
        this.readReferenceData = readReferenceData;
    }

    public Boolean getReadReferenceDataForFilters() {
        return readReferenceDataForFilters;
    }

    public void setReadReferenceDataForFilters(Boolean readReferenceDataForFilters) {
        this.readReferenceDataForFilters = readReferenceDataForFilters;
    }

    public Boolean getReadReferenceDataConstraints() {
        return readReferenceDataConstraints;
    }

    public void setReadReferenceDataConstraints(Boolean readReferenceDataConstraints) {
        this.readReferenceDataConstraints = readReferenceDataConstraints;
    }

    public Boolean getReadMetadataConstraints() {
        return readMetadataConstraints;
    }

    public void setReadMetadataConstraints(Boolean readMetadataConstraints) {
        this.readMetadataConstraints = readMetadataConstraints;
    }

    public Boolean getReadRecordsCount() {
        return readRecordsCount;
    }

    public void setReadRecordsCount(Boolean readRecordsCount) {
        this.readRecordsCount = readRecordsCount;
    }

    public Boolean getReadWithDistinct() {
        return readWithDistinct;
    }

    public void setReadWithDistinct(Boolean readWithDistinct) {
        this.readWithDistinct = readWithDistinct;
    }

    public Boolean getReadAllRecords() {
        return readAllRecords;
    }

    public void setReadAllRecords(Boolean readAllRecords) {
        this.readAllRecords = readAllRecords;
    }

    public Boolean getUploadNeeded() {
        return isUploadNeeded;
    }

    public void setUploadNeeded(Boolean uploadNeeded) {
        isUploadNeeded = uploadNeeded;
    }

    public Boolean getPrependAlias() {
        return prependAlias;
    }

    public void setPrependAlias(Boolean prependAlias) {
        this.prependAlias = prependAlias;
    }

    public Boolean getShowSQLStats() {
        return showSQLStats;
    }

    public void setShowSQLStats(Boolean showSQLStats) {
        this.showSQLStats = showSQLStats;
    }

    public Integer getCollectSQLScript() {
        return collectSQLScript;
    }

    public void setCollectSQLScript(Integer collectSQLScript) {
        this.collectSQLScript = collectSQLScript;
    }

    public Boolean getWriteNullValues() {
        return writeNullValues;
    }

    public void setWriteNullValues(Boolean writeNullValues) {
        this.writeNullValues = writeNullValues;
    }

    public String getSheetName() { return sheetName; }

    public void setSheetName(String sheetName) { this.sheetName = sheetName; }

    public List<HavingFilter> getHav_ingFilters() { return havingFilters; }

    public void setHav_ingFilters(List<HavingFilter> hav_ingFilters) { this.havingFilters = hav_ingFilters; }
}
