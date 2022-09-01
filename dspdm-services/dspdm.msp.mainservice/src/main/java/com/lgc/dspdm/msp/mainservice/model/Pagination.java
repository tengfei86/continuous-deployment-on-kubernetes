package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@Schema(name="Pagination", description="Pagination option")
public class Pagination implements Serializable {

    @JsonProperty("recordsPerPage")
    @Schema(name="recordsPerPage",description="max results per page")
    private Integer recordsPerPage;

    @JsonProperty("pages")
    @Schema(name="pages",description="selected pages to show")
    List<Integer> pages = new ArrayList<Integer>();

    public Pagination(){}

    public Integer getRecordsPerPage() {
        return recordsPerPage;
    }

    public List<Integer> getPages() {
        return pages;
    }

    public void setRecordsPerPage(Integer recordsPerPage) {
        this.recordsPerPage = recordsPerPage;
    }

    public void setPages(List<Integer> pages) {
        this.pages = pages;
    }

    public Pagination(Integer recordsPerPage, List<Integer> pages) {
        this.recordsPerPage = recordsPerPage;
        this.pages = pages;
    }
}
