package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement
@Schema(name="OrderBy", description="order option")
public class OrderBy implements Serializable {

    @JsonProperty("boAttrName")
    @Schema(name="boAttrName",description="field to be sorted")
    String boAttrName;

    @JsonProperty("order")
    @Schema(name="order",description="field order, asc or desc")
    Order order;

    public OrderBy(){}

    public String getBoAttrName() {
        return boAttrName;
    }

    public Order getOrder() {
        return order;
    }

    public void setBoAttrName(String boAttrName) {
        this.boAttrName = boAttrName;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public OrderBy(String boAttrName, Order order) {
        this.boAttrName = boAttrName;
        this.order = order;
    }
}