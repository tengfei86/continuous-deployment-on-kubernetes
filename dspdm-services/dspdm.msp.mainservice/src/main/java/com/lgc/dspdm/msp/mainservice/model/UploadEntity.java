package com.lgc.dspdm.msp.mainservice.model;

//import org.jboss.resteasy.annotations.providers.multipart.PartType;

import javax.ws.rs.FormParam;
import java.util.Map;

public class UploadEntity {

    public UploadEntity() {
    }

    private Map<String,Object> params;

    private byte[] data;

    public byte[] getData() {
        return data;
    }
    @FormParam("uploadedFile")
    //@PartType("application/octet-stream") //TODO
    public void setData(byte[] data) {
        this.data = data;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @FormParam("params")
    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
