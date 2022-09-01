package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.lgc.dspdm.core.common.data.common.DSPDMMessage;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.msp.mainservice.utils.DSPDMResponseDeserializer;
import com.lgc.dspdm.msp.mainservice.utils.DSPDMResponseSerializer;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.ws.rs.core.Response;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Schema(name = "DSPDMResponse", description = "respond entity")
@JsonSerialize(using = DSPDMResponseSerializer.class)
@JsonDeserialize(using = DSPDMResponseDeserializer.class)
public class DSPDMResponse implements Serializable {
    @Schema(description = "status of request", name = "status")
    private DSPDMConstants.Status status = null;
    @Schema(description = "response messages of request", name = "messages")
    private List<DSPDMMessage> messages = null;
    @Schema(description = "payload of data", name = "data")
    private Map<String, Object> data;
    @Schema(description = "stacktrace", name = "exception")
    private Throwable exception = null;
    @Schema(description = "Execution Context info like user name, user locale, user timezone ", name = "executionContext")
    private ExecutionContext executionContext = null;

    private transient Integer httpStatusCode = null;

    public DSPDMResponse() {
        messages = new ArrayList<>();
        this.httpStatusCode = Response.Status.OK.getStatusCode();
    }

    public DSPDMResponse(Throwable exception, ExecutionContext executionContext) {
        this();
        this.exception = exception;
        this.status = DSPDMConstants.Status.ERROR;
        this.messages.add(new DSPDMMessage(this.status, exception.getMessage()));
        this.executionContext = executionContext;
        this.httpStatusCode = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
        if (exception instanceof DSPDMException) {
            DSPDMException dspdmException = (DSPDMException) exception;
            if (dspdmException.getHttpStatusCode() != null) {
                this.httpStatusCode = dspdmException.getHttpStatusCode();
            }
        }
    }

    public DSPDMResponse(DSPDMConstants.Status status, List<DSPDMMessage> messages, ExecutionContext executionContext) {
        this.status = status;
        this.messages = messages;
        this.executionContext = executionContext;
        this.httpStatusCode = Response.Status.OK.getStatusCode();
    }

    public DSPDMResponse(DSPDMConstants.Status status, List<DSPDMMessage> messages, Map<String, Object> data, ExecutionContext executionContext) {
        this.status = status;
        this.messages = messages;
        this.data = data;
        this.executionContext = executionContext;
        this.httpStatusCode = Response.Status.OK.getStatusCode();
    }

    public DSPDMConstants.Status getStatus() {
        return status;
    }

    public List<DSPDMMessage> getMessages() {
        return messages;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Throwable getException() {
        return exception;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public Response getResponse() {
        return Response.status(this.httpStatusCode).entity(this).build();
    }
}
