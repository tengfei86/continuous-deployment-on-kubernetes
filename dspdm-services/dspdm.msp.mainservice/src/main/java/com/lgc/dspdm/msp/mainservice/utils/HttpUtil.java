package com.lgc.dspdm.msp.mainservice.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import jakarta.ws.rs.core.Response;

import com.lgc.dspdm.core.common.util.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext; 

/**
 * @author rao.alikhan
 * @since 10-07-2020
 */
public class HttpUtil {

    /**
     * This will send post request to the required url
     *
     * @param url              URL is required to send request
     * @param json             Request body in json string
     * @param executionContext Execution Context
     * @return Will return response body in json string
     * @author rao.alikhan
     * @since 10-07-2020
     */
    public static String sendHttpPost(String url, String json, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("Content-type", "application/json");
            request.setEntity(new StringEntity(json));
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != Response.Status.OK.getStatusCode()) {
                throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return response;
    }

    public static String sendHttpPost(String url, List<NameValuePair> params, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(url); 
			
            request.setHeader("Content-type", "application/x-www-form-urlencoded");
            request.setEntity(new UrlEncodedFormEntity(params,StandardCharsets.UTF_8));
            
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != Response.Status.OK.getStatusCode()) {
                throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return response;
    }
    /**
     * This will perform Http GET method using the given url
     *
     * @param url              URL is required to execute request
     * @param headers          Header to attach in the request
     * @param executionContext Execution Context
     * @return Will return response of the request
     * @author rao.alikhan
     * @since 14-07-2020
     */
    public static String sendHttpGET(String url, Map<String, String> headers, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            if (CollectionUtils.hasValue(headers)) {
                for (Map.Entry<String, String> e : headers.entrySet()) {
                    request.setHeader(e.getKey(), e.getValue());
                }
            }
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != Response.Status.OK.getStatusCode()) {
                if(StringUtils.isNullOrEmpty(response)) {
                    response = httpResponse.getStatusLine().getReasonPhrase();
                }
                if (statusCode == Response.Status.UNAUTHORIZED.getStatusCode()) {
                    throw new DSPDMException(statusCode, "401 Http UnAuthorized", null, executionContext.getExecutorLocale());
                } else {
                    throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
                }
            }
        } catch (Throwable ex) {
            DSPDMException.throwException(ex, executionContext);
        }
        return response;
    }

    /**
     * This will transform http response in json string
     *
     * @param httpResponse     Http response is required
     * @param executionContext Execution context
     * @return Will return response json string
     * @author rao.alikhan
     * @since 14-07-2020
     */
    private static String processResponseBody(HttpResponse httpResponse, ExecutionContext executionContext) {
        StringBuilder response = new StringBuilder();
        try (InputStream is = httpResponse.getEntity().getContent();
             InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
             BufferedReader br = new BufferedReader(isr);
        ) {
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine);
            }
        } catch (Exception ex) {
            DSPDMException.throwException(ex, executionContext);
        }
        return response.toString();
    }
}
