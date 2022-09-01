package com.lgc.dspdm.config.utils;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author rao.alikhan
 * @since 10-07-2020
 */
public class HttpUtils {
    private static DSPDMLogger logger = new DSPDMLogger(HttpUtils.class);

    static final Integer DEFAULT_SOCKET_TIMEOUT_SECOUNDS = 30;
    static final Integer DEFAULT_CONNECT_TIMEOUT_SECOUNDS = 30;
    static final Integer DEFAULT_REQUEST_TIMEOUT_SECOUNDS = 60;

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
    public static String sendHttpPost(String url, String json, String token, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("Content-type", "application/json");
            if (StringUtils.hasValue(token)) {
                request.setHeader("Authorization", token);
            }
            request.setEntity(new StringEntity(json));
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_SECOUNDS * 1000)
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SECOUNDS * 1000)
                    .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT_SECOUNDS * 1000);
            request.setConfig(requestConfigBuilder.build());
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != DSPDMConstants.HTTP_STATUS_CODES.OK) {
                throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
            }
        } catch (Exception e) {
            logger.error("URL : {}", url);
            DSPDMException.throwException(e, executionContext);
        }
        return response;
    }

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
    public static String sendHttpPut(String url, String json, String token, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPut request = new HttpPut(url);
            request.setHeader("Content-type", "application/json");
            if (StringUtils.hasValue(token)) {
                request.setHeader("Authorization", token);
            }
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_SECOUNDS * 1000)
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SECOUNDS * 1000)
                    .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT_SECOUNDS * 1000);
            request.setConfig(requestConfigBuilder.build());
            request.setEntity(new StringEntity(json));
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != DSPDMConstants.HTTP_STATUS_CODES.OK) {
                throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
            }
        } catch (Exception e) {
            logger.error("URL : {}", url);
            DSPDMException.throwException(e, executionContext);
        }
        return response;
    }

    /**
     * This will send post request to the required url
     *
     * @param url              URL is required to send request
     * @param executionContext Execution Context
     * @return Will return response body in json string
     * @author rao.alikhan
     * @since 10-07-2020
     */
    public static String sendHttpDelete(String url, String token, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpDelete request = new HttpDelete(url);
            request.setHeader("Content-type", "application/json");
            if (StringUtils.hasValue(token)) {
                request.setHeader("Authorization", token);
            }
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_SECOUNDS * 1000)
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SECOUNDS * 1000)
                    .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT_SECOUNDS * 1000);
            request.setConfig(requestConfigBuilder.build());
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != DSPDMConstants.HTTP_STATUS_CODES.OK) {
                throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
            }
        } catch (Exception e) {
            logger.error("URL : {}", url);
            DSPDMException.throwException(e, executionContext);
        }
        return response;
    }

    public static String sendHttpPost(String url, List<NameValuePair> params, String token, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("Content-type", "application/x-www-form-urlencoded");
            if (StringUtils.hasValue(token)) {
                request.setHeader("Authorization", token);
            }
            request.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_SECOUNDS * 1000)
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SECOUNDS * 1000)
                    .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT_SECOUNDS * 1000);
            request.setConfig(requestConfigBuilder.build());
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != DSPDMConstants.HTTP_STATUS_CODES.OK) {
                throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
            }
        } catch (Exception e) {
            logger.error("URL : {}", url);
            DSPDMException.throwException(e, executionContext);
        }
        return response;
    }

    /**
     * This will perform Http GET method using the given url
     *
     * @param url              URL is required to execute request
     * @param token            Header to attach in the request
     * @param executionContext Execution Context
     * @return Will return response of the request
     * @author rao.alikhan
     * @since 14-07-2020
     */
    public static String sendHttpGET(String url, String token, ExecutionContext executionContext) {
        String response = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            if (StringUtils.hasValue(token)) {
                request.setHeader("Authorization", token);
            }
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_SECOUNDS * 1000)
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SECOUNDS * 1000)
                    .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT_SECOUNDS * 1000);
            request.setConfig(requestConfigBuilder.build());
            HttpResponse httpResponse = client.execute(request);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            response = processResponseBody(httpResponse, executionContext);
            if (statusCode != DSPDMConstants.HTTP_STATUS_CODES.OK) {
                if (StringUtils.isNullOrEmpty(response)) {
                    response = httpResponse.getStatusLine().getReasonPhrase();
                }
                if (statusCode == DSPDMConstants.HTTP_STATUS_CODES.UN_AUTH) {
                    throw new DSPDMException(statusCode, "401 Http UnAuthorized", null, executionContext.getExecutorLocale());
                } else {
                    throw new DSPDMException(statusCode, response, null, executionContext.getExecutorLocale());
                }
            }
        } catch (Throwable ex) {
            logger.error("URL : {}", url);
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
