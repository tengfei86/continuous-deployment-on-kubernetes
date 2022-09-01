package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * This util is for all Http call to the required server
 *
 * @author rao.alikhan
 */
public class HttpUtils {

    /**
     * Http POST method to send data to the server
     *
     * @param url  Server url to where the request should make connection
     * @param json Request body in json string form
     * @return Will return server response
     * @throws Exception
     */
    public static String sendPOST(String url, String json, ExecutionContext executionContext) {
        StringBuilder response = new StringBuilder();
        try {
            HttpURLConnection httpClient = (HttpURLConnection) new URL(url).openConnection();
            // add request header
            httpClient.setRequestMethod("POST");
            httpClient.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:25.0) Gecko/20100101 Firefox/25.0");
            httpClient.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
            httpClient.setRequestProperty("Content-Type", "application/json; utf-8");
            httpClient.setDoOutput(true);

            try (
                    OutputStream os = httpClient.getOutputStream()
            ) {
                byte[] input = json.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            httpClient.connect();
            try (
                    InputStream is = httpClient.getInputStream();
                    InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
                    BufferedReader br = new BufferedReader(isr);
            ) {
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine);
                }
            }
            if (httpClient.getResponseCode() != 200) {
                throw new DSPDMException("Failed : HTTP error code : {}, Error message : {}", executionContext.getExecutorLocale(), httpClient.getResponseCode(), response.toString());
            }
            httpClient.disconnect();
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return response.toString();
    }
}
