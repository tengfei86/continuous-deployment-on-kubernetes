package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.util.CollectionUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class HttpFileUtil {

    public static final int cache = 10 * 1024;

    public static String upload(String url, String fileParam, List<File> uploadFileList, Map<String, String> paramMap, Map<String, Object> headers) {
        String responseEntityStr = "";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpPost httppost = new HttpPost(url);
            if (CollectionUtils.hasValue(headers)) {
                for (Map.Entry<String, Object> e : headers.entrySet()) {
                    httppost.setHeader(e.getKey(), String.valueOf(e.getValue()));
                }
            }
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(200000).setSocketTimeout(200000)
                    .build();
            httppost.setConfig(requestConfig);
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
            for (String paramName : paramMap.keySet()) {
                String paramValue = paramMap.get(paramName);
                StringBody valueBody = new StringBody(paramValue, ContentType.APPLICATION_JSON);
                entityBuilder.addPart(paramName, valueBody);
            }

            for (File file : uploadFileList) {
                FileBody bin = new FileBody(file);
                entityBuilder.addPart(fileParam, bin);
            }

            HttpEntity reqEntity = entityBuilder.build();
            httppost.setEntity(reqEntity);
            CloseableHttpResponse response = httpclient.execute(httppost);
            try {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    responseEntityStr = EntityUtils.toString(response.getEntity());
                }
                EntityUtils.consume(resEntity);
            } finally {
                response.close();
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpclient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return responseEntityStr;
    }

    public static File download(String url, String fileFolder, String fileName) {
        File file = null;
        FileOutputStream fileout = null;
        InputStream is = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {

            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);

            is = response.getEntity().getContent();

            String filePath = fileFolder
                    + (fileName != null && !fileName.equals("") ? fileName : getFileName(response));

            file = new File(filePath);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            fileout = new FileOutputStream(file);
            byte[] buffer = new byte[cache];
            int ch = 0;
            while ((ch = is.read(buffer)) != -1) {
                fileout.write(buffer, 0, ch);
            }
            fileout.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	try {
        		if(fileout!=null) {
        			fileout.close();
        		}
        		if(is!=null) {
        			is.close();
        		}
        	}catch(IOException e) {
        		e.printStackTrace();
        	}
        }
        return file;
    }

    public static String getFileName(HttpResponse response) {
        Header contentHeader = response.getFirstHeader("Content-Disposition");
        String filename = null;
        if (contentHeader != null) {
            HeaderElement[] values = contentHeader.getElements();
            if (values.length == 1) {
                NameValuePair param = values[0].getParameterByName("filename");
                if (param != null) {
                    try {
                        // filename = new
                        // String(param.getValue().toString().getBytes(),
                        // "utf-8");
                        // filename=URLDecoder.decode(param.getValue(),"utf-8");
                        filename = param.getValue();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return filename;
    }
}
