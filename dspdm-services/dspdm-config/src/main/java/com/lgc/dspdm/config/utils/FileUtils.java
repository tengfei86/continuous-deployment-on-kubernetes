/**
 * @author: muqingyang
 * @Email: Qingyang.MU@halliburton.com
 * @date: 02/20/2020 4:21:50 PM
 * @version: V1.0
 * @Description: TODO
 * @History:
 */
package com.lgc.dspdm.config.utils;

import java.io.File;
import java.io.FileInputStream;

/**
 * @author: muqingyang
 * @Email: Qingyang.MU@halliburton.com
 * @date: 02/20/2020 4:21:50 PM
 * @Description: TODO
 */
public class FileUtils {

    public static String readFileAsString(String filePath) throws Exception {
        String textString = "";
        if (new File(filePath).exists()) {
            try (FileInputStream in = new FileInputStream(filePath)) {
                byte[] fileContent = new byte[in.available()];
                in.read(fileContent);
                in.close();
                textString = new String(fileContent, "UTF-8");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new Exception("workflow log file not exists");
        }
        return textString;
    }
}
