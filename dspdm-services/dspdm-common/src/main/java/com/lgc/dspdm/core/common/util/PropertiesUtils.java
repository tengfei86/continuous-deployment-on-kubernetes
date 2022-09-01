package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * @author Muhammad Imran Ansari
 */
public class PropertiesUtils {
    private static DSPDMLogger logger = new DSPDMLogger(PropertiesUtils.class);

    public static OrderedProperties loadProperties(InputStream inputStream, ExecutionContext executionContext) {
        OrderedProperties properties = null;
        try {
            properties = new OrderedProperties();
            properties.load(inputStream);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return properties;
    }

    public static void printAllProperties(OrderedProperties properties) {
        if (properties != null) {
            for (Object key : properties.keySet()) {
                logger.info("{} : {}", key, properties.getProperty((String) key));
            }
        }
    }
    
    public static String cleanString(String aString) {
        if (aString == null) return null;
        String cleanString = "";
        for (int i = 0; i < aString.length(); ++i) {
            cleanString += cleanChar(aString.charAt(i));
        }
        return cleanString;
    }

    private static char cleanChar(char aChar) {

        // 0 - 9
        for (int i = 48; i < 58; ++i) {
            if (aChar == i) return (char) i;
        }

        // 'A' - 'Z'
        for (int i = 65; i < 91; ++i) {
            if (aChar == i) return (char) i;
        }

        // 'a' - 'z'
        for (int i = 97; i < 123; ++i) {
            if (aChar == i) return (char) i;
        }

        // other valid characters
        switch (aChar) {
            case '/':
                return '/';
            case '.':
                return '.';
            case '-':
                return '-';
            case '_':
                return '_';
            case ' ':
                return ' ';
            case '\\':
                return '\\';
            case ':':
                return ':';
        }
        return '%';
    }
    
    public static File getConfigFileFromExternalLocation(String fileName) throws FileNotFoundException {
        File file = null;
        try {
            logger.info("Going to read system property for name : '{}'", DSPDMConstants.SystemPropertyNames.PATH_TO_CONFIG_ROOT_DIR);
            String mainDirPath = System.getProperty(DSPDMConstants.SystemPropertyNames.PATH_TO_CONFIG_ROOT_DIR);
            if ((mainDirPath != null) && (mainDirPath.trim().length() > 0)) {
                logger.info("System Property '{}' found sucessfully with value : '{}'", DSPDMConstants.SystemPropertyNames.PATH_TO_CONFIG_ROOT_DIR, mainDirPath);
                mainDirPath = mainDirPath.trim();
                // verify external dir path does not contain a parent directory link and does not contain a symbolic link
                if (mainDirPath.contains("..")) {
                    // throw error do not throw exception
                    throw new Error("Initialization failed. External config files path contains /../ and relative paths are not allowed");
//                } else if (Files.isSymbolicLink(Paths.get(mainDirPath))) {
//                    throw new Error("Initialization failed. External config files path contains /../ and relative paths are not allowed");
                }

                String connectionFilePath = null;
                if (mainDirPath.endsWith("/")) {
                    connectionFilePath = mainDirPath + "config" + System.getProperty("file.separator") + fileName;
                } else if (mainDirPath.endsWith("\\")) {
                    connectionFilePath = mainDirPath + "config" + System.getProperty("file.separator") + fileName;
                } else {
                    connectionFilePath = mainDirPath + System.getProperty("file.separator") + "config" + System.getProperty("file.separator") + fileName;
                }
                // clean path and escape invalid characters
                connectionFilePath = cleanString(connectionFilePath);
                logger.info("Going to read property file located at absolute path : {}", connectionFilePath);
                file = new File(connectionFilePath);
                 
                if (!(file.exists())) {
                    logger.info("Property file does not exists at path : {}", connectionFilePath);
                    file = null;
                } else if (!(file.canRead())) {
                    logger.info("Property file exists at path but unable to read : {}", connectionFilePath);
                    file = null;
                } else if (!(file.isFile())) {
                    logger.info("Given path does not point to a file : {}", connectionFilePath);
                    file = null;
                } else {
                    logger.info("Property file located at absolute path found successfully : {}", connectionFilePath);
                }
            }
        } catch (Exception e) {
            // eat exception and do not propagate just log and return null
            logger.error(e);
            file = null;
        }
        return file;
    }

    public static InputStream getConfigFileInputStreamFromClassPath(String fileName) throws FileNotFoundException {
        InputStream inputStream = null;
        try {
            logger.info("Now going to read property file from class path with file name : {}", fileName);
            URL resource = ConnectionProperties.class.getClassLoader().getResource(fileName);
            if (resource != null) {
                logger.info("Now going to read property file located at absolute path: {}", resource.getPath());
                inputStream = ConnectionProperties.class.getClassLoader().getResourceAsStream(fileName);
            }
        } catch (Exception e) {
            // eat exception and do not propagate just log and return null
            logger.error(e);
        }
        return inputStream;
    }
}
