package com.lgc.dspdm.core.common.logging;

import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.data.common.DSPDMMessage;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.PropertiesUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class DSPDMLogger {
    private Logger logger = null;

    public DSPDMLogger(Class c) {
        logger = LogManager.getLogger(c);
    }

    // trace
    public void trace(Throwable t) {
        log(Level.TRACE, t.getMessage(), t);
    }

    public void trace(String message, Throwable t) {
        log(Level.TRACE, message, t);
    }

    public void trace(String message, Object... params) {
        log(Level.TRACE, message, null, params);
    }

    // debug
    public void debug(Throwable t) {
        log(Level.DEBUG, t.getMessage(), t);
    }

    public void debug(String message, Throwable t) {
        log(Level.DEBUG, message, t);
    }

    public void debug(String message, Object... params) {
        log(Level.DEBUG, message, null, params);
    }

    // info
    public void info(Throwable t) {
        log(Level.INFO, t.getMessage(), t);
    }

    public void info(String message, Throwable t) {
        log(Level.INFO, message, t);
    }

    public void info(String message, Object... params) {
        log(Level.INFO, message, null, params);
    }

    // warn
    public void warn(Throwable t) {
        log(Level.WARN, t.getMessage(), t);
    }

    public void warn(String message, Throwable t) {
        log(Level.WARN, message, t);
    }

    public void warn(String message, Object... params) {
        log(Level.WARN, message, null, params);
    }

    // error
    public void error(Throwable t) {
        log(Level.ERROR, t.getMessage(), t);
    }

    public void error(String message, Throwable t) {
        log(Level.ERROR, message, t);
    }

    public void error(String message, Object... params) {
        log(Level.ERROR, message, null, params);
    }

    // fatal
    public void fatal(Throwable t) {
        log(Level.FATAL, t.getMessage(), t);
    }

    public void fatal(String message, Throwable t) {
        log(Level.FATAL, message, t);
    }

    public void fatal(String message, Object... params) {
        log(Level.FATAL, message, null, params);
    }

    public void log(DSPDMMessage dspdmMessage) {
        if (DSPDMConstants.Status.ERROR == dspdmMessage.getStatus()) {
            log(Level.ERROR, dspdmMessage.getMessage(), null);
        } else {
            log(Level.INFO, dspdmMessage.getMessage(), null);
        }
    }

    /**
     * The only wrapper to be invoked. All log4j2 logging calls will pass through here
     *
     * @param level
     * @param message
     * @param t
     * @param params
     */
    private void log(Level level, String message, Throwable t, Object... params) {
        if (logger.isEnabled(level)) {
            if (t != null) {
                logger.log(level, message, t);
            } else {
                logger.log(level, message, params);
            }
        }
    }

    public boolean isErrorEnabled() {
        return logger.isEnabled(Level.ERROR);
    }

    public boolean isWarnEnabled() {
        return logger.isEnabled(Level.WARN);
    }

    public boolean isInfoEnabled() {
        return logger.isEnabled(Level.INFO);
    }

    public boolean isDebugEnabled() {
        return logger.isEnabled(Level.DEBUG);
    }

    public boolean isTraceEnabled() {
        return logger.isEnabled(Level.TRACE);
    }

    public static void setRootLoggerLevelToDebug() {
        // You can set the logger:
        // org.apache.logging.log4j.core.config.Configurator.setLevel("com.lgc.dspdm", Level.DEBUG);

        // You can also set the root logger:
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(Level.DEBUG);
    }

    public static void setRootLoggerLevelToInfo() {
        // You can set the logger:
        // org.apache.logging.log4j.core.config.Configurator.setLevel("com.lgc.dspdm", Level.DEBUG);

        // You can also set the root logger:
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(Level.INFO);
    }

    public static void setRootLoggerLevelToWarn() {
        // You can set the logger:
        // org.apache.logging.log4j.core.config.Configurator.setLevel("com.lgc.dspdm", Level.DEBUG);

        // You can also set the root logger:
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(Level.WARN);
    }

    public static void setRootLoggerLevelToError() {
        // You can set the logger:
        // org.apache.logging.log4j.core.config.Configurator.setLevel("com.lgc.dspdm", Level.DEBUG);

        // You can also set the root logger:
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(Level.ERROR);
    }

    static {
        initFromFile();
    }

    public static void init(PropertySource propertySource, String log4j2FileContents) {
        switch (propertySource) {
            case MAP_PROVIDED:
                try {
                    initFromStringContents(log4j2FileContents);
                } catch (Exception e) {
                    initFromFile();
                }
                break;
            case FILE:
                initFromFile();
                break;
        }
    }

    public static void initFromFile() {
        try {
            File file = PropertiesUtils.getConfigFileFromExternalLocation(DSPDMConstants.ConfigFileNames.LOG4J2_FILE_NAME);
            if (file != null) {
                // org.apache.logging.log4j.core.LoggerContext;
                LoggerContext context = (LoggerContext) LogManager.getContext(false);
                // this will force a reconfiguration
                context.setConfigLocation(file.toURI());
                System.out.println("===============================================");
                System.out.println("Logger configured successfully from an external file located at path : " + file.getPath());
                System.out.println("===============================================");
            }
        } catch (Exception e) {
            // eat exception because logger will be configured from file in class path
            System.out.println("===============================================");
            System.out.println("Unable to configure logger from an external path");
            System.out.println("===============================================");
            e.printStackTrace();
        }
    }

    public static void initFromStringContents(String log4j2FileContents) {
        try {
            File file = writeContentsToTempFile(log4j2FileContents.getBytes(DSPDMConstants.UTF_8));
            if (file != null) {
                // org.apache.logging.log4j.core.LoggerContext;
                LoggerContext context = (LoggerContext) LogManager.getContext(false);
                // this will force a reconfiguration
                context.setConfigLocation(file.toURI());
                System.out.println("===============================================");
                System.out.println("Logger configured successfully from config map inline string content, temp file located at path : " + file.getPath());
                System.out.println("===============================================");
            }
        } catch (Exception e) {
            // eat exception because logger will be configured from file in class path
            System.out.println("===============================================");
            System.out.println("Unable to configure logger from an external path");
            System.out.println("===============================================");
            e.printStackTrace();
        }
    }

    private static File writeContentsToTempFile(byte[] content) throws IOException {
        File tempFile = File.createTempFile("tmp_" + System.currentTimeMillis(), ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(content);
        }
        return tempFile;
    }
}
