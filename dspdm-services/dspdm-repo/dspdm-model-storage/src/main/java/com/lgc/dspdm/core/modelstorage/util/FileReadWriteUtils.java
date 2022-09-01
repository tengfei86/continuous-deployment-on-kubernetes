package com.lgc.dspdm.core.modelstorage.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileReadWriteUtils {

    public static void main(String[] args) {

        String inputFile = null;
        String outputFile = null;
        copyFilesUsingNewFileByteChannel(inputFile, outputFile);
    }


    public static void copyFilesUsingByteBuffer(String inputFile, String outputFile) {
        // takes 5 to 6 seconds for 2GB file copy
        long startTime = System.currentTimeMillis();
        final int BUFFER_SIZE = 4096; // 4KB
        try (
                InputStream inputStream = new FileInputStream(inputFile);
                OutputStream outputStream = new FileOutputStream(outputFile);
        ) {

            byte[] buffer = new byte[BUFFER_SIZE];

            int readLength = -1;
            while ((readLength = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, readLength);
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Total Time Taken : " + (System.currentTimeMillis() - startTime) + " millis");
    }

    public static void copyFilesUsingBufferedStreams(String inputFile, String outputFile) {
        // takes 4.5 to 5.5 seconds for 2GB file copy
        long startTime = System.currentTimeMillis();
        final int BUFFER_SIZE = 4096; // 4KB

        try (
                InputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile), BUFFER_SIZE);
                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile), BUFFER_SIZE);
        ) {

            byte[] buffer = new byte[BUFFER_SIZE];

            while (inputStream.read(buffer) != -1) {
                outputStream.write(buffer);
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Total Time Taken : " + (System.currentTimeMillis() - startTime) + " millis");
    }

    public static void copyFilesUsingNewFileIO(String inputFile, String outputFile) {
        // takes 10 seconds for 2GB file copy
        long startTime = System.currentTimeMillis();
        final int BUFFER_SIZE = 4096; // 4KB

        try {
            long start = System.currentTimeMillis();

            byte[] allBytes = Files.readAllBytes(Paths.get(inputFile));
            Files.write(Paths.get(outputFile), allBytes);

            long end = System.currentTimeMillis();
            System.out.println("Copied in " + (end - start) + " ms");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Total Time Taken : " + (System.currentTimeMillis() - startTime) + " millis");
    }

    public static void copyFileUsingChannels(String sourceFile, String destFile) {
        // takes 3.5 to 4.5 seconds for 2GB file copy
        long startTime = System.currentTimeMillis();
        try (
                FileChannel sourceChannel = new FileInputStream(new File(sourceFile)).getChannel();
                FileChannel destChannel = new FileOutputStream(new File(destFile)).getChannel();
        ) {

            sourceChannel.transferTo(0, sourceChannel.size(), destChannel);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Total Time Taken : " + (System.currentTimeMillis() - startTime) + " millis");
    }

    public static void copyFilesUsingNewFileByteChannel(String inputFile, String outputFile) {
        // takes 10 seconds for 2GB file copy
        long startTime = System.currentTimeMillis();
        final int BUFFER_SIZE = 819200; // 4KB

        try (
                ReadableByteChannel rbc = Files.newByteChannel(Paths.get(inputFile), StandardOpenOption.READ);
                WritableByteChannel wbc = Files.newByteChannel(Paths.get(outputFile), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        ) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            while(rbc.read(byteBuffer) > 0) {
                byteBuffer.clear();
                wbc.write(byteBuffer);
                byteBuffer.clear();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Total Time Taken : " + (System.currentTimeMillis() - startTime) + " millis");
    }
}
