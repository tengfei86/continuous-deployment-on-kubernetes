package com.lgc.dspdm.core.common.util;


/**
 * @author: muqingyang
 * @Email: Qingyang.MU@halliburton.com
 * @date: 04/21/2020 9:23:49 AM
 * @version: V1.0
 * @Description: TODO
 * @History:
 */


import com.lgc.dspdm.core.common.exception.DSPDMException;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Locale;

/**
 * @author: muqingyang
 * @Email: Qingyang.MU@halliburton.com
 * @date: 04/21/2020 9:23:49 AM
 * @Description: TODO
 */

public class AESSecurityUtils {
    private static final String ALGORITHM = "AES";
    private static final String CipherMode = "AES/CBC/PKCS5Padding";
    private static final Integer IVSize = 16;
    private static final String Encode = "UTF-8";
    private static final int SecretKeySize = 32;
    private static final String AES_KEY_FILENAME = "SecurityKey";
    private static String AES_KEY = "";

    static {
        AES_KEY = "1681fe53f572df0052956d098f5585d8";//ConfigReader.readAsString(AES_KEY_FILENAME);
    }

    public static String encode(String content) {
        try {
            return encode(content, AES_KEY);
        } catch (Exception e) {
            DSPDMException.throwException(e, Locale.getDefault());
        }
        return null;
    }

    public static String decode(String encodeString) {
        try {
            return decode(encodeString, AES_KEY);
        } catch (Exception e) {
            DSPDMException.throwException(e, Locale.getDefault());
        }
        return null;
    }

    public static String encode(String content, String key) throws Exception {
        byte[] cipherBytes = encrypt(content.getBytes("utf-8"), key.getBytes("utf-8"));
        String AES_encode = Base64.getEncoder().encodeToString(cipherBytes);
        return AES_encode;
    }

    public static String decode(String encodeString, String key) throws Exception {
        byte[] cipherBytes = Base64.getDecoder().decode(encodeString);
        byte[] plainBytes = decrypt(cipherBytes, key.getBytes("utf-8"));
        return new String(plainBytes);
    }

    public static byte[] encrypt(byte[] plainBytes, byte[] key) throws Exception {
        //Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        SecretKeySpec secKey = generateKey(key);
        IvParameterSpec iv = createIV(key);
        Cipher cipher = Cipher.getInstance(CipherMode);
        cipher.init(Cipher.ENCRYPT_MODE, secKey, iv);
        byte[] cipherBytes = cipher.doFinal(plainBytes);
        return cipherBytes;
    }

    public static byte[] decrypt(byte[] cipherBytes, byte[] key) throws Exception {
        SecretKeySpec secKey = generateKey(key);
        IvParameterSpec iv = createIV(key);
        Cipher cipher = Cipher.getInstance(CipherMode);
        cipher.init(Cipher.DECRYPT_MODE, secKey, iv);
        byte[] plainBytes = cipher.doFinal(cipherBytes);
        return plainBytes;
    }

    private static IvParameterSpec createIV(byte[] key) {
        // Mike: The below code segment is useless
//        StringBuffer sb = new StringBuffer(IVSize);
//        sb.append(new String(key));
//        if (sb.length() > IVSize) {
//            sb.setLength(IVSize);
//        }
//
//        if (sb.length() < IVSize) {
//            while (sb.length() < IVSize) {
//                sb.append("0");
//            }
//        }

        byte[] data = null;
        try {
            String iv = "9f59wi41ufY8NxF5";
            data = iv.getBytes(Encode);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new IvParameterSpec(data);
    }


    private static SecretKeySpec generateKey(byte[] key) {
        StringBuilder sb = new StringBuilder(SecretKeySize);
        sb.append(new String(key));
        if (sb.length() > SecretKeySize) {
            sb.setLength(SecretKeySize);
        }

        if (sb.length() < SecretKeySize) {
            while (sb.length() < SecretKeySize) {
                sb.append(" ");
            }
        }

        try {
            byte[] data = sb.toString().getBytes(Encode);
            return new SecretKeySpec(data, ALGORITHM);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        String targetText = null;
        try {
            String encryptOrDecrypt = args[0];
            targetText = args[1];
            String result = null;
            if (encryptOrDecrypt.equalsIgnoreCase("ENCRYPT")) {
                result = encode(targetText);
            } else {
                result = decode(targetText);
            }
            System.out.println(result);
        } catch (Exception e) {
            System.out.println("Help usage: java -jar dspdm-common-1.0-SNAPSHOT.jar encrypt " + ((targetText != null) ? targetText : "buffalo"));
            System.out.println("Help usage: java -jar dspdm-common-1.0-SNAPSHOT.jar decrypt " + ((targetText != null) ? targetText : "buffalo"));
            e.printStackTrace();
        }
    }
}