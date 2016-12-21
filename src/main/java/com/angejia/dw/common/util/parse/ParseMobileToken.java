package com.angejia.dw.common.util.parse;

import java.net.URLDecoder;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 解密 token
 */
public class ParseMobileToken {

    public static String evaluate(String s, String index) throws Exception {
        if (s == null || s.length() <= 0) {
            return "";
        }

        String token = Decrypt(s);

        if (token != null && token.length() > 0) {// json decode
            Map<String, Map<String, Object>> maps;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                try {
                    maps = objectMapper.readValue(token, Map.class);
                    if (maps.containsKey(index)) {
                        return String.valueOf(maps.get(index));
                    }
                } catch (JsonParseException e) {
                    maps = objectMapper.readValue(URLDecoder.decode(token, "utf-8"), Map.class);
                    if (maps.containsKey(index)) {
                        return String.valueOf(maps.get(index));
                    }
                }
            } catch (Exception e) {
                System.err.println(e.toString());
                return "";
            }
        }

        return "";
    }

    public static String Decrypt(String data) throws Exception {
        try {
            String key = "12345678123456xx";
            String iv = "12345678123456xx";

            byte[] encrypted1 = new Base64().decode(data);

            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
            SecretKeySpec keyspec = new SecretKeySpec(key.getBytes(), "AES");
            IvParameterSpec ivspec = new IvParameterSpec(iv.getBytes());

            cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
            try {
                byte[] original = cipher.doFinal(encrypted1);
                String originalString = new String(original);
                return originalString;
            } catch (Exception e) {
                System.err.println(e.toString());
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
