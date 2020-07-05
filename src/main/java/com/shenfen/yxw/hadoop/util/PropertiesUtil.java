package com.shenfen.yxw.hadoop.util;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }


    public static void main(String[] args) {
        getProperties().getProperty("");
    }
}
