package com.shenfen.yxw.hadoop.hdfs.wordcount;

import java.util.HashMap;
import java.util.Map;

public class CustomContext {

    private Map<Object, Object> cacheMap = new HashMap<Object, Object>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    public Object get(Object key) {
        return cacheMap.get(key);
    }
}
