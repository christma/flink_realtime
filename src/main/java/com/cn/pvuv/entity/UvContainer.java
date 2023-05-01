package com.cn.pvuv.entity;

import java.util.HashSet;
import java.util.Set;

/**
 * uv去重的数据存储对象
 */
public class UvContainer {

    private Set<String> ids = new HashSet<>();

    public Set<String> getIds() {
        return ids;
    }

    public void setIds(Set<String> ids) {
        this.ids = ids;
    }
}
