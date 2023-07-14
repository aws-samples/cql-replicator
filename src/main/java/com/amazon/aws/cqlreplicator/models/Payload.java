/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.models;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.Map;

public class Payload {
    private String pk;
    private String cl;
    private long ts;
    private String payload;
    private Map<String, String> cls;

    public Payload() {
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public void setClusteringColumn(String cl) {
        this.cl = cl;
    }

    public long getTimestamp() {
        return ts;
    }

    public void setTimestamp(long ts) {
        this.ts = ts;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Map<String, String> getClusteringColumns() {
        return cls;
    }

    public void setClusteringColumns(Map<String, String> cls) {
        this.cls = cls;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
