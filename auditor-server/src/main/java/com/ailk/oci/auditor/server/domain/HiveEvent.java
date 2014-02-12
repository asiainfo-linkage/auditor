package com.ailk.oci.auditor.server.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-4
 * Time: 下午5:58
 * To change this template use File | Settings | File Templates.
 */
@Entity
@Table(name = "HIVE_EVENT")
public class HiveEvent extends Event {
    @Column(name = "EVENT_QUERY")
    private String query;
    @Column(name = "EVENT_DATABASE")
    private String database;
    @Column(name = "EVENT_TABLE")
    private String table;
    @Column(name = "EVENT_LOCATION")
    private String location;
    @Column(name = "EVENT_TARGETTYPE")
    private String targetType;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }
}
