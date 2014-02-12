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
@Table(name = "HBASE_EVENT")
public class HBaseEvent extends Event {
    @Column(name = "EVENT_TABLE")
    private String table;
    @Column(name = "EVENT_FAMILY")
    private String family;
    @Column(name = "EVENT_QUALIFIER")
    private String qualifier;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }
}
