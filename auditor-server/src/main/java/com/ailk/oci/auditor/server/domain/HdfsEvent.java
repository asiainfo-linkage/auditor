package com.ailk.oci.auditor.server.domain;

import javax.persistence.*;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-4
 * Time: 下午5:58
 * To change this template use File | Settings | File Templates.
 */
@Entity
@Table(name = "HDFS_EVENT")
public class HdfsEvent extends Event {
    @Column(name = "EVENT_SRC")
    private String src;
    @Column(name = "EVENT_DST")
    private String dst;
    @Column(name = "EVENT_OWNER")
    private String owner;
    @Column(name = "EVENT_GROUP")
    private String group;
    @Column(name = "EVENT_PERMISSIONS")
    private String permissions;


    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }
}
