package com.dgtz.api.boot.beans;


import com.brocast.riak.api.beans.DcMediaEntity;

import java.io.Serializable;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
public class MediaContent implements Serializable {
    private static final long serialVersionUID = 1L;
    private Error error = new Error();
    private DcMediaEntity entity;

    public MediaContent() {

    }

    public MediaContent(Error error) {
        this.error = error;
    }

    public MediaContent(DcMediaEntity entity) {
        this.entity = entity;
    }

    public Error getError() {
        return error;
    }

    public void setError(Error error) {
        this.error = error;
    }

    public DcMediaEntity getEntity() {
        return entity;
    }

    public void setEntity(DcMediaEntity entity) {
        this.entity = entity;
    }
}
