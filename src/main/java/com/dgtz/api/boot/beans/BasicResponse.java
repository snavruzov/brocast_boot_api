package com.dgtz.api.boot.beans;

import java.io.Serializable;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
public class BasicResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private Error error = new Error();

    public BasicResponse() {
        this.error = new Error("OK", "The request successfully applied with no errors.");
    }

    public BasicResponse(Error error) {
        this.error = error;
    }

    public Error getError() {
        return error;
    }

    public void setError(Error error) {
        this.error = error;
    }
}
