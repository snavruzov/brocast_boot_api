package com.dgtz.api.boot.beans;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
public class Error {
    private String code = "OK";
    private String message = "";

    public Error() {
    }

    public Error(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
