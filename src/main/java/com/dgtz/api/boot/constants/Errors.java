package com.dgtz.api.boot.constants;

import com.dgtz.api.boot.beans.Error;
/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
public class Errors {
    public final static Error HASH_EMPTY = new Error("INVALID_HASH", "Wrong hash or not found");
    public final static Error AUTH_ERROR = new Error("AUTH_ERROR", "Invalid password or email");
    public final static Error USER_ACCOUNT_SESSION_EXPIRED = new Error("USER_ACCOUNT_SESSION_EXPIRED", "Your account has an invalid session or your session's been expired, try to Sign In again.");
    public final static Error EMPTY_PARAM = new Error("EMPTY_PARAM", "Empty request parameter or missed field");
    public final static Error SYSTEM_FAIL = new Error("SYSTEM_FAIL", "We couldn't perform your request, please try again later.");
    public final static Error TITLE_REQUIRED = new Error("TITLE_REQUIRED", "Live cannot be started without title");
    public final static Error EMAIL_ALREADY_EXISTED = new Error("EMAIL_ALREADY_EXISTED", "Email you entered is already registered, choose another address");
    public final static Error INVALID_EML_FORMAT = new Error("INVALID_EML_FORMAT", "Invalid email format, please use a valid format ex: user@example.com");
    public final static Error INVALID_USERNAME_FORMAT = new Error("INVALID_USERNAME_FORMAT", "Username should be 3 to 25 characters with any lower case character, " +
            "digit or special symbol “_-” only");
    public final static Error NO_LIVE_FOUND = new Error("NO_LIVE_FOUND", "No Lives found by requested url, live ended or not existed");
    public final static Error NO_VIDEO_FOUND = new Error("NO_VIDEO_FOUND", "No such video found by requested url, check if video is existed");
    public final static Error MEDIA_REMOVE_ERROR = new Error("MEDIA_REMOVE_ERROR", "Unable to remove the content, see logs or contact administrator");
    public final static Error COMMENT_BLOCK_ERROR = new Error("COMMENT_BLOCK_ERROR", "Unable to block the comment, see logs or contact administrator");
    public final static Error WRONG_ID = new Error("WRONG_ID", "Wrong ID format check for ID format it should be numeric, non-negative, non-zero or alphanumeric only");
    public final static Error CHANNEL_REMOVE_ERROR = new Error("CHANNEL_REMOVE_ERROR", "Something wrong happened while removing the channel, check the logs of the server");
    public final static Error CHANNEL_NOT_FOUND = new Error("CHANNEL_NOT_FOUND", "No such channel, please make sure that channel exists");

}
