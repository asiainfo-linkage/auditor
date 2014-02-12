package com.ailk.oci.auditor.plugin.audit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserInfo {
    public static final String USER_UNKNOWN = "UNKNOWN";
    private static final Pattern PARSER = Pattern.compile("(.+?)(?: \\(auth:.*?\\)(?: via (.+?) \\(auth:.+?\\))?)?");
    private final String username;
    private final String impersonator;

    public UserInfo(String userString) {
        String username = USER_UNKNOWN;
        String impersonator = null;
        if (userString != null) {
            Matcher m = PARSER.matcher(userString);
            if (m.matches()) {
                username = m.group(1);
                impersonator = m.group(2);
            }
        }

        this.username = username;
        this.impersonator = impersonator;
    }

    public String getUsername() {
        return this.username;
    }

    public String getImpersonator() {
        return this.impersonator;
    }
}