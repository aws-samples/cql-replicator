package com.amazon.aws.cqlreplicator.util;

public class ApiEndpoints {
    public static final String API = "/api";
    public static final String HEALTH = "/health";
    public static final String HEALTH_ROUTE = API + HEALTH;
    private ApiEndpoints()
    {
        throw new IllegalStateException(getClass() + " is a constants container and shall not be instantiated");
    }
}
