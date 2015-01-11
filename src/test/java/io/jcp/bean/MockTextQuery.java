package io.jcp.bean;

public class MockTextQuery implements Query {
    private final String request;

    public MockTextQuery() {
        this("");
    }

    public MockTextQuery(String request) {
        this.request = request;
    }

    public String getRequest() {
        return request;
    }
}
