package io.jcp.bean;

import java.util.Optional;

public class MockTextProduct implements Product<MockTextQuery> {
    private final String response;
    private final Optional<MockTextQuery> query;

    public MockTextProduct(MockTextQuery query) {
        this("", Optional.empty());

    }

    public MockTextProduct(String response, Optional<MockTextQuery> query) {
        this.response = response;
        this.query = query;
    }

    public String getResponse() {
        return response;
    }

    @Override
    public Optional<MockTextQuery> getQuery() {
        return query;
    }
}
