package io.jcp.bean;

import java.util.Optional;

public class MockIntProduct implements Product<MockIntQuery> {
    private final int response;
    private final Optional<MockIntQuery> query;

    public MockIntProduct(MockIntQuery query) {
        this(0, Optional.empty());

    }

    public MockIntProduct(int response, Optional<MockIntQuery> query) {
        this.response = response;
        this.query = query;
    }

    public int getResponse() {
        return response;
    }

    @Override
    public Optional<MockIntQuery> getQuery() {
        return query;
    }
}
