package io.jcp.bean;

public class MockTextQuery {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MockTextQuery that = (MockTextQuery) o;

        if (!request.equals(that.request)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return request.hashCode();
    }

    @Override
    public String toString() {
        return "MockTextQuery{" +
            "request='" + request + '\'' +
            '}';
    }
}
