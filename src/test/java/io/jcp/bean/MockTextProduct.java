package io.jcp.bean;

import java.util.Optional;

public class MockTextProduct{
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MockTextProduct that = (MockTextProduct) o;

        if (!query.equals(that.query)) return false;
        if (!response.equals(that.response)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = response.hashCode();
        result = 31 * result + query.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MockTextProduct{" +
            "response='" + response + '\'' +
            ", query=" + query +
            '}';
    }
}
