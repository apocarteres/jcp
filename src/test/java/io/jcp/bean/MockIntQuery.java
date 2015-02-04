package io.jcp.bean;

public class MockIntQuery {
    private final int value;

    public MockIntQuery() {
        this(0);
    }

    public MockIntQuery(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MockIntQuery that = (MockIntQuery) o;

        if (value != that.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return "MockIntQuery{" +
            "value=" + value +
            '}';
    }
}
