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
}