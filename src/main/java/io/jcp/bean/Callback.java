package io.jcp.bean;

public interface Callback<T extends Product> {
    void call(T product);
}
