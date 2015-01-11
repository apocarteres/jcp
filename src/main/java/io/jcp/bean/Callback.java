package io.jcp.bean;

public interface Callback<T, H> {
    void call(T query, H product);
}
