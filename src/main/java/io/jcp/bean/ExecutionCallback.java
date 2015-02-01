package io.jcp.bean;

import java.util.Optional;

public interface ExecutionCallback<T, H> {
    void call(T query, Optional<H> product);
}
