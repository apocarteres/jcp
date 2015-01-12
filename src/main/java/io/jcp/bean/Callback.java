package io.jcp.bean;

import java.util.Optional;

public interface Callback<T, H> {
    void call(T query, Optional<H> product);
}
