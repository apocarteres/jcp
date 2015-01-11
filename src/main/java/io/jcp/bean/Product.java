package io.jcp.bean;

import java.util.Optional;

public interface Product<T extends Query> {
    default Optional<T> getQuery() {
        return Optional.empty();
    }
}
