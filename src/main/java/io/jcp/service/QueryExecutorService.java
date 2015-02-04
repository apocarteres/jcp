package io.jcp.service;

import java.util.Optional;

@FunctionalInterface
public interface QueryExecutorService<T, H> {
    Optional<H> exec(T query);

    default void shutdown() {
    }

}
