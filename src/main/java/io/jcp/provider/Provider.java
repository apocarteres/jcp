package io.jcp.provider;

import java.util.Optional;

/**
 * Consumes object of type {@link T} known as query
 * and produces the object of type {@link H} known as product
 *
 * @param <T> is a type of query
 * @param <H> is a type of product
 */
@FunctionalInterface
public interface Provider<T, H> {
    /**
     * Executes specified query and produces a product
     *
     * @param query query to execute
     * @return {@link java.util.Optional#empty} if something went wrong,
     * {@link java.util.Optional} of object of type {@link H} if fetch was successful
     */
    Optional<H> fetch(T query);
}
