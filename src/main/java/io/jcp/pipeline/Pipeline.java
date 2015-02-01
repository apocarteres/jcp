package io.jcp.pipeline;

import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.service.QueryExecutorService;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * <p>The interface which represents the basic abstraction of the
 * sequence of user-defined operations.
 * <p>
 * <p>Basically pipeline consumes special objects of type {@link T}, known
 * as queries, applies user operations in defined order on
 * queries and finally builds a product of type {@link H}
 *
 *
 * @param <T> is a type of query
 * @param <H> is a type of product
 * @author Alexander Paderin
 * @since 0.1
 */
public interface Pipeline<T, H> {

    /**
     * Puts specified query to pipeline
     *
     * @param query Query to execution
     * @return Pipeline initialized with query
     */
    Pipeline<T, H> run(T query);

    /**
     * Puts specified queries to pipeline
     *
     * @param queries Queries to execution
     * @return Pipeline initialized with queries
     */
    Pipeline<T, H> run(Collection<T> queries);

    /**
     * Puts specified function into the pipeline
     *
     * Uses underlying pipeline to process function
     * The result product type depends on underlying pipeline
     * generics.
     *
     * @param function Function to execution
     * @param underlying Pipeline to process function
     * @return Pipeline initialized with function
     */
    <R, K> Pipeline<R, K>
    run(Function<H, R> function, Pipeline<R, K> underlying);

    /**
     * Puts specified function into the pipeline
     * @param function
     * @return
     */
    Pipeline<T, H>
    run(Function<H, T> function);

    /**
     * Sets up service to execute queries
     *
     * @param service Service to execute queries
     * @return Pipeline initialized with service
     */
    <R, K> Pipeline<R, K> using(QueryExecutorService<R, K> service);

    /**
     * Sets up callback to handle query completion
     *
     * @param callback to handle query completion
     * @return Pipeline initialized with callback
     */
    Pipeline<T, H> on(QueryCompleteCallback<T, H> callback);

    /**
     * Fetches products and wrap them into the {@link java.util.stream.Stream}.
     *
     * @return {@link java.util.stream.Stream} of objects of type {@link H} known as products
     */
    default Stream<H> stream() {
        return Stream.empty();
    }

    /**
     * Fetches exactly one product
     * <p>
     * If pipeline contains more than one product, which product will be returned,
     * completely depends on implementation
     *
     * @return {@link java.util.Optional} of object of type {@link H} known as product
     */
    default Optional<H> product() {
        return stream().findFirst();
    }

    /**
     * Fetches all available products
     * <p>
     * The order of products in list completely depends on implementation
     *
     * @return {@link java.util.List} of objects of type {@link H} known as products
     */
    default List<H> products() {
        return stream().collect(toList());
    }

    /**
     * Returns parent pipeline
     *
     * @return {@link java.util.Optional} of {@link io.jcp.pipeline.Pipeline}
     */
    default Optional<Pipeline<T, H>> getParent() {
        return Optional.empty();
    }

    /**
     * Returns queries of pipeline
     *
     * @return {@link java.util.Optional} of objects of type {@link T} known as queries
     */
    default Optional<Collection<T>> getQueries() {
        return Optional.empty();
    }

    /**
     * Returns service for executing queries
     *
     * @return {@link java.util.Optional} of {@link io.jcp.service.QueryExecutorService}
     */
    default Optional<QueryExecutorService<T, H>> getService() {
        return Optional.empty();
    }

    /**
     * Returns callback for handling query executing finish
     *
     * @return {@link java.util.Optional} of {@link io.jcp.pipeline.callback.QueryCompleteCallback}
     */
    default Optional<QueryCompleteCallback<T, H>> getCompleteCallback() {
        return Optional.empty();
    }

    /**
     * Returns function which transforms product, fetched on previous pipeline step
     * to new query
     *
     * @return {@link java.util.Optional} of {@link java.util.function.Function}
     */
    default Optional<Function<H, T>> getProductMapper() {
        return Optional.empty();
    }
}
