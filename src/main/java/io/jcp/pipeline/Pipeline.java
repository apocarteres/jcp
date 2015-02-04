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
    run(Function<H, R> function,
        Pipeline<R, K> underlying
    );

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
    <R, K> Pipeline<R, K> using(
        QueryExecutorService<R, K> service
    );

    /**
     * Sets up callback to handle query completion
     *
     * @param callback to handle query completion
     * @return Pipeline initialized with callback
     */
    Pipeline<T, H> on(
        QueryCompleteCallback<T, H> callback
    );

    /**
     * Fetches exactly one product
     * <p>
     * If pipeline contains more than one product, which product will be returned,
     * completely depends on implementation
     *
     * @return {@link java.util.Optional} of object of type {@link H} known as product
     */
    default Optional<H> product() {
        return Optional.empty();
    }
}
