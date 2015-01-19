package io.jcp.pipeline.callback;

import java.util.Optional;

@FunctionalInterface
public interface QueryCompleteCallback<T, H> {
    void onComplete(T query, Optional<H> product);
}
