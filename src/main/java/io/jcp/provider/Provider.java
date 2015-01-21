package io.jcp.provider;

import java.util.Optional;

public interface Provider<T, H> {
    Optional<H> fetch(T query);
}
