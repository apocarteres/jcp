package io.jcp.service;

import io.jcp.bean.Callback;
import io.jcp.bean.Product;
import io.jcp.bean.Query;

import java.util.Optional;

public interface QueryManagerService<T extends Query, H extends Product<T>> {
    void submit(T task, Optional<Callback<H>> callback);

    H exec(T task) throws InterruptedException;

    long countSubmitted();

    long countInProgress();

    void shutdown() throws InterruptedException;
}
