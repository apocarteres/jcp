package io.jcp.service;

import io.jcp.bean.Callback;
import io.jcp.bean.Product;
import io.jcp.bean.Query;

import java.util.Optional;

public interface QueryExecutorService<T extends Query, H extends Product<T>> {
    void exec(T task, Optional<Callback<H>> callback);
}
