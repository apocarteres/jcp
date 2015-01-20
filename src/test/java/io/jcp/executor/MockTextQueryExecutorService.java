package io.jcp.executor;

import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;

import java.util.Optional;

public class MockTextQueryExecutorService extends MockQueryExecutorService<MockTextQuery, MockTextProduct> {
    @Override
    protected MockTextProduct build(MockTextQuery query) {
        return new MockTextProduct(query.getRequest() + "_pong", Optional.of(query));
    }
}
