package io.jcp.provider;

import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;

import java.util.Optional;

import static java.lang.Thread.sleep;

public class MockProvider implements Provider<MockTextQuery, MockTextProduct> {

    public static final int FETCH_DELAY = 1000;

    @Override
    public Optional<MockTextProduct> fetch(MockTextQuery query) {
        try {
            sleep(FETCH_DELAY);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Optional.of(new MockTextProduct(
            query.getRequest() + "_pong", Optional.of(query))
        );
    }
}
