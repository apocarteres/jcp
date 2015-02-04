package io.jcp.provider;

import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;

import java.util.Optional;

import static java.lang.Thread.sleep;

public final class MockTextProvider implements Provider<MockTextQuery, MockTextProduct> {

    public static final int FETCH_DELAY = 1000;
    public static final String DEFAULT_RESPONSE = "_pong";

    private final String pong;

    public MockTextProvider() {
        this(DEFAULT_RESPONSE);
    }

    public MockTextProvider(String pong) {
        this.pong = pong;
    }

    @Override
    public Optional<MockTextProduct> fetch(MockTextQuery query) {
        try {
            sleep(FETCH_DELAY);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Optional.of(new MockTextProduct(
                query.getRequest() + pong, Optional.of(query)
            )
        );
    }
}
