package io.jcp.provider;

import io.jcp.bean.MockIntProduct;
import io.jcp.bean.MockIntQuery;

import java.util.Optional;

import static java.lang.Thread.sleep;

public final class MockIntProvider implements Provider<MockIntQuery, MockIntProduct> {

    public static final int FETCH_DELAY = 1000;

    @Override
    public Optional<MockIntProduct> fetch(MockIntQuery query) {
        try {
            sleep(FETCH_DELAY);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Optional.of(
            new MockIntProduct(
                query.getValue() * 1000, Optional.of(query)
            )
        );
    }
}
