package io.ingestr.framework.service.workers.lock;

import java.util.concurrent.locks.Lock;

public interface LoaderLock {
    Lock getLock();
}
