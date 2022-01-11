package io.ingestr.framework.service.workers.lock;

import java.util.concurrent.locks.Lock;

public class LoaderLockImpl implements LoaderLock{
    private Lock lock;

    public LoaderLockImpl(Lock lock) {
        this.lock = lock;
    }

    @Override
    public Lock getLock() {
        return lock;
    }
}
