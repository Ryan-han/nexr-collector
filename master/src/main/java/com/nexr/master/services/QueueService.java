package com.nexr.master.services;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.nexr.master.CollectorException;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class QueueService implements AppService{

    private ScheduledExecutorService scheduledExecutorService;

    public QueueService() {

    }

    @Override
    public void start() throws CollectorException {
        scheduledExecutorService = Executors.newScheduledThreadPool(10,
                new ThreadFactoryBuilder().setNameFormat("queueservice-%d").build());
    }

    @Override
    public void shutdown() throws CollectorException {
        scheduledExecutorService.shutdown();
    }

    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return scheduledExecutorService.schedule(callable, delay, unit);
    }
}
