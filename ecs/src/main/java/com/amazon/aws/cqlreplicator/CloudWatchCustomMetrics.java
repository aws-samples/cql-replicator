package com.amazon.aws.cqlreplicator;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

public class CloudWatchCustomMetrics {

    private final static CloudWatchConfig cloudWatchConfig = new CloudWatchConfig() {
        @NotNull
        @Override
        public String namespace() {
            return "cqlreplicator";
        }

        @Override
        public String get(@NotNull String key) {
            return null;
        }
    };
    private final MeterRegistry meterRegistry;

    public CloudWatchCustomMetrics(CloudWatchAsyncClient cloudWatchAsyncClient) {

        this.meterRegistry = new CloudWatchMeterRegistry(cloudWatchConfig, Clock.SYSTEM, cloudWatchAsyncClient);
        this.meterRegistry.config().commonTags("application", "cqlreplicator");
        //new JvmGcMetrics().bindTo(meterRegistry);
        //new JvmMemoryMetrics().bindTo(meterRegistry);
        //new ProcessorMetrics().bindTo(meterRegistry);

    }

    public MeterRegistry getMeterRegistry() {
        return this.meterRegistry;
    }

}
