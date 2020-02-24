package com.dy.kafka.clients.consumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;

public interface LifecycleConsumerElements {

    OnConsumerStop ON_CONSUMER_STOP_DEF = () -> {};
    ConsumerRebalanceListener DEF_NOOP_REBALANCE_LISTENER = new NoOpConsumerRebalanceListener();
    FlowErrorHandler DEF_ON_FLOW_ERROR_HANDLER = new FlowErrorHandler() {};

    default OnConsumerStop onStop() { return ON_CONSUMER_STOP_DEF; }
    default ConsumerRebalanceListener rebalanceListener() { return null; }
    default FlowErrorHandler flowErrorHandler() { return DEF_ON_FLOW_ERROR_HANDLER; }

    default Builder toBuilder() {
        return new Builder().onConsumerStop(onStop())
                .flowErrorHandler(flowErrorHandler()).rebalanceListener(rebalanceListener());
    }

    static Builder builder() {
        return new Builder();
    }

    @Getter
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    class Builder {
        private OnConsumerStop onConsumerStop = () -> {};
        private ConsumerRebalanceListener rebalanceListener = new NoOpConsumerRebalanceListener();
        private FlowErrorHandler flowErrorHandler = new FlowErrorHandler() {};

        public LifecycleConsumerElements build() {
            return new LifecycleConsumerElements() {
                public OnConsumerStop onStop() { return getOnConsumerStop(); }
                public ConsumerRebalanceListener rebalanceListener() { return getRebalanceListener(); }
                public FlowErrorHandler flowErrorHandler() {
                    return getFlowErrorHandler();
                }
            };
        }

        public Builder onConsumerStop(OnConsumerStop onConsumerStop) {
            if (onConsumerStop != null)
                this.onConsumerStop = onConsumerStop;
            return this;
        }

        public Builder rebalanceListener(ConsumerRebalanceListener rebalanceListener) {
            this.rebalanceListener = rebalanceListener;
            return this;
        }

        public Builder flowErrorHandler(FlowErrorHandler flowErrorHandler) {
            if (flowErrorHandler != null)
                this.flowErrorHandler = flowErrorHandler;
            return this;
        }
    }
}
