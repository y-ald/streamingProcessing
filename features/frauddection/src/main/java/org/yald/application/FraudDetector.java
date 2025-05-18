package org.yald.application;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.domain.Alert;
import org.yald.domain.Transaction;

import java.io.IOException;

public class FraudDetector extends KeyedProcessFunction <String, Transaction, Alert>{
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;
    private static final Logger logger
            = LoggerFactory.getLogger(FraudDetector.class);

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN
        );
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        flagState = getRuntimeContext().getState(flagDescriptor);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();
        logger.info("Processing transaction: " + transaction.toString(), flagState.value());
        if (lastTransactionWasSmall != null) {
            identifyLargeAmountTransactionAndCreateAlert(transaction, collector);
            cleanUp(context);
        }

        if (transaction.amount() < SMALL_AMOUNT) {
            setStateForSmallAmountTransaction(context);
        }
    }

    private static void identifyLargeAmountTransactionAndCreateAlert(Transaction transaction, Collector<Alert> collector) {
        if (transaction.amount() > LARGE_AMOUNT) {
            Alert alert = new Alert(transaction.accountId(), transaction.transactionId());

            collector.collect(alert);
        }
    }

    private void cleanUp(Context ctx) throws Exception {
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        timerState.clear();
        flagState.clear();
    }

    private void setStateForSmallAmountTransaction(KeyedProcessFunction<String, Transaction, Alert>.Context context) throws IOException {
        flagState.update(true);

        long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
        context.timerService().registerProcessingTimeTimer(timer);

        timerState.update(timer);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

}
