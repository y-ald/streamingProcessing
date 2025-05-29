package org.yald.infrastructure;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.yald.domain.Transaction;

public class TransactionDataSource {
    private static final GeneratorFunction<Long, Transaction> generatorFunction = index  -> {
        String transactionId = index.toString();
        String accountId = Integer.toString( (int) (Math.random() * 5));
        double amount = index % 2==0 ? 0.93: Math.pow(Math.random(), 0.2) * 700;

        return new Transaction(transactionId, accountId, amount);
    };

    private final DataGeneratorSource<Transaction> transactionDataGeneratorSource;

    public TransactionDataSource() {
        transactionDataGeneratorSource = new DataGeneratorSource<Transaction>(
                generatorFunction,
                10L,
                TypeInformation.of(Transaction.class)
        );
    }

    public DataGeneratorSource<Transaction> getTransactionDataGeneratorSource() {
        return transactionDataGeneratorSource;
    }
}
