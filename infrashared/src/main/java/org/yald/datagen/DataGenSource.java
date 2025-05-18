package org.yald.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.yald.domain.Transaction;

public class DataGenSource<T> {

    private final GeneratorFunction<Long, T> generatorFunction;
    private final TypeInformation<T> typeInformation;
    private final Long numberOfRecords;

    public DataGenSource(GeneratorFunction<Long, T> generatorFunction, long numberOfRecords, TypeInformation<T> typeInformation) {
        this.generatorFunction = generatorFunction;
        this.numberOfRecords = numberOfRecords;
        this.typeInformation = typeInformation;
    }

    public DataGeneratorSource<T> getDataGeneratorSource() {
        return new DataGeneratorSource<T>(generatorFunction, numberOfRecords, typeInformation);
    }
}
