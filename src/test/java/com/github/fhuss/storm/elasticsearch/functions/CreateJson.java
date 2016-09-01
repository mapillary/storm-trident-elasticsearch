package com.github.fhuss.storm.elasticsearch.functions;

import org.apache.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class CreateJson extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            collector.emit(new Values(new ObjectMapper().writeValueAsString(tuple.getValue(0))));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}