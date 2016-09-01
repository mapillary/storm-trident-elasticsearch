package com.github.fhuss.storm.elasticsearch.functions;

import org.apache.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class ExtractSearchArgs extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String args = (String)tuple.getValue(0);
        String[] split = args.split(" ");
        collector.emit(new Values(split[0], Lists.newArrayList(split[1]), Lists.newArrayList(split[2])));
    }
}