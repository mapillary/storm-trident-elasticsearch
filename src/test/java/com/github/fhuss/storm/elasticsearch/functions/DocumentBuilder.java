package com.github.fhuss.storm.elasticsearch.functions;


import org.apache.storm.tuple.Values;
import com.github.fhuss.storm.elasticsearch.Document;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class DocumentBuilder extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        collector.emit(new Values( new Document<>("my_index", "my_type", sentence, String.valueOf(sentence.hashCode()))));
    }
}
