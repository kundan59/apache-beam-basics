package org.knoldus.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This [[ApacheBeamInMemory]] class represents a beam pipeline to create pcollection from
 * In memory data and use it in pipeline. The pipeline read it and printing type of cards and their
 * Count
 * Diners:1
 * Visa:2
 * MasterCard:2
 * Rupayee:1
 */
final public class ApacheBeamInMemory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheBeamInMemory.class);
    public static void main(String[] args) {

        final List<String> list = Arrays.asList("15/01/2021 5:39,Shoes,1200,MasterCard,Banglore",
                "16/02/2021 7:49,Jacket,1500,MasterCard,Delhi",
                "01/02/2021 4:50,Phones,2400,Visa,Noida",
                "23/03/2021 2:30,Cooler,1100,Diners,Delhi",
                "05/02/2021 6:35,Ac,30000,Rupayee,Gurgaon",
                "31/01/2021 1:39,Jeans,1100,Visa,Banglore");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(list)).setCoder(StringUtf8Coder.of())
                .apply("print-before", MapElements.via(new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        LOGGER.info(input);
                        return input;
                    }
                }))
                .apply("payment-extractor", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[3])))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + ":" + typeCount.getValue()))
                .apply("print-after", MapElements.via(new SimpleFunction<String, Void>() {
                    @Override
                    public Void apply(String input) {
                        LOGGER.info(input);
                        return null;
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
