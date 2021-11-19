package org.knoldus.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * This [[ApacheBeamTextIO]] represents a beam pipeline that reads sales data(csv file)
 * and extracting payment type card and counting for every card. result is writing to
 * another csv file with header, Card,Count.
 *
 * Card,Count
 * Visa,522
 * Mastercard,277
 * Diners,89
 * Amex,110
 */
final public class ApacheBeamTextIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheBeamTextIO.class);

    private static final String CSV_HEADER = "Transaction_date,Product,Price,Payment_Type,Name,City,State,Country," +
            "Account_Created,Last_Login,Latitude,Longitude,US Zip";

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read-Lines", TextIO.read().from("src/main/resources/source/SalesJan2009.csv"))
                .apply("Filter-Header", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("payment-extractor", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[3])))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + "," + typeCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to("src/main/resources/sink/payment_type_count")
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("Card,Count"));

        LOGGER.info("Executing pipeline");
        pipeline.run();
    }

    //TODO: keep it in another class not as inner class.
    private static class FilterHeaderFn extends DoFn<String, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(FilterHeaderFn.class);

        private final String header;

        FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext processContext) {
            String row = processContext.element();
            if (!row.isEmpty() && !row.contains(header))
                processContext.output(row);
            else
                LOGGER.info("Filtered out the header of the csv file: [{}]", row);

        }
    }
}
