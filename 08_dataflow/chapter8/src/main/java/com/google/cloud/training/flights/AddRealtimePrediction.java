/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.training.flights;

import java.text.DecimalFormat;
import java.util.Map;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.training.flights.Flight.INPUTCOLS;

/**
 * Listens to Pub/Sub, adds prediction and writes out data to Datastore.
 * 
 * @author vlakshmanan
 *
 */
@SuppressWarnings("serial")
public class AddRealtimePrediction {
  private static final Logger LOG = LoggerFactory.getLogger(AddRealtimePrediction.class);

  public static interface MyOptions extends DataflowPipelineOptions {
    @Description("Output directory")
    @Default.String("gs://cloud-training-demos-ml/flights/chapter10/output/")
    String getOutput();

    void setOutput(String s);

    @Description("Path to average departure delay file")
    @Default.String("gs://cloud-training-demos-ml/flights/chapter8/output/delays.csv")
    String getDelayPath();

    void setDelayPath(String s);
  }

  private static PCollectionView<Map<String, Double>> readAverageDepartureDelay(Pipeline p, String path) {
    return p.apply("Read delays.csv", TextIO.Read.from(path)) //
        .apply("Parse delays.csv", ParDo.of(new DoFn<String, KV<String, Double>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            String line = c.element();
            String[] fields = line.split(",");
            c.output(KV.of(fields[0], Double.parseDouble(fields[1])));
          }
        })) //
        .apply("toView", View.asMap());
  }

  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    // options.setStreaming(true);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://cloud-training-demos-ml/flights/staging");
    Pipeline p = Pipeline.create(options);

    String query = "SELECT EVENT_DATA FROM flights.simevents WHERE ";
    query += " STRING(FL_DATE) = '2015-01-04' AND ";
    query += " (EVENT = 'wheelsoff' OR EVENT = 'arrived') ";
    LOG.info(query);

    PCollection<Flight> allFlights = readFlights(p, query);

    PCollectionView<Map<String, Double>> avgDepDelay = readAverageDepartureDelay(p, options.getDelayPath());

    PCollection<Flight> hourlyFlights = allFlights.apply(Window.<Flight> into(SlidingWindows//
        .of(Duration.standardHours(1))//
        .every(Duration.standardMinutes(5)))); // .discardingFiredPanes());

    PCollection<KV<String, Double>> avgArrDelay = CreateTrainingDataset.computeAverageArrivalDelay(hourlyFlights);

    hourlyFlights = CreateTrainingDataset.addDelayInformation(hourlyFlights, avgDepDelay, avgArrDelay);

    writeFlights(hourlyFlights, options);

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  private static PCollection<Flight> readFlights(Pipeline p, String query) {
    PCollection<Flight> allFlights = p //
        .apply("ReadLines", BigQueryIO.Read.fromQuery(query)) //
        .apply("ParseFlights", ParDo.of(new DoFn<TableRow, Flight>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            TableRow row = c.element();
            String line = (String) row.getOrDefault("EVENT_DATA", "");
            Flight f = Flight.fromCsv(line);
            if (f != null) {
              c.outputWithTimestamp(f, f.getEventTimestamp());
            }
          }
        }));
    return allFlights;
  }

  private static void writeFlights(PCollection<Flight> outFlights, MyOptions options) {
    // PCollection<String> lines = addPredictionOneByOne(outFlights);
    try {
      PCollection<String> lines = addPredictionInBatches(outFlights);
      lines.apply("Write", TextIO.Write.to(options.getOutput() + "flightPreds").withSuffix(".csv"));
    } catch (Throwable t) {
      LOG.warn("Inference failed", t);
    }
  }

/*  private static PCollection<String> addPredictionOneByOne(PCollection<Flight> outFlights) {
    PCollection<String> lines = outFlights //
        .apply("Inference", ParDo.of(new DoFn<Flight, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            String ontime = "";
            try {
              if (f.isNotCancelled() && f.isNotDiverted()) {
                if (f.getField(INPUTCOLS.EVENT).equals("arrived")) {
                  // actual ontime performance
                  ontime = Double.toString(f.getFieldAsFloat(INPUTCOLS.ARR_DELAY, 0) < 15 ? 1 : 0);
                } else {
                  // wheelsoff: predict ontime arrival probability
                  ontime = Double.toString(CallPrediction.predictOntimeProbability(f, -5.0));
                }
              }
            } catch (Throwable t) {
              LOG.warn("Prediction failed: ", t);
              ontime = t.getMessage();
            }
            // create output CSV
            String csv = String.join(",", f.getFields());
            csv = csv + "," + ontime;
            c.output(csv);
          }
        }));
    return lines;
  }*/

  private static PCollection<String> addPredictionInBatches(PCollection<Flight> outFlights) {
    final int NUM_BATCHES = 2;
    PCollection<String> lines = outFlights //
        .apply("Batch->Flight", ParDo.of(new DoFn<Flight, KV<String, Flight>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            String key;
            if (f.isNotCancelled() && f.isNotDiverted()) {
              key = f.getField(INPUTCOLS.EVENT); // "arrived" "wheelsoff"
            } else {
              key = "ignored";
            }
            // add a randomized part to provide batching capability
            key = key + " " + (System.identityHashCode(f) % NUM_BATCHES);
            c.output(KV.of(key, f));
          }
        })) //
        .apply("CreateBatches", GroupByKey.<String, Flight> create()) // within window
        .apply("Inference", ParDo.of(new DoFn<KV<String, Iterable<Flight>>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            String key = c.element().getKey();
            Iterable<Flight> flights = c.element().getValue();

            // write out all the ignored events as-is
            if (key.startsWith("ignored")) {
              for (Flight f : flights) {
                c.output(createOutput(f, ""));
              }
              return;
            }

            // for arrived events, emit actual ontime performance
            if (key.startsWith("arrived")) {
              for (Flight f : flights) {
                double ontime = f.getFieldAsFloat(INPUTCOLS.ARR_DELAY, 0) < 15 ? 1 : 0;
                c.output(createOutput(f, ontime));
              }
              return;
            }

            // do ml inference for wheelsoff events, but as batch
            CallPrediction.Request request = new CallPrediction.Request();
            for (Flight f : flights) {
              request.instances.add(new CallPrediction.Instance(f));
            }
            CallPrediction.Response resp = CallPrediction.sendRequest(request);
            double[] result = resp.getOntimeProbability(-5);
            // append probability
            int resultno = 0;
            for (Flight f : flights) {
              double ontime = result[resultno++];
              c.output(createOutput(f, ontime));
            }
          }
        }));
    return lines;
  }

  private static String createOutput(Flight f, double ontime) {
    // round off to nearest 0.01
    DecimalFormat df = new DecimalFormat("0.00");
    return createOutput(f, df.format(ontime));
  }
  
  private static String createOutput(Flight f, String ontime) {
    String csv = String.join(",", f.getFields());
    csv = csv + "," + ontime;
    return csv;
  }
}
