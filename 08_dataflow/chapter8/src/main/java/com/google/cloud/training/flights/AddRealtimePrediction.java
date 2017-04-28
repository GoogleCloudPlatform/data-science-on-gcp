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

import java.rmi.UnexpectedException;
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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
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
 * Listens to Pub/Sub, adds prediction and writes out data to Bigtable.
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
    //PCollection<String> lines = toCsvOneByOne(outFlights);
    PCollection<String> lines = addPredictionInBatches(outFlights);
    lines.apply("Write", TextIO.Write.to(options.getOutput() + "flightPreds").withSuffix(".csv"));
  }

  private static PCollection<String> addPredictionOneByOne(PCollection<Flight> outFlights) {
    PCollection<String> lines = outFlights //
        .apply("Inference", ParDo.of(new DoFn<Flight, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            String ontime = "unset";
            try {
              if (f.getField(INPUTCOLS.EVENT).equals("arrived")) {
                // actual ontime performance
                ontime = Double.toString(f.getFieldAsFloat(INPUTCOLS.ARR_DELAY, 0) < 15? 1 : 0);
              } else {
                // wheelsoff: predict ontime arrival probability
                ontime = Double.toString(CallPrediction.predictOntimeProbability(f, -5.0));
              }
            } catch (Throwable t) {
              LOG.warn("Prediction failed: " + t);
              ontime = t.getMessage();
            }
            // create output CSV
            String csv = String.join(",", f.getFields());
            csv = csv + "," + ontime;
            c.output(csv); // actual ontime performance
          }
        }));
    return lines;
  }

  private static PCollection<String> addPredictionInBatches(PCollection<Flight> outFlights) {
    PCollection<String> lines = outFlights //
        .apply(Window.<Flight> into(FixedWindows.of(Duration.standardMinutes(1)))) //
        .apply("Minibatch_s1", ParDo.of(new DoFn<Flight, KV<String, Flight>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            c.output(KV.of("all", f));
          }
        }))//
        .apply("Minibatch_s2", GroupByKey.<String, Flight>create()) //
        .apply("Inference", ParDo.of(new DoFn<KV<String, Iterable<Flight>>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Iterable<Flight> flights = c.element().getValue();
            // do all the arrived events immediately
            for (Flight f : flights) {
              if (f.getField(INPUTCOLS.EVENT).equals("arrived")) {
                double ontime = f.getFieldAsFloat(INPUTCOLS.ARR_DELAY, 0) < 15? 1 : 0;
                String csv = String.join(",", f.getFields());
                csv = csv + "," + ontime;
                c.output(csv); // actual ontime performance
              }
            }
            
            // batch all the wheelsoff events into single request
            try {
              CallPrediction.Request request = new CallPrediction.Request();
              for (Flight f : flights) {
                if (f.getField(INPUTCOLS.EVENT).equals("wheelsoff") && f.isNotCancelled() && f.isNotDiverted()) {
                  CallPrediction.Instance instance = new CallPrediction.Instance(f);
                  request.instances.add(instance);
                }
              }
              // send request
              CallPrediction.Response resp = CallPrediction.sendRequest(request);
              double[] result = resp.getOntimeProbability(-5);
              if (result.length == request.instances.size()) {
                int resultno = 0;
                for (Flight f : flights) {
                  if (f.getField(INPUTCOLS.EVENT).equals("wheelsoff") && f.isNotCancelled() && f.isNotDiverted()) {
                    double ontime = result[resultno++];
                    String csv = String.join(",", f.getFields());
                    csv = csv + "," + ontime;
                    c.output(csv); // predicted ontime performance
                  }
                }
              } else {
                throw new UnexpectedException("Should receive as many results as instances sent");
              }
            } catch (Throwable t) {
              LOG.warn("Inference failed", t);
              c.output(t.getMessage());
            }
          }
        }));
    return lines;
  }
  
}
