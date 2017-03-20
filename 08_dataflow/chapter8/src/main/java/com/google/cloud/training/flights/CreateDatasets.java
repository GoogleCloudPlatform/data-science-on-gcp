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
import org.apache.beam.sdk.transforms.Mean;
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
 * Runs on the cloud on 3 days of data, and takes about 10 minutes to process.
 * 
 * @author vlakshmanan
 *
 */
public class CreateDatasets {
  private static final Logger LOG = LoggerFactory.getLogger(CreateDatasets.class);
  
  public static interface MyOptions extends DataflowPipelineOptions {
    @Description("Should we process the full dataset or just a small sample?")
    @Default.Boolean(false)
    boolean getFullDataset();

    void setFullDataset(boolean b);
    
    @Description("Path of the output directory")
    @Default.String("gs://cloud-training-demos-ml/flights/chapter8/output/")
    String getOutput();

    void setOutput(String s);

    @Description("Path of trainday.csv")
    @Default.String("gs://cloud-training-demos-ml/flights/trainday.csv")
    String getTraindayCsvPath();

    void setTraindayCsvPath(String s);
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    // options.setStreaming(true);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://cloud-training-demos-ml/flights/staging");
    Pipeline p = Pipeline.create(options);

    // read traindays.csv into memory for use as a side-input
    PCollectionView<Map<String, String>> traindays = getTrainDays(p, options.getTraindayCsvPath());

    String query = "SELECT EVENT_DATA FROM flights.simevents WHERE ";
    if (!options.getFullDataset()) {
      query += " STRING(FL_DATE) < '2015-01-04' AND ";
    }
    query += " (EVENT = 'wheelsoff' OR EVENT = 'arrived') ";
    LOG.info(query);
    
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
        })) //
        .apply("GoodFlights", ParDo.of(new DoFn<Flight, Flight>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            if (f.isNotCancelled() && f.isNotDiverted()) {
              c.output(f);
            }
          }
        }));
    
    PCollection<KV<String, Double>> depDelays = //
        filterTrainOrTest("globalTrain", allFlights, traindays, true) //
        .apply("airport:hour->depdelay", ParDo.of(new DoFn<Flight, KV<String, Double>>() {

          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            if (f.getField(Flight.INPUTCOLS.EVENT).equals("wheelsoff")) {
              String key = f.getField(Flight.INPUTCOLS.ORIGIN) + ":" + f.getDepartureHour();
              double value = f.getFieldAsFloat(Flight.INPUTCOLS.DEP_DELAY)
                  + f.getFieldAsFloat(Flight.INPUTCOLS.TAXI_OUT);
              c.output(KV.of(key, value));
            }
          }

        })) //
        .apply("avgDepDelay", Mean.perKey());
    
    depDelays.apply("DepDelayToCsv", ParDo.of(new DoFn<KV<String, Double>, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        KV<String, Double> kv = c.element();
        c.output(kv.getKey() + "," + kv.getValue());
      }
    })) //
        .apply("WriteDepDelays", TextIO.Write.to(options.getOutput() + "delays").withSuffix(".csv").withoutSharding()); 

    PCollectionView<Map<String, Double>> avgDepDelay = depDelays.apply("depdelay->map", View.asMap());

    // note that departure delay is computed only on the training data and reused in test ...
    writeFlights(allFlights, traindays, depDelays, avgDepDelay, options, true);
    writeFlights(allFlights, traindays, depDelays, avgDepDelay, options, false);
    
    PipelineResult result = p.run();
    if (!options.getFullDataset()) {
      // for small datasets, block
      result.waitUntilFinish();
    }
  }

  @SuppressWarnings("serial")
  private static void writeFlights(PCollection<Flight> allFlights, PCollectionView<Map<String, String>> traindays, PCollection<KV<String, Double>> depDelays,
      PCollectionView<Map<String, Double>> avgDepDelay, MyOptions options, boolean trainOrTest) {
    
    String name = trainOrTest? "train_" : "test_";
    
    PCollection<Flight> trainFlights = //
        filterTrainOrTest(name + "hourly", allFlights, traindays, true) //
        .apply(Window.into(SlidingWindows//
        .of(Duration.standardHours(1))//
        .every(Duration.standardMinutes(5))));    
    
    PCollection<KV<String, Double>> arrDelays = trainFlights
        .apply(name + "airport->arrdelay", ParDo.of(new DoFn<Flight, KV<String, Double>>() {

          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            if (f.getField(Flight.INPUTCOLS.EVENT).equals("arrived")) {
              String key = f.getField(Flight.INPUTCOLS.DEST);
              double value = f.getFieldAsFloat(Flight.INPUTCOLS.ARR_DELAY);
              c.output(KV.of(key, value));
            }
          }

        })) //
        .apply("avgArrDelay", Mean.perKey());

    PCollectionView<Map<String, Double>> avgArrDelay = arrDelays.apply(name + "arrdelay->map", View.asMap());
    
    trainFlights = trainFlights.apply(name + "AddDelayInfo", ParDo.withSideInputs(avgDepDelay, avgArrDelay).of(new DoFn<Flight, Flight>() {

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Flight f = c.element().newCopy();
        String depKey = f.getField(Flight.INPUTCOLS.ORIGIN) + ":" + f.getDepartureHour();
        Double depDelay = c.sideInput(avgDepDelay).get(depKey);
        String arrKey = f.getField(Flight.INPUTCOLS.DEST);
        Double arrDelay = c.sideInput(avgArrDelay).get(arrKey);
        f.avgDepartureDelay = (float) ((depDelay == null) ? 0 : depDelay);
        f.avgArrivalDelay = (float) ((arrDelay == null) ? 0 : arrDelay);
        c.output(f);
      }

    }));
   
    trainFlights.apply(name + "ToCsv", ParDo.of(new DoFn<Flight, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Flight f = c.element();
        if (f.getField(INPUTCOLS.EVENT).equals("arrived")) {
          c.output(f.toTrainingCsv());
        }
      }
    })) //
        .apply("WriteFlights", TextIO.Write.to(options.getOutput() + name + "flights").withSuffix(".csv"));
  }
  
  @SuppressWarnings("serial")
  private static PCollection<Flight> filterTrainOrTest(String name, PCollection<Flight> allFlights, PCollectionView<Map<String, String>> traindays, boolean trainOnly){
    return allFlights.apply(name, ParDo.withSideInputs(traindays).of(new DoFn<Flight, Flight>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Flight f = c.element();
        String date = f.getField(Flight.INPUTCOLS.FL_DATE);
        boolean isTrainDay = c.sideInput(traindays).containsKey(date);
        if (isTrainDay == trainOnly) {
          c.output(f); // training days only
        }
      }
    }));
  }

  @SuppressWarnings("serial")
  private static PCollectionView<Map<String, String>> getTrainDays(Pipeline p, String path) {
    return p.apply("Read trainday.csv", TextIO.Read.from(path)) //
        .apply("Parse trainday.csv", ParDo.of(new DoFn<String, KV<String, String>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            String line = c.element();
            String[] fields = line.split(",");
            if (fields.length > 1 && "True".equals(fields[1])) {
              c.output(KV.of(fields[0], "")); // ignore value
            }
          }
        })) //
        .apply("toView", View.asMap());
  }
}
