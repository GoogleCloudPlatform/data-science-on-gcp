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

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.training.flights.Flight.INPUTCOLS;

/**
 * Adds prediction to incoming flight information.
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
    
    @Description("If real-time, it will read incoming flight info from Pub/Sub")
    @Default.Boolean(true)
    boolean isRealtime();
    void setRealtime(boolean r);
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
  
  private static interface InputOutput extends Serializable {
    public abstract PCollection<Flight> readFlights(Pipeline p, MyOptions options);
    public abstract void writeFlights(PCollection<Flight> outFlights, MyOptions options);
  }
  
  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    if (options.isRealtime()) {
      options.setStreaming(true);
    }    
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://cloud-training-demos-ml/flights/staging");
    Pipeline p = Pipeline.create(options);

    // in real-time, we read from PubSub and write to BigQuery
    InputOutput io;
    if (options.isRealtime()) {
      io = new PubSubBigQuery();
    } else {
      io = new BatchInputOutput();
    } 
    
    PCollection<Flight> allFlights = io.readFlights(p, options);

    PCollectionView<Map<String, Double>> avgDepDelay = readAverageDepartureDelay(p, options.getDelayPath());

    PCollection<Flight> hourlyFlights = allFlights.apply(Window.<Flight> into(SlidingWindows//
        .of(Duration.standardHours(1))//
        .every(Duration.standardMinutes(5)))); // .discardingFiredPanes());

    PCollection<KV<String, Double>> avgArrDelay = CreateTrainingDataset.computeAverageArrivalDelay(hourlyFlights);

    hourlyFlights = CreateTrainingDataset.addDelayInformation(hourlyFlights, avgDepDelay, avgArrDelay);

    io.writeFlights(hourlyFlights, options);

    PipelineResult result = p.run();
    if (!options.isRealtime()) {
      result.waitUntilFinish();
    }
  }
  
  @DefaultCoder(AvroCoder.class)
  private static class FlightPred {
    Flight flight;
    double ontime;
    @SuppressWarnings("unused")
    FlightPred(){}
    FlightPred(Flight f, double ontime) {
      this.flight = f;
      this.ontime = ontime;
    }
  }
  
  private static abstract class InputOutputHelper implements InputOutput {
    public PCollection<FlightPred> addPredictionInBatches(PCollection<Flight> outFlights) {
      final int NUM_BATCHES = 2;
      PCollection<FlightPred> lines = outFlights //
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
          .apply("Inference", ParDo.of(new DoFn<KV<String, Iterable<Flight>>, FlightPred>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              String key = c.element().getKey();
              Iterable<Flight> flights = c.element().getValue();
               
              // write out all the ignored events as-is
              if (key.startsWith("ignored")) {
                for (Flight f : flights) {
                  c.output(new FlightPred(f, -1));
                }
                return;
              }

              // for arrived events, emit actual ontime performance
              if (key.startsWith("arrived")) {
                for (Flight f : flights) {
                  double ontime = f.getFieldAsFloat(INPUTCOLS.ARR_DELAY, 0) < 15 ? 1 : 0;
                  c.output(new FlightPred(f, ontime));
                }
                return;
              }

              // do ml inference for wheelsoff events, but as batch
              FlightsMLService.Request request = new FlightsMLService.Request();
              for (Flight f : flights) {
                request.instances.add(new FlightsMLService.Instance(f));
              }
              FlightsMLService.Response resp = FlightsMLService.sendRequest(request);
              double[] result = resp.getOntimeProbability(-5);
              // append probability
              int resultno = 0;
              for (Flight f : flights) {
                double ontime = result[resultno++];
                c.output(new FlightPred(f, ontime));
              }
            }
          }));
      return lines;
    }
  }
  
  private static class PubSubBigQuery extends InputOutputHelper {

    @Override
    public PCollection<Flight> readFlights(Pipeline p, MyOptions options) {
      // empty collection to start
      PCollectionList<Flight> pcs = PCollectionList.empty(p);
      // read flights from each of two topics
      for (String eventType : new String[]{"wheelsoff", "arrived"}){
        String topic = "projects/" + options.getProject() + "/topics/" + eventType;
        PCollection<Flight> flights = p.apply(eventType + ":read",
            PubsubIO.<String> read().topic(topic).withCoder(StringUtf8Coder.of())) //
            .apply(eventType + ":parse", ParDo.of(new DoFn<String, Flight>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                String line = c.element();
                Flight f = Flight.fromCsv(line);
                c.output(f);
              }
            }));
        pcs = pcs.and(flights);
      }
      // flatten collection
      return pcs.apply(Flatten.<Flight>pCollections());
    }
  
    @Override
    public void writeFlights(PCollection<Flight> outFlights, MyOptions options) {
      String outputTable = options.getProject() + ':' + "flights.predictions";
      TableSchema schema = new TableSchema().setFields(getTableFields());
      PCollection<FlightPred> preds = addPredictionInBatches(outFlights);
      preds.apply("pred->row", ParDo.of(new DoFn<FlightPred, TableRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          FlightPred pred = c.element();
          TableRow row = new TableRow();
          for (INPUTCOLS col : INPUTCOLS.values()) {
            String name = col.name();
            row.set(name, pred.flight.getField(col));
          }
          row.set("ontime", Math.round(pred.ontime*100)/100.0);
          c.output(row);
        }})) //
     .apply("flights:write_toBQ",BigQueryIO.Write.to(outputTable) //
          .withSchema(schema)//
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    }

    private List<TableFieldSchema> getTableFields() {
      List<TableFieldSchema> fields = new ArrayList<>();
      String[] floatPatterns = new String[] {"LAT", "LON", "DELAY", "DISTANCE", "TAXI"};
      String[] timePatterns  = new String[] {"TIME", "WHEELS" };
      for (INPUTCOLS col : INPUTCOLS.values()) {
        String name = col.name();
        String type = "STRING";
        for (String pattern : floatPatterns) {
          if (name.contains(pattern)) {
            type = "FLOAT";
          }
        }
        for (String pattern : timePatterns) {
          if (name.contains(pattern)) {
            type = "TIMESTAMP";
          }
        }
        fields.add(new TableFieldSchema().setName(name).setType(type));
      }
      fields.add(new TableFieldSchema().setName("ontime").setType("FLOAT"));
      return fields;
    }    
  }
  
  private static class BatchInputOutput extends InputOutputHelper {

    @Override
    public PCollection<Flight> readFlights(Pipeline p, MyOptions options) {
      String query = "SELECT EVENT_DATA FROM flights.simevents WHERE ";
      query += " STRING(FL_DATE) = '2015-01-04' AND ";
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
          }));
      return allFlights;
    }

    @Override
    public void writeFlights(PCollection<Flight> outFlights, MyOptions options) {
      // PCollection<String> lines = addPredictionOneByOne(outFlights);
      try {
        PCollection<FlightPred> lines = addPredictionInBatches(outFlights);
        lines.apply("pred->csv", ParDo.of(new DoFn<FlightPred, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            FlightPred pred = c.element();
            String csv = String.join(",", pred.flight.getFields());
            csv = csv + "," + new DecimalFormat("0.00").format(pred.ontime);
            c.output(csv);
          }})) //
        .apply("Write", TextIO.Write.to(options.getOutput() + "flightPreds").withSuffix(".csv"));
      } catch (Throwable t) {
        LOG.warn("Inference failed", t);
      }
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
}
