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
    @Default.Boolean(false)
    boolean isRealtime();
    void setRealtime(boolean r);
    
    @Description("Simulation speedup factor if applicable")
    @Default.Long(1)
    long getSpeedupFactor();

    void setSpeedupFactor(long d);
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
    Duration averagingInterval = CreateTrainingDataset.AVERAGING_INTERVAL;
    Duration averagingFrequency = CreateTrainingDataset.AVERAGING_FREQUENCY;
    if (options.isRealtime()) {
      io = new PubSubBigQuery();
      // If we need to average over 60 minutes and speedup is 30x,
      // then we need to average over 2 minutes of sped-up stream
      averagingInterval = averagingInterval.dividedBy(options.getSpeedupFactor());
      averagingFrequency = averagingFrequency.dividedBy(options.getSpeedupFactor());
    } else {
      io = new BatchInputOutput();
    } 
    
    PCollection<Flight> allFlights = io.readFlights(p, options);

    PCollectionView<Map<String, Double>> avgDepDelay = readAverageDepartureDelay(p, options.getDelayPath());

    PCollection<Flight> hourlyFlights = allFlights.apply(Window.<Flight> into(SlidingWindows//
        .of(averagingInterval)//
        .every(averagingFrequency)));

    PCollection<KV<String, Double>> avgArrDelay = CreateTrainingDataset.computeAverageArrivalDelay(hourlyFlights);

    hourlyFlights = CreateTrainingDataset.addDelayInformation(hourlyFlights, avgDepDelay, avgArrDelay, averagingFrequency);

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
    private static class CreateBatch extends DoFn<Flight, KV<String, Flight>> {
      private static final int NUM_BATCHES = 2;
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
    }
    public PCollection<FlightPred> addPredictionInBatches(PCollection<Flight> outFlights) {
      
      PCollection<FlightPred> lines = outFlights //
          .apply("Batch->Flight", ParDo.of(new CreateBatch())) //
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
              double[] result = FlightsMLService.batchPredict(flights, -5);
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
    private static final String BQ_TABLE_NAME = "flights.predictions";
    
//    private SerializableFunction<ValueInSingleWindow, String> getTableNameFunction() {
//      return new SerializableFunction<ValueInSingleWindow, String>() {
//                  public String apply(ValueInSingleWindow value) {
//                     // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.
//                     String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
//                          .withZone(DateTimeZone.UTC)
//                          .print(((IntervalWindow) value.getWindow()).start());
//                     return BQ_TABLE_NAME + "_" + dayString;
//                   }
//      }
//    }
    
    @Override
    public PCollection<Flight> readFlights(Pipeline p, MyOptions options) {
      // return readFlights_flatten(p, options);
      return readFlights_workaround(p, options);
    }
    
    // cleaner implementation, but doesn't work because of b/34884809
    @SuppressWarnings("unused")
    private PCollection<Flight> readFlights_flatten(Pipeline p, MyOptions options) {
      // empty collection to start
      PCollectionList<Flight> pcs = PCollectionList.empty(p);
      // read flights from each of two topics
      for (String eventType : new String[]{"wheelsoff", "arrived"}){
        String topic = "projects/" + options.getProject() + "/topics/" + eventType;
        PCollection<Flight> flights = p.apply(eventType + ":read",
            PubsubIO.<String> read().topic(topic).withCoder(StringUtf8Coder.of())/*.timestampLabel("EventTimeStamp")*/) //
            .apply(eventType + ":parse", ParDo.of(new DoFn<String, Flight>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                String line = c.element();
                Flight f = Flight.fromCsv(line);
                if (f != null) {
                  c.output(f);
                }
              }
            }));
        pcs = pcs.and(flights);
      }
      // flatten collection
      return pcs.apply(Flatten.<Flight>pCollections());
    }
   
    // workaround  b/34884809 by streaming to a new topic and reading from it.
    public PCollection<Flight> readFlights_workaround(Pipeline p, MyOptions options) {
      String tempTopic = "projects/" + options.getProject() + "/topics/dataflow_temp";
      
      // read flights from each of two topics, and write to combined
      for (String eventType : new String[]{"wheelsoff", "arrived"}){
        String topic = "projects/" + options.getProject() + "/topics/" + eventType;
        p.apply(eventType + ":read", //
            PubsubIO.<String> read().topic(topic).withCoder(StringUtf8Coder.of())) //
            .apply(eventType + ":write", PubsubIO.<String> write().topic(tempTopic).withCoder(StringUtf8Coder.of()));
      }
      
      return p.apply("combined:read",
          PubsubIO.<String> read().topic(tempTopic).withCoder(StringUtf8Coder.of())) //
          .apply("parse", ParDo.of(new DoFn<String, Flight>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              String line = c.element();
              Flight f = Flight.fromCsv(line);
              if (f != null) {
                c.output(f);
              }
            }
          }));
    } 
  
    @Override
    public void writeFlights(PCollection<Flight> outFlights, MyOptions options) {
      String outputTable = options.getProject() + ':' + BQ_TABLE_NAME;
      TableSchema schema = new TableSchema().setFields(getTableFields());
      PCollection<FlightPred> preds = addPredictionInBatches(outFlights);
      PCollection<TableRow> rows = toTableRows(preds);
      rows.apply("flights:write_toBQ",BigQueryIO.Write.to(outputTable) //
          .withSchema(schema)//
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    }

    private PCollection<TableRow> toTableRows(PCollection<FlightPred> preds) {
      return preds.apply("pred->row", ParDo.of(new DoFn<FlightPred, TableRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          FlightPred pred = c.element();
          TableRow row = new TableRow();
          for (int i=0; i < types.length; ++i) {
            INPUTCOLS col = INPUTCOLS.values()[i];
            String name = col.name();
            String value = pred.flight.getField(col);
            if (value.length() > 0) {
              if (types[i].equals("FLOAT")) {
                row.set(name, Float.parseFloat(value));
              } else {
                row.set(name, value);
              }
            }
          }
          if (pred.ontime >= 0) {
            row.set("ontime", Math.round(pred.ontime*100)/100.0);
          }
          c.output(row);
        }})) //
;
    }

    private String[] types;
    public PubSubBigQuery() {
      this.types = new String[INPUTCOLS.values().length];   
      String[] floatPatterns = new String[] {"LAT", "LON", "DELAY", "DISTANCE", "TAXI"};
      String[] timePatterns  = new String[] {"TIME", "WHEELS" };
      for (int i=0; i < types.length; ++i) {
        String name = INPUTCOLS.values()[i].name();
        types[i] = "STRING";
        for (String pattern : floatPatterns) {
          if (name.contains(pattern)) {
            types[i] = "FLOAT";
          }
        }
        for (String pattern : timePatterns) {
          if (name.contains(pattern)) {
            types[i] = "TIMESTAMP";
          }
        }
      }
    }
    
    private List<TableFieldSchema> getTableFields() {
      List<TableFieldSchema> fields = new ArrayList<>();
      for (int i=0; i < types.length; ++i) {
        String name = INPUTCOLS.values()[i].name();
        fields.add(new TableFieldSchema().setName(name).setType(types[i]));
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
        PCollection<FlightPred> prds = addPredictionInBatches(outFlights);
        PCollection<String> lines = predToCsv(prds);
        lines.apply("Write", TextIO.Write.to(options.getOutput() + "flightPreds").withSuffix(".csv"));
      } catch (Throwable t) {
        LOG.warn("Inference failed", t);
      }
    }

    private PCollection<String> predToCsv(PCollection<FlightPred> preds) {
      return preds.apply("pred->csv", ParDo.of(new DoFn<FlightPred, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          FlightPred pred = c.element();
          String csv = String.join(",", pred.flight.getFields());
          if (pred.ontime >= 0) {
            csv = csv + "," + new DecimalFormat("0.00").format(pred.ontime);
          } else {
            csv = csv + ","; // empty string -> null
          }
          c.output(csv);
        }})) //
;
    }
  }

  @SuppressWarnings("unused")
  private static PCollection<FlightPred> addPredictionOneByOne(PCollection<Flight> outFlights) {
    return outFlights //
        .apply("Inference", ParDo.of(new DoFn<Flight, FlightPred>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            double ontime = -5;
            if (f.isNotCancelled() && f.isNotDiverted()) {
              if (f.getField(INPUTCOLS.EVENT).equals("arrived")) {
                // actual ontime performance
                ontime = f.getFieldAsFloat(INPUTCOLS.ARR_DELAY, 0) < 15 ? 1 : 0;
              } else {
                // wheelsoff: predict ontime arrival probability
                ontime = FlightsMLService.predictOntimeProbability(f, -5.0);
              }
            }
            c.output(new FlightPred(f, ontime));
          }
        }));
  }
}
