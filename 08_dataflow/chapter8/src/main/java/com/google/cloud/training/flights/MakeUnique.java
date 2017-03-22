/*
 * Copyright (C) 2017 Google Inc.
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

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Write.Bound;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A dataflow pipeline that removes repeated lines from training and test datasets.
 * 
 * @author vlakshmanan
 *
 */
public class MakeUnique {
  public static interface MyOptions extends DataflowPipelineOptions {
    @Description("Name of the output directory")
    @Default.String("gs://cloud-training-demos-ml/flights/chapter8/output/")
    String getOutput();

    void setOutput(String s);
    
    @Description("Name of the input directory")
    @Default.String("gs://cloud-training-demos-ml/flights/chapter8/output_windowed/")
    String getInput();

    void setInput(String s);
  }

  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://cloud-training-demos-ml/flights/staging");
    Pipeline p = Pipeline.create(options);
    
    String[] prefixes = new String[] {
        "trainFlights",
        "testFlights",
        "delays"
    };
    
    for (String prefix : prefixes) {
      PCollection<String> lines = p //
        .apply(prefix + "_read", TextIO.Read.from(options.getInput() + prefix + "*"));
      
      lines = makeUnique(prefix, lines);

      Bound<String> write = TextIO.Write.to(options.getOutput() + prefix).withSuffix(".csv");
      if (prefix.equals("delays")) {
        write = write.withoutSharding();
      }
      lines.apply(prefix + "_write", write);
    }
    
    p.run();
  }
  
  @SuppressWarnings("serial")
  public static <T> PCollection<T> makeUnique(String prefix, PCollection<T> flights) {
    
    // TODO: replace by flights.apply(RemoveDuplicates<T>.create())
    
    return flights //
        .apply(prefix + "_unique1", ParDo.of(new DoFn<T, KV<T, Integer>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(KV.of(c.element(), 1));
          }
        })) //
        .apply(prefix + "_unique2", Sum.integersPerKey()) //
        .apply(prefix + "_unique3", ParDo.of(new DoFn<KV<T, Integer>, T>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(c.element().getKey());
          }
        }));
  }
}
