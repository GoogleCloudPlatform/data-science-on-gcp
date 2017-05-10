package com.google.cloud.training.flights;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.training.flights.AddRealtimePrediction.MyOptions;
import com.google.cloud.training.flights.Flight.INPUTCOLS;
import com.google.protobuf.ByteString;

@SuppressWarnings("serial")
public class PubSubBigtable extends PubSubInput {
  private static final String INSTANCE = "flights";
  private static final String TABLE    = "predictions";

  @Override
  public void writeFlights(PCollection<Flight> outFlights, MyOptions options) {
    PCollection<FlightPred> preds = addPredictionInBatches(outFlights);
    BigtableOptions.Builder optionsBuilder = //
        new BigtableOptions.Builder()//
            .setProjectId(options.getProject()) //
            .setInstanceId(INSTANCE);
    createEmptyTable(options, optionsBuilder);
    PCollection<KV<ByteString, Iterable<Mutation>>> mutations = toMutations(preds);
    mutations.apply("write:cbt", //
        BigtableIO.write().withBigtableOptions(optionsBuilder).withTableId(TABLE));
  }

  private PCollection<KV<ByteString, Iterable<Mutation>>> toMutations(PCollection<FlightPred> preds) {
    return preds.apply("pred->mutation", ParDo.of(new DoFn<FlightPred, KV<ByteString, Iterable<Mutation>>>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        FlightPred pred = c.element();
        ByteString key = null;
        List<Mutation> mutations = new ArrayList<>();
        for (INPUTCOLS col : INPUTCOLS.values()) {
          setCell(mutations, col.name(), pred.flight.getField(col));
        }
        if (pred.ontime >= 0) {
          setCell(mutations, "ontime", new DecimalFormat("0.00").format(pred.ontime));
        }
        c.output(KV.of(key, mutations));
      }
    }));
  }

  private void setCell(List<Mutation> mutations, String cellName, String cellValue) {
    if (cellValue.length() > 0) {
      ByteString value = ByteString.copyFromUtf8(cellValue);
      Mutation m = //
          Mutation.newBuilder().setSetCell(//
              Mutation.SetCell.newBuilder().setValue(value).setFamilyName(cellName)//
          ).build();
      mutations.add(m);
    }
  }

  private void createEmptyTable(MyOptions options, BigtableOptions.Builder optionsBuilder) {
    Table.Builder tableBuilder = Table.newBuilder();
    for (INPUTCOLS col : INPUTCOLS.values()) {
      tableBuilder.putColumnFamilies(col.name(), ColumnFamily.newBuilder().build());
    }

    CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.newBuilder().setParent(INSTANCE)
        .setTableId(TABLE).setTable(tableBuilder.build());

    try (BigtableSession session = new BigtableSession(optionsBuilder
        .setCredentialOptions(CredentialOptions.credential(options.as(GcpOptions.class).getGcpCredential())).build())) {
      BigtableTableAdminClient tableAdminClient = session.getTableAdminClient();
      tableAdminClient.createTable(createTableRequestBuilder.build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
