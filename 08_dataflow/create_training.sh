OUTDIR=gs://cloud-training-demos-ml/flights/chapter8/trainingdata
mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.training.flights.CreateTrainingDataset \
      -Dexec.args="--fullDataset --maxNumWorkers=100 \
                   --autoscalingAlgorithm=THROUGHPUT_BASED \
                   --stagingLocation=$OUTDIR \
                   --output=$OUTDIR"

