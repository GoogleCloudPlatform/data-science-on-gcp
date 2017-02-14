mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.training.flights.CreateTrainingDataset2 \
      -Dexec.args="--project=cloud-training-demos \
      --stagingLocation=gs://cloud-training-demos-ml/staging/ \
      --input=gs://cloud-training-demos-ml/flights/chapter8/small.csv \
      --output=gs://cloud-training-demos-ml/flights/chapter8/output/ \
      --runner=DataflowRunner"
