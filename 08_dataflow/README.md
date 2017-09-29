# 8. Time-windowed aggregate features

### [optional] Setup Dataflow Development environment
To develop Apache Beam pipelines in Eclipse:
* Install the latest version of the Java SDK (not just the runtime) from http://www.java.com/
* Install Maven from http://maven.apache.org/
* Install Eclipse for Java Developers from http://www.eclipse.org/
* If you have not already done so, git clone the respository:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Open Eclipse and do File | Import | From existing Maven files.
  Browse to, and select `data-science-on-gcp/08_dataflow/chapter8/pom.xml`
* You can now click on any Java file with a `main()` (e.g: `CreateTrainingDataset1.java`) and select Run As | Java Application.
  Note that you might have to change paths (replace vlakshmanan by your user name, and cloud-training-demos-ml by your bucket).

### Run Dataflow from command-line to create augmented dataset for machine learning
To run the Dataflow pipelines from CloudShell:
* If you haven't already done so, git clone the repository.
* Launch a Dataflow pipeline:
    ```
    cd data-science-on-gcp/08_dataflow
    ./create_datasets.sh bucket-name  max-num-workers
    ```
* Visit https://console.cloud.google.com/dataflow to monitor the running pipeline.
* Once pipeline is finished, go to https://console.cloud.google.com/dataflow to see that you now have new data.
* Load the augmented data into BigQuery also:
    ```
    ./to_bq.sh bucket-name
    ```
