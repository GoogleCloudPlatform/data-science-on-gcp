# 10. Real-time machine learning

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
  Browse to, and select `data-science-on-gcp/10_realtime/chapter10/pom.xml`
* You can now click on any Java file with a `main()` (e.g: `EvaluateModel.java`) and select Run As | Java Application.

### Run Dataflow from command-line to create augmented dataset for machine learning
To run the Dataflow pipelines from CloudShell:
* If you haven't already done so, git clone the repository.
* Launch a Dataflow pipeline (it will take 20-25 minutes):
    ```
    cd data-science-on-gcp/08_dataflow
    ./create_datasets.sh bucket-name  max-num-workers
    ```
* Visit https://console.cloud.google.com/dataflow to monitor the running pipeline.
* Once pipeline is finished, go to https://console.cloud.google.com/dataflow to see that you now have new data.
