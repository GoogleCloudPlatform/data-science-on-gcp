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
* Edit the PROJECT in to reflect your project.
* You can now click on any Java file with a `main()` (e.g: `FlightsMLService.java`) and select Run As | Java Application.


### Invoking prediction service
* Edit the PROJECT setting in:
  `10_realtime/chapter10/src/main/java/com/google/cloud/training/flights/FlightsMLService.java`
* In CloudShell, try out the service using Maven:
  ```
     cd data-science-on-gcp/10_realtime/chapter10
     mvn compile exec:java -Dexec.mainClass=com.google.cloud.training.flights.FlightsMLService
  ```
      
### Run streaming pipeline to add predictions to flight information
In CloudShell:
* Create a Bigtable instance:
  ```
     ./create_cbt.sh
  ```  
* Install the necessary packages and provide the simulation code authorization to access your BigQuery dataset:
  ```
     cd data-science-on-gcp/10_realtime
     ../04_streaming/simulate/install_packages.sh
     gcloud auth application-default login
  ```  
* Start the simulation:
  ```
     ./simulate.sh
  ``` 
* Open a second CloudShell tab, and authenticate again within this tab:
  ```
     cd data-science-on-gcp/10_realtime
     gcloud auth application-default login
  ``` 
* In the second CloudShell tab, launch the Dataflow pipeline:
  ```
     ./predict.sh bucket-name project-name
  ```  
* Visit https://console.cloud.google.com/dataflow to monitor the running pipeline.
  If the pipeline fails due to insufficient quota, go to https://cloud.google.com/compute/quotas and
  clean up all unused resources, and if that still isn't enough, request the appropriate quota increase.
  Because this is a real-time pipeline that streams into
  Bigtable and BigQuery, you might not be able to do it on the free tier.
  Once you see elements being written into BigQuery and Bigtable, go to next step.

* Query Bigtable:
* Query BigQuery:
* <b>Important</b> Cleanup by deleting your Bigtable instance:
  ```
     ./delete_cbt.sh
  ```

### Evaluate model performance
