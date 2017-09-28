# 7. Machine Learning: Logistic regression on Spark

### [Optional] Set up Datalab and Dataproc
* Use the instructions in the <a href="../06_dataproc/README.md">Chapter 6 README</a> to:
  * launch a minimal Cloud Dataproc cluster with initialization actions for Datalab (`./create_cluster.sh BUCKET ZONE`)
  * start a SSH tunnel (`./start_tunnel.sh`),
  * a Chrome session via the network proxy (`./start_chrome.sh`),
  * and browse to http://ch6cluster-m:8080/.

* Start a new notebook and in a cell, download a read-only clone of this repository:
    ```
    %bash
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    rm -rf data-science-on-gcp/.git
    ```
* Browse to http://ch6cluster-m:8080/notebooks/datalab/data-science-on-gcp/07_sparkml/logistic_regression.ipynb
  and run the cells in the notebook (change the BUCKET appropriately).

### Logistic regression using Spark
* If you haven't already done so, launch a minimal Dataproc cluster:
    ```
    cd ~/data-science-on-gcp/06_dataproc
    ./create_cluster.sh BUCKET ZONE
    ```
* Submit a Spark job to run the full dataset (change the BUCKET appropriately).
    ```
    cd ~/data-science-on-gcp/07_sparkml
    ../06_dataproc/increase_cluster.sh
    ./submit_spark.sh BUCKET logistic.py
    ```

### Feature engineering
* Submit a Spark job to do experimentation: `./submit_spark.sh BUCKET experiment.py`

### Cleanup
* Delete the cluster either from the GCP web console or by typing in CloudShell, `../06_dataproc/delete_cluster.sh`
