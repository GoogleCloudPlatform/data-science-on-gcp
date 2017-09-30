# Machine Learning Classifier using TensorFlow

In CloudShell, or in your local development machine:
* Create a small sample of the full dataset you created in Chapter 8 in ~/data:
    ```
    cd data-science-on-gcp/09_cloudml
    ./create_small.sh bucket-name
    ```
* If in CloudShell, enable Boost mode. Then, install tensorflow:
    ```
    pip install tensorflow
    ```
* Train locally:
    ```
    ./train_local.sh
    ```
* To submit training job to Cloud ML Engine on subset of data:
    ```
    ./authorize_cmle.sh bucket-name
    ./retrain_cloud.sh bucket-name region-name
    ```
  The authorization command is needed to allow the Cloud ML Engine service to read
  and write to your bucket.
  The region name will be something like `us-central1`.  Go to the GCP console (
  https://console.cloud.google.com/mlengine) to monitor job progress. This will take
  10-15 minutes.
* Submit training job to Cloud ML Engine to train on the full dataset:
    ```
    ./train_cloud.sh bucket-name region-name
    ```
  This will take about 1.5 hours.
* Deploy the trained model to Cloud ML Engine:
    ```
    ./deploy_model.sh bucket-name region-name
    ```
  This will take 3-5 minutes.
* Get the model to predict:
    ```
    ./call_predict.py --project=$(gcloud config get-value core/project)
    ```
* Get the model to predict, but also provide a reason:
    ```
    ./call_predict_reason.py --project=$(gcloud config get-value core/project)
    ```
