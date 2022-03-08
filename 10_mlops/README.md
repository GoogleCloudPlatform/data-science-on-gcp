# Machine Learning Classifier using TensorFlow

### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-7, the simplest way to catch up is to copy data from my bucket:

#### Catch up from Chapters 2-7
* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Go to the 02_ingest folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 04_streaming folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 05_bqnotebook folder of the repo, run the script to load data into BigQuery:
	```
	bash create_trainday.sh <BUCKET-NAME>
	```
* In this (10_mlops) folder, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
    
## This Chapter

In CloudShell, do the following steps:

* Install the aiplatform library
    ```
    pip3 install google-cloud-aiplatform cloudml-hypertune kfp
    ```
* Try running the standalone model file on a small sample:
    ```
    python3 model.py  --bucket <bucket-name> --develop
    ```
* [Optional] Run a Vertex AI Pipeline on the small sample (will take about ten minutes):
    ```
    python3 train_on_vertexai.py --project <project> --bucket <bucket-name> --develop
    ```
* Train on the full dataset using Vertex AI:
    ```
    python3 train_on_vertexai.py --project <project> --bucket <bucket-name>
    ```
* Try calling the model using bash:
    ```
    cd ../09_vertexai
    bash ./call_predict.sh
    cd ../10_mlops
    ```
* Try calling the model using Python:
    ```
    python3 call_predict.py
    ```
* [Optional] Train an AutoML model using Vertex AI:
    ```
    python3 train_on_vertexai.py --project <project> --bucket <bucket-name> --automl
    ```
* [Optional] Hyperparameter tune the custom model using Vertex AI:
    ```
    python3 train_on_vertexai.py --project <project> --bucket <bucket-name> --num_hparam_trials 10
    ```


## Articles
Some of the content in this chapter was published as blog posts (links below).

To try out the code in the articles without going through the chapter, copy the necessary data to your bucket:
  ```
  gsutil cp gs://data-science-on-gcp/edition2/ch9/data/all.csv gs://BUCKET/ch9/data/all.csv
```

Now you will be able to run model.py and train_on_vertexai.py as in the directions above.

* [Developing and Deploying a Machine Learning Model on Vertex AI using Python](https://medium.com/@lakshmanok/developing-and-deploying-a-machine-learning-model-on-vertex-ai-using-python-865b535814f8): Write training pipelines that will make your MLOps team happy
* [How to build an MLOps pipeline for hyperparameter tuning in Vertex AI](https://lakshmanok.medium.com/how-to-build-an-mlops-pipeline-for-hyperparameter-tuning-in-vertex-ai-45cc2faf4ff5):
Best practices to set up your model and orchestrator for hyperparameter tuning


