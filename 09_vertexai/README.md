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
 
## This Chapter

### Vertex AI Workbench
* Open a new notebook in Vertex AI Workbench from https://console.cloud.google.com/vertex-ai/workbench
* Launch a new terminal window and type in it:
```
git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
```
* In the navigation pane on the left, navigate to data-science-on-gcp/09_vertexai
* Open the notebook flights_model_tf2.ipynb and run the cells.  Note that the notebook has
DEVELOP_MODE=True and so it will train on a very, very small amount of data. This is just
to make sure the code works.


#### From CloudShell
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
    ./call_predict.sh
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
* Get the model to predict, but also provide a reason:
    ```
    ./call_predict_reason.py --project=$(gcloud config get-value core/project)
    ```


## Articles
Some of the content in this chapter was published as blog posts (links below).

To try out the code in the articles without going through the chapter, copy the necessary data to your bucket:
  ```
  gsutil cp gs://data-science-on-gcp/edition2/ch9/data/all.csv gs://BUCKET/ch9/data/all.csv
```

Now you will be able to run model.py and train_on_vertexai.py as in the directions above.

* [Giving Vertex AI, the New Unified ML Platform on Google Cloud, a Spin](https://towardsdatascience.com/giving-vertex-ai-the-new-unified-ml-platform-on-google-cloud-a-spin-35e0f3852f25):
Why do we need it, how good is the code-free ML training, really, and what does all this mean for data science jobs?
* [How to Deploy a TensorFlow Model to Vertex AI](https://towardsdatascience.com/how-to-deploy-a-tensorflow-model-to-vertex-ai-87d9ae1df56): Working with saved models and endpoints in Vertex AI
* [Developing and Deploying a Machine Learning Model on Vertex AI using Python](https://medium.com/@lakshmanok/developing-and-deploying-a-machine-learning-model-on-vertex-ai-using-python-865b535814f8): Write training pipelines that will make your MLOps team happy
* [How to build an MLOps pipeline for hyperparameter tuning in Vertex AI](https://lakshmanok.medium.com/how-to-build-an-mlops-pipeline-for-hyperparameter-tuning-in-vertex-ai-45cc2faf4ff5):
Best practices to set up your model and orchestrator for hyperparameter tuning


