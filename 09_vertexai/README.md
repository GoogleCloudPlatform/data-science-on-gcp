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
DEVELOP_MODE=True and so it will take on a very, very small amount of data. This is just
to make sure the code works.


#### From CloudShell
* Install the aiplatform library
    ```
    pip3 install google-cloud-aiplatform
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
* Get the model to predict, but also provide a reason:
    ```
    ./call_predict_reason.py --project=$(gcloud config get-value core/project)
    ```
