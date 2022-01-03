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

* Open a new notebook in Vertex AI Workbench from https://console.cloud.google.com/vertex-ai/workbench
* Launch a new terminal window and type in it:
```
git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
```
* In the navigation pane on the left, navigate to data-science-on-gcp/09_vertexai
* Open the notebook flights_model_tf2.ipynb and run the cells.  Note that the notebook has
DEVELOP_MODE=True and so it will train on a very, very small amount of data. This is just
to make sure the code works.


## Articles
Some of the content in this chapter was published as blog posts (links below).

* [Giving Vertex AI, the New Unified ML Platform on Google Cloud, a Spin](https://towardsdatascience.com/giving-vertex-ai-the-new-unified-ml-platform-on-google-cloud-a-spin-35e0f3852f25):
Why do we need it, how good is the code-free ML training, really, and what does all this mean for data science jobs?
* [How to Deploy a TensorFlow Model to Vertex AI](https://towardsdatascience.com/how-to-deploy-a-tensorflow-model-to-vertex-ai-87d9ae1df56): Working with saved models and endpoints in Vertex AI


