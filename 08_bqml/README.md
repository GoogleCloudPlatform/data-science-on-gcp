# 8. Machine Learning with BigQuery ML

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
* In the navigation pane on the left, navigate to data-science-on-gcp/08_bqml

### Logistic regression using BigQuery ML
* Open the notebook bqml_logistic.ipynb
* Edit | Clear All Outputs
* Run through the cells one-by-one, reading the commentary, looking at the code, and examining the output.
* Close the notebook
* Click on the square icon on the left-most bar to view Running Terminals and Notebooks
* Stop the notebook

### Other notebooks
* Repeat the steps above for the following notebooks (in order)
  * bqml_nonlinear.ipynb
  * bqml_timewindow.ipynb
  * bqml_timetxf.ipynb
  
