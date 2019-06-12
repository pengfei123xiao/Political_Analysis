# Political_Analysis

This is our capstone project harvester and analyser repository.
```
├── analyser               twitter analysis scripts
├── data                   list of politicians' screen name, polygon file and election results
├── restful_harvester      different restful harvesters
├── streaming_harvester    different streaming harvesters
├── README.md
├── config.py              configs including keys for Twiiter API Authentication and database   
└── requirements.txt       python dependencies
```


Pre-Requisite:
  - python 3.7+
  - virtualenv 16+

## Install dependencies
1. git clone the repo: **git clone https://github.com/pengfei123xiao/Political_Analysis.git**
2. install 'virtualenv' package. **sudo pip3 install virtualenv**
3. create a virtual environment: **virtualenv venv**
4. go into the virtual environment: for windows user: **venv\Scripts\activate**; for Mac/Linux user: **source venv/bin/activate**
5. run **sudo pip3 install -r requirements.txt** (to install all dependencies)
6. leave venv via: **deactivate**

## Steps to run
1. run streaming harvester: python3 streaming_by_mentioned.py 8
2. run restful harvester: python3 harvester_manager.py
3. launch automatic analyser: python3 analysis_crob_jobs.py
4. launch mongodb backup job: python3 mongodb_backup_crob_jobs.py

Notice: This repository offers data to the web application in:
https://github.com/sche0025/PoliticalCorrectness
