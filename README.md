# Data Engineering Nanodegree
## Project: Data Modeling with Postgres
## Table of Contents
* **Definition**
    * **Project Overview** :
    Sparkify is a music app, they wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
    Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
    
    * **Problem Statement** : 
       Sparkify Analytics team is particularly interested in understanding what songs users are listening to.
* **Design**
    **Schema Design** : 
    I have built the model on a star schema optimized for queries on song play analysis. I have defined one fact table (songplays)  and dimension tables (users,songs, artists  and times). Below is the star schema. ![Sparkify Star Schema](https://github.com/ddgope/Data-Modeling-with-Postgres/blob/master/StarModel.JPG)
* **ETL Design**
    * **Create Tables** : 
        This will create all the required table. If the table exists, it will drop and recreate. create and drop queries are available in sql_queries.py file.
    * **ETL Process** : 
        Before running ETL pipeline, I need to make sure that the songs and log files both are valid files. Following data quality I have validated
        1. Data validation.
        1. Constraint validation: ensuring the constraints for tables are properly defined.
        1. Data completeness: checking all expected data has been loaded. For e.g. verify any required column is having either Null or Empty value.
        1. Data correctness: making sure the data has been accurately recorded. For e.g. verify if any duplicates record exists or not.
        1. Validating dates e.g. ts timestamp column should be integer and should be able to convert into datetime.
        1. Data cleanliness: removing unnecessary columns.
        1. Either song or log file content is empty, then reject the files and send message that invalid file.
        
    * **ETL Pipeline** :
    Once you run the pipeline, it will collect, process, and store into tables. Once it is processed, you can get timely insights and react quickly to new information. Below is three step process
        1. Table Creation: Go to the Terminal, type python create_tables.py, which will create the database and tables.
        1. Execute ETL PipeLine : Go to the Terminal, type python etl.py- which will process the entire datasets, redaing from the songs and log data and store into postgress db.
        1. Verification: open the test.ipynb and verify that records were successfully inserted into each table.   

* **How to Run** : Open the terminal, type as below
    1. python create_tables.py
    1. python etl.py
    1. analysis.ipynb - run you all analysis
    
* **Final Result / Analysis** : Now Sparkify Analytics team can run multiple queries     
        1. Currently how many users are listening songs ?
        1. How the users are distributes across the geography ?
        1. Which are the songs they are playing ?  

* **Software Requirements** : This project uses the following software and Python libraries:
        1. Python 3.0
        1. psycopg2
        1. PostgreSQL
        
    You will also need to have software installed to run and execute a Jupyter Notebook.
    If you do not have Python installed yet, it is highly recommended that you install the Anaconda distribution of Python, which already has the above packages and more included.    

* **Acknowledgement** : Must give credit to Udacity for the project. You can't use this for you Udacity capstone project. Otherwise, feel free to use the code here as you would like!

