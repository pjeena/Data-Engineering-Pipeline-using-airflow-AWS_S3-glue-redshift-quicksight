# Building a ETL pipeline using airflow, S3, glue, redshift and quicksight

* Scraped [data](https://github.com/pjeena/Data-Engineering-Pipeline-using-airflow-AWS_S3-glue-redshift-quicksight/blob/main/extract_recipes_etl.py) from [allrecipes](https://www.allrecipes.com/) using python
* Deployed the code on [Airflow](https://github.com/pjeena/Data-Engineering-Pipeline-using-airflow-AWS_S3-glue-redshift-quicksight/blob/main/extract_recipes_dag.py) on AWS EC2 instance to monitor workflows
* Saved scraped data in AWS S3 and [cleaned](https://github.com/pjeena/Data-Engineering-Pipeline-using-airflow-AWS_S3-glue-redshift-quicksight/blob/main/preprocessing_and_cleaning.py) it.
* Launched AWS Glue job to migrate data from S3 to redshift
* Wrote SQL queries to query data and visualize in Amazon [Quicksight](https://github.com/pjeena/Data-Engineering-Pipeline-using-airflow-AWS_S3-glue-redshift-quicksight/blob/main/Quicksight_allrecipes_visual.jpg).


## Dashboard

![quicsight](https://github.com/pjeena/Data-Engineering-Pipeline-using-airflow-AWS_glue-redshift-athena-quicksight/blob/main/Quicksight_allrecipes_visual.jpg)


# Future Work

* Persiinalized Recipe recommender system based on food nutrients could be developed.
* Sentiment analysis of the reviews on recipe websites.
* Images can be collected from different recipes and a food recognition system could be developed showing nutritional values.
