# Churn Data Analytics Data Pipeline 

## Project Overview 
This project involves a robust ETL pipeline that starts with data stored in Amazon RDS, primarily serving OLTP purposes. The data is then transferred to Amazon S3, which acts as an intermediate storage layer. To facilitate efficient querying, AWS Glue Crawler is employed to scan the data in S3 and automatically generate metadata in the Glue Data Catalog. This metadata can be leveraged for querying the data directly from S3 using Amazon Athena or Amazon Redshift.

In this architecture, Amazon S3 acts as a staging area, allowing for the creation of an external schema in Amazon Redshift. This setup enables seamless access to the data in S3 through Redshift, where it is organized into landing tables. From these landing tables, dimension and fact tables are subsequently generated, forming the backbone of the data warehouse.

Apache Airflow plays a critical role in orchestrating the entire workflow. It automates the process of extracting data from RDS, loading it into S3, triggering the Glue Crawler to update the metadata, and executing the necessary SQL commands on Redshift.

The pipeline is designed to handle incremental loads, capturing only the data that has changed since the last ETL sync. This incremental approach ensures that the data warehouse is updated efficiently and in a timely manner, minimizing unnecessary data processing and enhancing overall performance.

By integrating these technologies—Amazon RDS, S3, Glue, Redshift, and Airflow—the project delivers a scalable and efficient data processing solution that supports advanced analytics and reporting.

**Dataset Link:** Telco customer churn: IBM dataset  
**Repository:** [Churn Data Analytics Data Pipeline](https://github.com/Sanjay-dev-ds/churn-data-analytics-data-pipeline.git)

## Prerequisites
- Apache Airflow
- Python
- AWS Cloud account 

## ETL Architecture
![etl.png](images%2Fetl.png)

## Airflow Workflow
![airflow.png](images%2Fairflow.png)

## Technologies Used
- AWS Glue
- AWS S3
- Apache Airflow
- Redshift Serverless
- AWS RDS

## Steps to Deploy the Project 

1. Clone the repository on AWS EC2
    ```bash
    git clone https://github.com/Sanjay-dev-ds/churn-data-analytics-data-pipeline.git
    ```
2. Set up all infrastructure
3. Run and invoke Airflow UI 
    ```bash
    airflow standalone
    ```
4. Test the project!

## Author
[Sanjay Jayakumar](https://www.linkedin.com/in/sanjayjayakumar)
