# Amazon Product Review Analysis Data Pipeline

Amazon-Product-Review-Analysis-Data-Pipeline project aims to collect, process and analyze customer reviews of products 
on the Amazon platform. The data pipeline is responsible for collecting and ingesting the raw Review and Product data from 
Amazon S3 storage. After the data is processed and transformed, it is stored in a data warehouse. The data warehouse serves as a 
centralized repository that consolidates and organizes the review and product data for further analysis.

## Design Approach

There are four distinct layers in the data warehouse data pipeline:

#### 1-Sourcing Layer:
In the sourcing layer, Raw data is extracted from the sources and then loaded in the landing tables.

#### 2-Transformation layer:
In the transformation layer, raw data is deduplicated and transformed for loading into the data warehouse fact and dimension tables.

#### 3-Warehouse layer:
In the Warehouse layer, fact and related dimensions are updated and maintained.  

#### 4-Presentation Layer:

The presentation layer is responsible for delivering valuable insights and visualizations to end-users. It focuses on 
making the data warehouse data easily accessible, understandable, and actionable for users across the organization.


## Architectural Design Flow
![Architecture Diagram](https://github.com/tarvihafatima/Amazon_Product_Review_Analysis/assets/26660037/1f162f56-7c68-40a4-a957-acf5607735b5)



## Technologies Used

#### ETL (Extract, Transform, Load) Tools: 
Apache Spark, Pyspark and Python scripts are used for data extraction from Amazon S3,data transformations and data loading into landing tables.

#### Landing Table: 
A Postgresql database system is used to create a landing table for data storage.

#### Data Warehouse:
Postgresql database and Postgresql user defined functions are utilized for creating and managing the data warehouse.

#### Scheduling framework:
Talend Open Studio has been utilized to create and schedule the flow of the pipeline in such a way that can support multiple runs a day in an incremental fashion. 

#### Note:
Google Bigquery(Column-oriented database) for data warehouse storage and Airflow for scheduling framework were the preferred choice of technologies.
However, due to hardware,platform and cost limitations above technologies were used for the scope of this project.  


## Dimensional Model

![Product Reviews - Dimensional Model](https://github.com/tarvihafatima/Amazon_Product_Review_Analysis/assets/26660037/d32d466f-4c4d-4cc0-986e-314686343d98)

## Data Quality Implementation

The data pipeline incorporates robust data quality checks at various stages to maintain the integrity, consistency, and reliability of the data. 
These checks help identify and address potential data anomalies, errors, or inconsistencies. The following data quality checks are 
implemented in the pipeline:

#### Data Completeness:
This check ensures that all the required fields and attributes in the product and reviews data are present and populated.
Such as asin in products data and reviewer id,overall_rating and asin in reviews data. 


#### Data Accuracy:
Data accuracy checks validate the correctness of the data values against defined rules. 

##### For example: 

* Product price should be greater than 0.
* The length of asin should be 10 characters. 
* Overall rating should fall between the range of 0 and 5.

#### Duplicate Detection: 
Duplicate detection checks identify and handle duplicate or redundant data in the product and reviews dataset.

#### Data Consistency:
Consistency checks ensure that schema and datatypes in the extracted product and reviews datasets are compliant with the 
pre-defined schema and datatypes in the database. 
 

## Installing / Getting started

You will need: 

* Python3
* Pip3
* Apache Spark
* Talend Open Studio
* PostgreSQL Database
* IDEs (Any for Database and Python Programming)

## Setup and Configuration

* Clone the repository
 ```shell
git clone https://github.com/tarvihafatima/Amazon-Product-Review-Analysis
```
* Install the depedencies by runnig below mentioned command in project main folder.
 ```shell
 pip install -r requirements.txt
```

* Fill In the Configurations for Talend and Python jobs 
  * Python Config Path: Product Review Analysis\Python Jobs\src\data\configuration.yaml
  * Talend Config Path: Product Review Analysis\Talend Job\Talend_Configs.txt
  
  
* Create Database, Schema and Tables for all Layers
  * Landing: Product Review Analysis\Postgres DB Scripts\Landing Layer\Database Schema and Log Table Creation.sql
  * Staging: Product Review Analysis\Postgres DB Scripts\Staging Layer\Staging DB Tables Creation.sql
  * Warehousing: Product Review Analysis\Postgres DB Scripts\Data Warehousing Layer\Dimensions and Facts Creation.sql
  
  
* Create Stored Procedures for Incremental Load of Dimensions and Fact in Data Warehouse
  * Dimensions: Postgres DB Scripts\Data Warehousing Layer\Dimension Tables Population\
  * Fact: Product Review Analysis\Postgres DB Scripts\Data Warehousing Layer\Fact Tables Population
  * Dimensions and Fact Parent: Product Review Analysis\Postgres DB Scripts\Data Warehousing Layer\Dimensions and Fact Incremental Update - Parent.sql


* Create Views for Presentation Layer
  * Product Category Level Analysis: Product Review Analysis\Postgres DB Scripts\Presentation Layer\Product Category Analysis.sql
  * Review Level Analysis: Product Review Analysis\Postgres DB Scripts\Presentation Layer\Review Analysis.sql
  * Product Review Consolidated Analysis: Product Review Analysis\Postgres DB Scripts\Presentation Layer\Product Reviews Consolidated View.sql
 
  
* Extract Talend Zip File from Product Review Analysis\Talend Job\


* Schedule or Run "Product_Review_Workflow_Parent_run.bat" file in Extracted Folder to Start the ETL process.

## Links

* Github Repository: https://github.com/tarvihafatima/Amazon-Product-Review-Analysis
* Python Download: https://www.python.org/downloads/
* Apache Spark Download: https://spark.apache.org/downloads.html
* Postgres Download: https://www.postgresql.org/download/
* DBeaver (IDE For PostgreSQL): https://dbeaver.io/download/
* Visual Studio Code (IDE for Python): https://code.visualstudio.com/download
