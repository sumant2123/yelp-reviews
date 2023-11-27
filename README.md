# yelp-reviews- Case study

<img src="https://github.com/sumant2123/yelp-reviews/blob/main/images/Yelp-Logo.png?raw=true"/>

## Table of Contents

* [Introduction](#Introduction)
   * [About Yelp](#About-Yelp)
   * [Objective](#Objective)
   * [Dataset](#Dataset)
* [ETL and Data Storage ](#ETL-and-Data-Storage)
* [EDA](#EDA)
* [Pipeline](#Pipeline)
* [Modeling](#Modeling)
* [Conclusion](#Conclusion)

## Introduction

### About Yelp

Yelp is an online platform that helps people find and review local businesses like restaurants, bars, salons, and more. Some key things to know about Yelp:

* Yelp connects people with local businesses by providing reviews, ratings, photos, and more for over 200 million reviews.
* Operates in 30+ countries.The Yelp app and website get around 30 million unique visitors per month.
* Community content driven. The reviews, ratings, photos and other content on Yelp is created by members of the Yelp community to share their experiences with different businesses.
* Key features include the review feed, ability to check-in at locations, make reservations or food orders through Yelp. 

In summary, Yelp aims to be a one-stop consumer resource for finding and engaging with local businesses, powered by crowdsourced user reviews and local business outreach/advertising. The company has evolved significantly since its early days.

### Goal

The project's main goal is designing an accessible interface that condenses the sentiments in Yelp reviews into a readable format. By gathering the opinions scattered across reviews and synthesizing them into digestible overviews, the application aims to benefit anyone seeking to quickly gauge or closely examine a business's reputation.

### Dataset

<img src="https://github.com/sumant2123/yelp-reviews/blob/main/images/yelp-dataset.jpg?raw=true"/>

The dataset used in this project is sourced from the [Yelp Open Dataset](https://www.yelp.com/dataset). This dataset fuels the project's web application, which digests user reviews and comments to help people make good choices about businesses. 


The dataset comprises 5 JSON files, with a compressed size of around 4 GB that expands to approximately 10 GB when uncompressed. These files capture diverse facets of Yelp's data, each presenting unique characteristics:

* **User Data**
    * Incorporates user information, encompassing friend mappings and associated metadata.
    * Consists of roughly 2 million data points with 22 features.
    * Occupies a storage size of 3.4 GB.
    * Includes data types such as int, float, and string.

* **Business Data**
    * Includes business-related details like location, attributes, and categories.
    * Encompasses about 150,000 data points with 14 features.
    * Utilizes 120 MB of storage.
    * Involves data types such as int, float, string, and struct.

* **Reviews Data**
    * Comprises complete review text data, specifying the user_id and business_id.
    * Extends to nearly 7 million data points with 9 features.
    * Consumes 5.4 GB of storage.
    * Involves data types such as int, float, and string.

* **Check-In Data**
    * Includes check-ins on a business.
    * Covers approximately 130,000 data points with 2 features.
    * Requires 290 MB of storage.
    * Involves data types such as string.
* **Tip Data**: 
    * Encompasses tips written by users on a business, offering concise suggestions.
    * Consists of about 1 million data points with 5 features.
    * Utilizes 180 MB of storage.
    * Involves data types such as int and string.


<br/>

## ETL and Data Storage

The project involved the extraction, transformation, and loading (ETL) of data into an AWS RDS Postgres instance, serving as the primary storage solution. This section outlines the ETL process steps and the rationale for selecting AWS RDS Postgres for data storage.

<img src="https://github.com/sumant2123/yelp-reviews/blob/main/images/ETL.png?raw=true"/>

### Extraction

The dataset, initially in compressed TGZ format, was uncompressed to obtain 5 JSON files. Given the dataset's volume, PySpark efficiently parsed and processed JSON files, preparing the data for subsequent steps.

### Transformation

Post-extraction, various transformation steps were implemented:

* **Data Integration**: Data from various JSON files were logically integrated to align with the project's objectives.

* **Data Formatting**: Data formatting was applied to maintain consistent data types.

* **Data Reduction**: Given the substantial size of the dataset, only the relevant features were selected.

* **Data Cleaning**: The data underwent a comprehensive cleaning process to address issues such as missing values, duplicate entries, and inconsistencies, ensuring enhanced data reliability.

### Loading

Transformed data was loaded into an AWS RDS Postgres database. The deliberate choice of AWS RDS Postgres considered factors like high availability, scalability, and security.

Explore a snapshot of the processed dataset:

|      |business_id           |user_id               |review_id             |review_date   |review_stars|review_text         |review_total_interaction|user_yelping_since|user_review_count|user_average_stars|user_fans|user_friends_count|user_total_interactions|user_total_compliments|user_elite_years_count|user_elite_min_year|user_elite_max_year|biz_name                                |biz_city    |biz_state|biz_postal_code|biz_latitude|biz_longitude|biz_stars|biz_review_count|checkin_count|checkin_date_min|checkin_date_max|
|------|----------------------|----------------------|----------------------|--------------|------------|--------------------|------------------------|------------------|-----------------|------------------|---------|------------------|-----------------------|----------------------|----------------------|-------------------|-------------------|----------------------------------------|------------|---------|---------------|------------|-------------|---------|----------------|-------------|----------------|----------------|
|1     |goXz8V980-WFIreZRbLVSQ|4Q-nWKm4_t1UgImW3D_sgA|MGuVuTo9XDgXwHHCEC8O_Q|4/8/17 0:41   |1           |My original post ...|0                       |1/16/16 13:43     |8                |2.56              |0        |1                 |7                      |0                     |0                     |0                  |0                  |317 Burger                              |Indianapolis|IN       |46220          |39.87054    |-86.14242    |4        |422             |693          |12/8/13 1:28    |12/8/13 1:23    |
|2     |4dTLWrSe0GN_GrMZ1uAZwA|5N7-UqSgVYMW68Zo1Nxn5A|8sZv6asRsjIif3es493iQQ|12/26/14 20:57|5           |Hooters is the sa...|0                       |10/5/13 2:22      |8                |4.3               |0        |1                 |8                      |1                     |0                     |0                  |0                  |Hooters                                 |Metairie    |LA       |70006          |30.004791   |-90.18974    |3        |94              |603          |7/11/10 16:48   |6/16/10 0:06    |
|3     |9Ldo7ocJHVrEzRXUpN3Weg|6WW9sM9H2pKMr01twTLfYw|WjEfcAKUDOg7ADeI9U5DPA|5/14/15 21:27 |5           |My favorite nail ...|1                       |5/20/13 17:32     |1                |5                 |0        |1                 |1                      |0                     |0                     |0                  |0                  |Tampa Nails                             |Tampa       |FL       |33606          |27.945114   |-82.48182    |2.5      |121             |40           |8/28/14 14:17   |6/7/14 15:20    |
|4     |9dL1rsPANYr-71hdwoY-CA|43aSMIBPuDYFvduQByx4gw|1CMbbUzQkJFER754NEpUBA|9/18/15 3:17  |3           |The food is aweso...|1                       |4/14/10 22:51     |66               |3.81              |6        |225               |89                     |14                    |0                     |0                  |0                  |Desi Tadka Cuisine                      |Oldsmar     |FL       |34677          |28.042406   |-82.67776    |4        |169             |213          |5/24/13 0:57    |5/5/13 18:26    |

[def]: ttps://github.com/sumant2123/yelp-reviews/blob/main/images/ETL.png?raw=tru