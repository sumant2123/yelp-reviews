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


The dataset consists of 5 JSON files, totaling approximately 4 GB in compressed format and expanding to roughly 10 GB when uncompressed. These files encompass various aspects of Yelp's data, each with distinct characteristics:

