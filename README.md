# Project 2
by Aditya Kishan Ankaraboyana, Tin Ngo, and Ryan Tavares

Project 2 Requirements

Each team will be tasked with the analysis of a large dataset (10-15 thousand records) using our E-Commerce data structure.

Data should be processed and cleaned using PySpark.

A Power BI report should be created and visualizations shown answering specific analysis questions (found below).

All findings should be presented in a slide deck. All team members must participate in the presentation.

The dataset to be analyzed will also be generated by each team, and swapped with other teams to find trends.

## Data Analysis Questions

What is the top selling category of items? Per country?

How does the popularity of products change throughout the year? Per country?

Which locations see the highest traffic of sales?

What times have the highest traffic of sales? Per country?


### Data Generator
```
- The data generator used trend functions which included weights to create a bias in the data.
- The trend functions were applied across various columns, such as 
  product category, location, and others, using weighted parameters to introduce bias into the dataset.
```
### Data Cleaning

```
- Reorganized columns for match the given schema
- Removed rows containing null values
- Analyzed duplicates and decided to retain them, as all columns (except for the duplicate column) contained distinct values
```
