# dbt Models Documentation

## Overview
This README provides an overview of the dbt models used in this project, As I couldn't setup localy dbt (I am using laptop of my current employeer) and I would need more days to go from the begining and learn about dbt to be able to set it up and test locally. 

I am quite sure this is not working well. But I have created tables localy in redshift and tested those metrics. Maybe syntax is not same in dbt so maybe some functions will not work but In general I was able to execute them in Redshift. 

On top of it I have created tabelau report stored on my tableau public profile. 

https://public.tableau.com/views/store_and_transactions/Dashboard1?:language=en-GB&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link


## Models

I have followed jaffle shop dbt project to generate what I think would be important here, as they also had fake sources in term of csv files (where we can consider them as real sources in db) I tried the same. I also know it could be done with seeds in term of uploading csv files to dbt but like I said I couldn't do it locally.

### staging

### 1. stg_transactions
The `stg_transactions` model captures all transaction data processed through the store devices.

### 2. stg_devices
The `stg_devices` model holds information about the devices used in the stores for processing transactions.

### 3. stg_store
The `stg_store` model contains details about the stores using the devices to manage transactions.

### marts

There I have created marts to answer on request from the project. I hope it is fine like it is.
