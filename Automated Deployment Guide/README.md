# Demand Forecasting and Price Optimization Solution

## Abstract
This **Automated Deployment Guide** contains the post-deployment instructions for the deployable [**Demand Forecasting and Price Optimization Solution**](https://gallery.cortanaintelligence.com/Solution/Demand-Forecasting-and-Price-Optimization) solution in the Cortana Intelligence Gallery. 

<Guide type="PostDeploymentGuidance" url="https://github.com/Azure/cortana-intelligence-price-optimization/blob/master/Automated%20Deployment%20Guide/Post%20Deployment%20Instructions.md"/>


## Summary
<Guide type="Summary">
Pricing is recognized as a pivotal determinant of success in many industries and can be one of the most challenging tasks. Companies often struggle with several aspects of the pricing process, including accurately forecasting the financial impact of potential tactics, taking reasonable consideration of core business constraints, and fairly validating the executed pricing decisions. Expanding product offerings add further computational requirements to make real-time pricing decisions, compounding the difficulty of this already overwhelming task. 

This solution addresses the challenges raised above by utilizing historical transaction data to train a demand forecasting model. Pricing of products in a competing group is also incorporated to predict cross-product impacts such as cannibalization. A price optimization algorithm then employs the model to forecast demand at various candidate price points and takes into account business constraints to maximize profit. The solution can be customized to analyze various pricing scenarios as long as the general data science approach remains similar. 

The process described above is operationalized and deployed in the Cortana Intelligence Suite. This solution will enable companies to ingest historical transaction data, predict future demand, and obtain optimal pricing recommendations on a regular basis. As a result, the solution drives opportunities for improved profitability and reductions in time and effort allocated to pricing tasks.
</Guide>

## Prerequisites
<Guide type="Prerequisites">

This pattern requires creation of **1 HDInsight Cluster with 16 cores**. Ensure adequate HDInsight core quotas are available before provisioning. By default one subscription can create a maximum of 20 core cluster.
The limit can be increased. Please consider deleting any unused HDInsight Cluster from your subscription. You may contact [Azure Support](https://azure.microsoft.com/support/faq/) if you need to increase the limit.
</Guide>


## Description

#### Estimated Provisioning Time: <Guide type="EstimatedTime">36 Minutes</Guide>
<Guide type="Description">
Cortana Intelligence provides advanced analytics tools through Microsoft Azure — data ingestion, data storage, data processing and advanced analytics components — all of the essential elements for building a demand forecasting and price optimization solution.

This solution combines several Azure services to provide powerful advantages. Azure Blob Storage Store stores the weekly raw sales data. Apache Spark for Azure HDInsight ingests the data and executes data preprocessing, forecasting modeling and price optimization algorithms. Finally, Data Factory orchestrates and schedules the entire data flow.

The 'Deploy' button will launch a workflow that will deploy an instance of the solution within a Resource Group in the Azure subscription you specify. The solution includes multiple Azure services (described below) along with a web job that simulates data so that immediately after deployment you have a working end-to-end solution. 

## Solution Diagram
![Solution Diagram](https://user-images.githubusercontent.com/20048518/29930577-b0e50cfe-8e3c-11e7-865a-4fed7f38b6af.png)

## Technical details and workflow
1.	The simulation data is generated hourly by newly deployed **Azure Web Jobs**.

2.	This synthetic data is stored at **Azure Blob Storage**, that will be used in the rest of the solution flow.

3.	**Spark on HDInsight** is used to ingest and preprocess the raw data, build and retrain the demand forecasting models, and execute price optimization algorithms. 

6. **Azure Data Factory** orchestrates and schedules the entire data flow.

7.	Finally, **Power BI** is used for results visualization, so that users can monitor the results of the sales, predicted future demand as well as recommended optimal prices for a variety of products sold in different stores.
</Guide>

##### Disclaimer
©2017 Microsoft Corporation. All rights reserved.  This information is provided "as-is" and may change without notice. Microsoft makes no warranties, express or implied, with respect to the information provided here.  Third party data was used to generate the solution.  You are responsible for respecting the rights of others, including procuring and complying with relevant licenses in order to create similar datasets.
