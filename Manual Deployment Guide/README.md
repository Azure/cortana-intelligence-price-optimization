# Retail Pirce Optimization Solution in Cortana Intelligence Suite

## Table of Contents  
- [Abstract](#abstract)  
- [Requirements](#requirements)
- [Architecture](#architecture)
- [Setup Steps](#setup-steps)
- [Validation and Results](#validation-and-results)

## Abstract[YC] 


## Requirements


## Architecture


## Setup Steps
This section walks the readers through the creation of each of the Cortana Intelligence Suite services in the architecture defined in Figure 1.
As there are usually many interdependent components in a solution, Azure Resource Manager enables you to group all Azure services in one solution into a [resource group](https://azure.microsoft.com/en-us/documentation/articles/resource-group-overview/#resource-groups). Each component in the resource group is called a resource.
We want to use a common name for the different services we are creating. The remainder of this document will use the assumption that the base service name is:

retailtemplate\[UI\]\[N\]

Where \[UI\] is the users initials and N is a random integer that you choose. Characters must be entered in in lowercase. Several services, such as Azure Storage, require a unique name for the storage account across a region and hence this format should provide the user with a unique identifier.
So for example, Steven X. Smith might use a base service name of *retailtemplatesxs01*  

**NOTE:** We create most resources in the South Central US region. The resource availability in different regions depends on your subscription. When deploying you own resources, make sure all data storage and compute resources are created in the same region to avoid inter-region data movement. Azure Resource Group and Azure Data Factory don’t have to be in the same region as the other resources. Azure Resource Group is a virtual group that groups all the resources in one solution. Azure Data Factory is a cloud-based data integration service that automates the movement and transformation of data. Data factory orchestrates the activities of the other services.



### 1. Create a new Azure Resource Group

  - Navigate to ***portal.azure.com*** and log in to your account

  - On the left tab click ***Resource Groups***
  
  - In the resource groups page that appears, click ***Add***
  
  - Provide a name ***energytemplate\_resourcegroup***
  
  - Select a ***location***. Note that resource group is a virtual group that groups all the resources in one solution. The resources don’t have to be in the same location as the resource group itself.
  
  - Click ***Create***

### 2. Setup Azure Storage account

An Azure Storage account is used by the Azure Machine Learning workspace in the Batch path. 

  - Navigate to ***portal.azure.com*** and log in to your account.

  - On the left tab click ***+ (New) > Storage > Storage Account***

  - Set the name to ***energytemplate[UI][N]***

  - Change the ***Deployment Model*** to ***Classic***

  - Set the resource group to the resource group we created by selecting the radio button ***Use existing***

  -  Location set to South Central US
	
  - Click ***Create***

  - Wait for the storage account to be created

Now that the storage account has been created we need to collect some information about it for other services like Azure Data Factory. 

  - Navigate to ***portal.azure.com*** and log in to your account

  - On the left tab click Resource Groups

  - Click on the resource group we created earlier ***energytemplate_resourcegroup***. If you don’t see the resource group, click ***Refresh*** 

  - Click on the storage account in Resources

  - In the Settings tab on the right, click ***Access Keys***

  - Copy the PRIMARY CONNECTION STRING and add it to the table below

  - Copy the Primary access key and add it to the table below

    | **Azure Storage Account** |                     |
    |------------------------|---------------------|
    | Storage Account        |energytemplate\[UI][N]|
    | Connection String      |             |
    | Primary access key     |             ||



### 3. Setup Azure Data Lake Store


### 4. Setup HDInsight with Spark


### 5 Setup Azure Data Factory (ADF)
Azure Data Factory can be used to orchestrate the entire data pipeline. In this solution, it is mainly used to schedule the data aggregation and model retraining. Here is an overview of the ADF pipeline.

**Data Aggregation Pipeline**: Simulated data from Azure web job are sent to Azure SQL every 5mins. When we are building machine learning model, we use hourly data. Therefore, we write a SQL procedure to aggregate the 5mins consumption data to hourly average consumption data. One pipeline is created in Azure Data Factory to trigger the procedure so that we always have the latest hourly consumption data.

**Model Training and Forecasting Pipelines**: There are 11 sub-regions in NYISO and we build one model for each region. Therefore, 11 pipelines are created in Azure Data Factory to trigger the Azure Machine Learning Web Service. Each pipeline sends data from a particular region to the web service and gets the latest retrained model and forecast results. All the results are written back to Azure SQL.

There are 3 main components of ADF: link service, dataset and pipeline. You can check the definition of each components [here](https://azure.microsoft.com/en-us/documentation/articles/data-factory-introduction/). In the following instructions, we will show you how to create them for this solution.

#### 1) Create Azure Data Factory


-   Navigate to ***portal.azure.com*** and login in to your account.

-   On the left tab click ***New&gt;Data and Analytics&gt;Data Factory***

-   Name: *energysolution\[UI\]\[N\]*

-   Resource Group: Choose the resource group created previously ***energysolution\_resourcegroup***

-   Location: EAST US

-   Click ***Create***

After the data factory is created successfully

-   On the left tab in the portal page (portal.azure.com), click ***Resource groups***

-   Search for the resource group created previously, ***energysolution\_resourcegroup***

-   Under Resources, click on the data factory we just created, *energysolution\[UI\]\[N\]*

-   Locate the ***Actions*** panel and click on ***Author and deploy***.

In the ***Author and deploy*** blade, we will create all the components of the data factory. Note that Datasets are dependent on Linked services, and pipelines are dependent on Linked services and Datasets. So we will create Linked services first, then Datasets, and Pipelines at last.


#### 2) Create Linked Services
We will create 2 Linked services in this solution. The scripts of the Linked services are located in the folder ***Azure Data Factory\\1-Linked Services*** of the solution package.

- **LinkedService-AzureSQL**: This is the Linked service for the Azure SQL database.

  -   Open the file ***Azure Data Factory\\1-Linked Services\\LinkedService-AzureSQL.json***. Replace the following items with your Azure SQL credentials.

    - Azure SQL server name

    - Azure SQL database name

    - Azure SQL login user name

    - Azure SQL login password

  -   Go back to ***Author and deploy*** in the data factory on ***portal.azure.com.***

  -   Click ***New data store*** and select ***Azure SQL***.

  -   Overwrite the content in the editor window with the content of the modified *LinkedService-AzureSQ.json*.

  -   Click ***Deploy***.

- **LinkedService-AzureML**: This is the Linked service for the Azure Machine Learning web service.

  -   Open the file ***Azure Data Factory\\1-Linked Services\\LinkedService-AzureML.json***. Replace the following items with your Azure ML Web Service information.

    - Azure Machine Learning web service URI

    - Azure Machine Learning web service API key

  -   Go back to ***Author and deploy*** in the data factory on ***portal.azure.com.***

  -   Click ***New compute*** and select ***Azure ML***.

  -   Overwrite the content in the editor window with the content of the modified LinkedService-AzureML.json.

  -   Click ***Deploy***.

#### 3) Create Datasets

We will create ADF datasets pointing to Azure SQL tables. We will use the JSON files located at ***Azure Data Factory\\2-Datasets***. No modification is needed on the JSON files.

- On ***portal.azure.com*** navigate to your data factory and click the ***Author and Deploy*** button.

For each JSON file under ***Azure Data Factory\\2-Datasets***:

-   At the top of the left tab, click ***New dataset*** and select ***Azure SQL***

-   Copy the content of the file into the editor

-   Click ***Deploy***

#### 4) Create Pipelines

We will create 12 pipelines in total. Here is a snapshot.

![](Figures/ADFPipelineExample.png)

We will use the JSON files located at ***Azure Data Factory\\3-Pipelines.*** At the bottom of each JSON file, the “start” and “end” fields identify when the pipeline should be active and are in UTC time. You will need to modify the start and end time of each file to customize the schedule. For more information on scheduling in Data Factory, see [Create Data Factory](https://azure.microsoft.com/en-us/documentation/articles/data-factory-create-pipelines/) and [Scheduling and Execution with Data Factory](https://azure.microsoft.com/en-us/documentation/articles/data-factory-scheduling-and-execution/).

- Data aggregation pipeline

  This pipeline trigger the SQL procedure to aggregate the 5mins consumption data to hourly data.

  - Open the file ***Azure Data Factory\\3-Pipelines\\Pipeline-SQLProcedure.json***

  - Specify an active period that you want the pipeline to run. For example, if you want to test the template for 5 days, then set the start and end time as something like:

    ```JSON
    "start": "2016-11-01T00:00:00Z",
    "end": "2016-11-06T00:00:00Z",
    ```
    NOTE: Please limit the active period to the amount of time you need to test the pipeline to limit the cost incurred by data movement and processing.

  - On ***portal.azure.com*** navigate to your data factory and click the ***Author and Deploy*** button
  - At the top of the tab, click ***More commands*** and then ***New pipeline***

  - Copy the content of the modified JSON file into the editor

  - Click ***Deploy***




### 6. Setup Power BI[YC]





## Validation and Results[YC]