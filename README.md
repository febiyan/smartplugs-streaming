# Smart Plugs Real-time Anomaly Detection

## Solution

The solution is hosted on Google Cloud Platform on it Cloud Dataproc product. Cloud Dataproc provides ready-to-use, transient Hadoop cluster in the cloud. A group of related jobs can share the same cluster to provide isolation. 

The technologies utilised in the solution are primarily Apache Spark (PySpark and Scala Spark), Apache Kafka, and BigQuery. More details of the choices and the overview of the solution is provided in this [Google Slides document](https://docs.google.com/presentation/d/1ckxWmTXRhzIYTy8BBE8j3BwgjI9jufLI2IR4K3f5KX8/edit?usp=sharing).

### Environment Setup

The cluster setup commands are stored in in `/setup/cluster-setup.sh`. Please change the project name inside the script if you're rerunning this in another Google Cloud Project.


### Notebooks

The solution is presented in notebooks so that I could quickly reiterate when coding and it will be more understandable with markdown annotations. It also keeps the previously-ran outputs just in case of technical issues during presentation.

The notebooks are under `/jupyter`. They should have easily-understandable file names so that you can map it to the solution diagram in the Google Slides or the problems in the definition.


## Problem Description
This challenge contains two stage problem:

1.	**Building a data pipeline**: Build a data pipeline to stream power consumption/load data using kafka. You may use any processing system of your choice to ingest the data.
2.	**Generate real-time alerts**: Generate real-time alerts on power consumption (coming from kafka stream) when:
  1.	_**(Alert Type 1)**_: Hourly consumption for a household is higher than 1 standard deviation of that household's historical mean consumption for that hour
  2.	_**(Alert Type 2)**_: Hourly consumption for a household is higher than 1 standard deviation of mean consumption across all households within that particular hour on that day
 
## Data Description
Dataset is based on recordings originating from smart plugs. These smart plugs are deployed in private households.

* We have a hierarchical structure with a house, identified by a unique house id, being the topmost entity.
* Every house contains one or more households, identified by a unique household id (within a house).
* Every household contains ome or more smart plugs and a smart plug has 2 sensors
	* a load sensor measuring current load with Watt as unit.
	* work sensor measuring total accumulated work since the start (or reset) of the sensor in kWh
* Data is collected every 20 secs for each sensor

### Description

* **id** - a unique identifier of the measurement [32 bit unsigned int]
* **timestamp** – timestamp of measurement (number of seconds since January 1, 1970, 00:00:00 GMT) [32 bit unsigned integer value]
* **value** – the measurement [32 bit floating point]
* **property** - type of the measurement: 0 for work or 1 for load [boolean] 
* **plug_id** - a unique identifier (within a household) of the smart plug [32 bit unsigned int] 
* **household_id** – a unique identifier of a household (within a house) where the plug is located [32 bit unsigned integer value]
* **house_id** – a unique identifier of a house where the household with the plug is located [32 bit unsigned integer value]


The data set is collected in an uncontrolled, real-world environment, which implies **the possibility of malformed data as well as missing measurements.**

### Source
Data is from the ACM DEBS 2014 Grand Challenge

### Sample

| id | timestamp | value | property | plug_id | household_id | house_id |
|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| 33011 | 1377986420 | 0 | 1 | 1 | 0 | 3 |
| 33012 | 1377986420 | 3.216 | 0 | 1 | 0 | 3 |
| 33429 | 1377986420 | 0 | 1 | 0 | 0 | 5 |
| 33430 | 1377986420 | 3.216 | 0 | 0 | 0 | 5 |
| 33431 | 1377986420 | 0 | 1 | 1 | 0 | 5 |
| 33432 | 1377986420 | 2.25 | 0 | 1 | 0 | 5 |
| 33433 | 1377986420 | 0 | 1 | 2 | 0 | 5 |
| 33434 | 1377986420 | 2.25 | 0 | 2 | 0 | 5 |
| 33461 | 1377986420 | 0 | 1 | 1 | 0 | 7 |
| 33462 | 1377986420 | 2.25 | 0 | 1 | 0 | 7 |

## Requirements
* Data must be consumed in a streaming fashion.
* A component which will help you read the data from the decompressed file and push it to Kafka.
* Once the data is being streamed through Kafka, you may use any processing system of your choice to ingest the data
* Expectation is that before proceeding with any calculations, data needs to be cleansed, free from duplicates and missing data conditioned.
* Since data must be consumed in streamed fashion, this means that when currently processing an event with timestamp **ts**, the system can only work with events with timestamps smaller or equal to **ts**, i.e., those it has already seen.
* **No precomputation based on the whole data set can be made**.
* Pipeline design and architecture must be diagrammed and explained in a standalone document

### Preferred Tech Stack choices
* Hadoop
* MapReduce
* Kafka
* Spark
* NiFi
* Parquet
* Hive
* Flink
* HBase
* Python
* Docker

