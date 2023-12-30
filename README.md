# Project 3: STEDI Human Balance Analytics


### Table of Contents
- [Project Summary](#Project-Summary)
- [Datasets](#Datasets)
- [Architecture and Developments](#Architecture-and-Developments)




### Project Summary
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

As a data engineer on the STEDI Step Trainer team, we'll need to **extract the data** produced by the STEDI Step Trainer sensors and the mobile app, and **curate them into a data lakehouse solution** on AWS so that Data Scientists can train the learning model.

During this project, three services from Amazon Web Services will be used:
* [AWS S3](https://aws.amazon.com/pt/s3/)
* [AWS Glue](https://aws.amazon.com/pt/glue/)
* [AWS Athena](https://aws.amazon.com/pt/athena/)

Regarding the sources used during the project, you can find them under [Datasets](#Datasets).


### Datasets
Three data sources were used, which are in public **repository on the Git Hub**, and the objects contained in both buckets are JSON files.

Information about the *accelerometer*, *customer* and *step_trainer* from the [nd027-Data-Engineering-Data-Lakes-AWS-Exercises](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project)
  * Accelerometer data: `s3://stedi-lake-house-lf/accelerometer/` data from the mobile app.
~~~~
accelerometer-1691348231445.json
accelerometer-1691348231495.json
~~~~
Example:
~~~~
{"user":"Santosh.Clayton@test.com","timestamp":1655564444103,"x":1.0,"y":-1.0,"z":-1.0}
~~~~

  * Customer data: `s3://stedi-lake-house-lf/customer/` from fulfillment and the STEDI website.
~~~~
customer-1691348231425.json
~~~~
Example:
~~~~
{"customerName": "Santosh Clayton", "email": "Santosh.Clayton@test.com", "phone": "8015551212", "birthDay": "1900-01-01", "serialNumber": "50f7b4f3-7af5-4b07-a421-7b902c8d2b7c", "registrationDate": 1655564376361, "lastUpdateDate": 1655564376361, "shareWithResearchAsOfDate": 1655564376361, "shareWithPublicAsOfDate": 1655564376361, "shareWithFriendsAsOfDate": 1655564376361}
~~~~

  * Step Trainer data:  `s3://stedi-lake-house-lf/step_trainer/` data from the motion sensor.
~~~~
step_trainer-1691348232038.json
step_trainer-1691348232085.json
~~~~
Example:
~~~~
{"sensorReadingTime":1655564149444,"serialNumber":"454a7430-d4ff-47cf-8666-7428f8a9894d","distanceFromObject":236}
~~~~

![img.png](img.png)



### Architecture and Developments

<details><summary>Landing Zone</summary>
Using the AWS glue Data Catalog, glue tables were created to access the data with AWS Athena:

+ Accelerometer Landing - Copies the data of customers, who have agreed to share their data with the researchers, from the "landing" zone to the "trusted" zone.
+ Customer Landing
+ Step Trainer Landing
</details>    
<details><summary>Trusted Zone</summary>
Using AWS Glue Jobs, various transformations were created on the raw data stored in the landing zone.These generated the following Python scripts:

+ `accelerometer_landing_to_trusted.py` - Copy the data of customers, who have agreed to share their data with the researchers, from the "landing" zone to the "trusted" zone.
+ `customer_landing_to_trusted.py` - Copy the data of the customers, who have agreed to share their data with the researchers, from the accelerometer in the "landing" zone, adding the data from customer_trusted and accelerometer_landing, to the "trust" zone.  
+ `step_trainer_trusted.py` -  Copy the data of the customers, who have agreed to share their data with the researchers, from the Step Trainer of the "landing" zones to the "trust" zones. 
</details>  
<details><summary>Curated Zone</summary>
Using AWS Glue Jobs, various transformations were created to make analysis. These generated the following Python scripts:

+ `customer_trusted_to_curated.py` - Copy customer data from the "trusted" zones to the "curated" zones. 
+ `machine_learning_curated.py` - Creates an aggregate table containing Step Trainer data and the associated accelerometer reading data by the same timestamp.
</details>  
