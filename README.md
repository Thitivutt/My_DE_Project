## Welcome to my data engineer project
I am making a project using knowledge that I learn from road to data engineer 2.0 courses. The concept of my project is loaded covid-19 Thailand found a case by API, then cleaning the data and making pipeline to load data to data warehouse then making dashboard.

![projectplan](image/project-plan.png)


### STEP1: CLEANING DATA

<img align="left" width="450" height="250" src="image/cleaning-data.png">

**In this step you can look at my code "[covid-19.ipynb](covid-19.ipynb)"**
I open jupyter notebook with docker and install pyspark. Next i download data from this [API](https://covid19.ddc.moph.go.th/api/Cases/today-cases-line-lists). After I got data, I select the only column that I need and fix the missing value. Then upload complete file to database.

Fix all missing value
![FixMissingValue](image/FixMissing.png)

