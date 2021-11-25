## Welcome to my data engineer project
I am making a project using knowledge that I learn from road to data engineer 2.0 courses. The concept of my project is loaded covid-19 Thailand found a case by API, then cleaning the data and making pipeline to load data to data warehouse then making dashboard.
### STEP1: CLEANING DATA
I use jupyter notebook for a tool. First, I run jupyter notebook by docker. Then I install pyspark for a tool to clean data. After I got a data I select only column that I need and change schema timestamp columns from string to timestamp.

![cleaning1](image/1.png)
![cleaning2](image/2.png)

Then, finding missing value
![missingV](image/5.png)
and fixing all missing values, for an example, the age_number column change Null to 0
![missingF](image/6.png)
after fixing all missing, save the file
![FixMissingValue](image/FixMissing.png)
![savefile](image/savefile.png)
