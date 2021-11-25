## Welcome to my data engineer project
I am making a project using knowledge that I learn from road to data engineer 2.0 courses. The concept of my project is loaded covid-19 Thailand found a case by API, then cleaning the data and making pipeline to load data to data warehouse then making dashboard.
### STEP1: CLEANING DATA
i use jupyter notebook for a tool. first i run jupyter notebook by docker. Then i install pyspark for a tool to cleaning data. After i got a data i select only cplumn that i need  and change schema timestamp column from string to timestamp

![cleaning1](image/1.png)
![cleaning2](image/2.png)

Then, finding missing value
![missingV](image/5.png)
and fixing all missing value
![missingF](image/6.png)


