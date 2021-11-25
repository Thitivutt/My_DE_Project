## Welcome to my data engineer project
I am making a project using knowledge that I learn from road to data engineer 2.0 courses. The concept of my project is loaded covid-19 Thailand found a case by API, then cleaning the data and making pipeline to load data to data warehouse then making dashboard.
### STEP1: CLEANING DATA
i use jupyter notebook for a tool. first i run jupyter notebook by docker. Then i install pyspark for a tool to cleaning data. After i got a data i select only cplumn that i need  and change schema timestamp column from string to timestamp

![cleaning1](image/1.png | width=100)
![cleaning2](https://scontent.fbkk22-8.fna.fbcdn.net/v/t1.15752-9/260934687_1002276647168715_701752162112100321_n.png?_nc_cat=108&ccb=1-5&_nc_sid=ae9488&_nc_ohc=9XTJtzwMKBgAX8i2dP0&_nc_ht=scontent.fbkk22-8.fna&oh=1c87f62ca2e775ba545808b24918c0a9&oe=61C582AF | width=100)

Then, finding missing value
![missingV](https://scontent.fbkk22-8.fna.fbcdn.net/v/t1.15752-9/261021064_493028225290474_3258532288862776281_n.png?_nc_cat=104&ccb=1-5&_nc_sid=ae9488&_nc_ohc=FZlLVR8Qd0wAX8Iboli&_nc_ht=scontent.fbkk22-8.fna&oh=d9f9227b26c33244d9432ad8dd55d540&oe=61C43947 | width=100)
and fixing all missing value
![missingV](https://scontent.fbkk22-7.fna.fbcdn.net/v/t1.15752-9/260842736_313484084112147_4667766382975145030_n.png?_nc_cat=107&ccb=1-5&_nc_sid=ae9488&_nc_ohc=gYIUL6pv-egAX_s4CEg&_nc_ht=scontent.fbkk22-7.fna&oh=34a4d0bd852da21ea4cd3554da85cebd&oe=61C69B83 | width=100)


