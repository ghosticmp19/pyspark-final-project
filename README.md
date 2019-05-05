# pyspark-final-project
## Objective

In this homework we ask you to install and configure Pyspark and write some queries to Batch process some data


## Initial setup
Start by rendering a new app via your cookiecutter template and linking to this repo as before. 

Start by installing Pyspark on your machine. There is a good wiki here: https://www.tutorialspoint.com/pyspark/pyspark_environment_setup.htm . 
Pyspark can be installed via Pip or Pipenv.

## Data setup
Create a data directory within your project and add the following dataset
Known Police department Calls for Service: https://data.sfgov.org/Public-Safety/-Known-Issue-Police-Department-Calls-for-Service/hz9m-tj6z

ADD THIS DIRECTORY TO YOUR .gitignore if it's not already there

Rename the dataset to police-data.csv

## Queries 50 points

First we must read the data into Pypark. For this assignment we will primarily using the DataFrame interface in Pyspark. Read the csv into a DataFrame and print the Schema.

Query 1:
Print the number of rows in the DataFrame

Query 2:
Determine if there are any duplicate rows in the DataFrame. If there are, print the row.

Query 3:
Write a filter to filter out only crimes that contain Suspicious Vehicle. How many?

Query 4:
Write a query to filter out crimes containing Assault in the Type Name

Query 5:
Write a query to figure out which month has the largest number of Intoxicated Person(s)

Query 6:
Which month has the largest number of crimes?

Query 7:
Which hour of the day has the most number of offences? 



