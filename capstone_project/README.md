# Capstone Project Udacity Data Engineering Nanodegree


## Purpose

We at the NGO Brasil Saúde (Brazil Health) want to understand if covid-19 vaccination numbers in Brazil has some correlation with Brazil's election results. In order to do that, our data engineering team retrieved data from the electoral court website and stored in a s3 bucket. In addition they also created a data model to make data easier to analyse in our database. 

We also were in need of demographics for Brazil cities, which we also included in this project. We got the data from an IBGE (brazilian geographic and statistic institue) dataset on kaggle.

obs: we already have the covid-19 vaccination numbers


## Choice of tools and technologies

We already already works with AWS cloud here at Brazil Saúde. We usually store raw data into S3 and use Redshift to store processed data. So it made sense to adapt this project into the techologies that we were familiar to. 

Also, we use airflow to manage our ELT pipeline. Knowing that, the choice for this project was again to adapt the pipeline into our tecnologies.

The amount of data wasn't a problem within the chosen stack. However, if in the future the data were to be increased by 100x, we deeply recommend the use of spark to manage the workload.

# Data Update

Although we used airflow to run the pipelines, election data only gets update every two years in Brazil. So we didn't turned on a recurrent schedule for this pipeline. However, if in some moment this is required (eg: The pipelines would be run on a daily basis by 7 am every day), we only need to update the schedule parameter on the dags.

# Access

The access of the database is managed by the our IAM team. If we need to increase the number of people that have access to this data, we will need to call theam and request the creation of more users in Redshift (and set these users permissions to access the dim and fact schemas)

## Raw data

Election data was collected from brazil's superior electoral court website (https://dadosabertos.tse.jus.br/dataset). 
Demographic data was collected from an IBGE dataset on kaggle (https://www.kaggle.com/datasets/crisparada/brazilian-cities?resource=download&select=Data_Dictionary.csv)

# DAGS

We have created 2 dags in this project:

1. election_data_dag:

 - This dag loads from S3 both candidates and votes files and insert its data into the raw_data schema on Redshift
 - The next step consist in create the dimension and fact tables
 - And then, to finish, we do some quality checks for each table

2. ibge_data_dag:

 - This dag loads from S3 ibge's population file and insert its data into the raw_data schema on Redshift
 - The next step consist in create the dimension and fact tables
 - And then, to finish, we do some quality checks 

As a lot of the steps repeat themselves, we could have coded some operators in a subdag. However, we have chosen to code each operator and task separately due to pipeline management quality

## Schema design

We've chosen to design our database using the star schema concept. This will help our analysts to query the data in a simpler way. A diagram of the table schema can be found in here: https://dbdiagram.io/d/6293e0fcf040f104c1ba0ca4

The ETL process creates 7 tables, being 1 fact table and 6 dimension tables:

#### Fact Tables:

1. fact.votes: table containing information about brazil's electoral votes
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | date | date | vote date (FK) |
    | location_id | varchar | location identification code (FK) |
    | candidate_id | varchar | candidate identification code (FK) |
    | party_id | integer | party identification code (FK) |
    | vote_type | varchar | in Brazil we can have votes on candidates, on partys and null/blank votes |
    | vote_amount | integer | amount of votes | 

    Obs: the rows with vote_type null_vote and blank_vote do not have a correspondent on dim.candidate or dim.party. Also, the rows that the column candidate_id is not null, do not have a correspondent on dim.party and vice-versa.

#### Dimension Tables

1. dim.location: information about electoral locations in Brazil
    
    | Column      | Type        | Description              |
    | ----------- | ----------- | ------------------------ |
    | id | varchar | location identification code (PK) |
    | electoral_zone | integer | a city can be divided into electoral zones|
    | electoral section | integer | an electoral zone can be divided into electoral sections |
    | city | varchar | city that contains the zone and section |
    | state | varchar | state that contains the zone and section |

2. dim.election: information about Brazil elections

    | Column      | Type        | Description              |
    | ----------- | ----------- | ------------------------ |
    | id | varchar | election identification code (PK) |
    | coverage_type | varchar | election coverage type (state, municipal or federal) | 
    | office | varchar | election office (president, congressman, governor, etc) |
    | round | integer | Some elections in Brazil takes 2 rounds |

3.  dim.candidate: information about the brazil's election candidates 

    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    |  cpf_number | varchar | candidate cpf number; cpf is a personal register number in Brazil, so it acts like a primary key|
    |  name | varchar | candidate full name (registered in the last election the candidate ran) |
    |  email | varchar | candidate email (registered in the last election the candidate ran) |
    |  birth_date | date | candidate birth date (registered in the last election the candidate ran) | 
    |  voter_registration_number | varchar | candidate voter registration number (registered in the last election the candidate ran) |
    |  marital_status | varchar | candidate marital status (registered in the last election the candidate ran) | 
    |  skin_color | varchar | candidate skin color (registered in the last election the candidate ran) | 
    |  gender | varchar | candidate gender (registered in the last election the candidate ran) | 
    |  educational_level | varchar | candidate educational_level (registered in the last election the candidate ran) | 

    
4. dim.date: table containing date attributes

    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    |  date | date | date in 'YYYY-MM-DD' format |
    |  year | integer | year attribute |
    |  month | integer | month attribute (1-12) | 
    |  day | integer | day attribute |
    |  day_of_week | integer | weekday attribute (0 = sunday) |

5. dim.party: table containing information about brazil's electoral partys

    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    |  id | varchar | party identification code (PK)|
    |  party_name | varchar | the party name |
    |  abbreviation | varchar | party abbreviation | 

6. dim.city: table containing information about brazil's electoral partys

    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    |  id | varchar | city identification code (PK)|
    |  city | varchar | city name |
    |  state | varchar | state name | 
    |  capital | boolean | if a city is a state capital or not |
    |  residential_population | integer | city's residential population | 
    |  brazilian_residentail_population | integer | city's brazilian residential population |
    |  foreign_residential_population | integer | city's foreign residential population | 
    |  household_amount | integer | city's household total number |
    |  urban_household_amount | integer | city's urban household total number | 
    |  rural_household_amount | integer | city's rural household total number | 

    As this is a additional data from a different data source, the link between data will need to be done using the city name column, and the link will be between two dimension tables.
    
    
** The NGO is fictional and created for this project only
