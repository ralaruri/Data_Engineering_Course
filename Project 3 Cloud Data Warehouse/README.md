## Overall Goal With Sparkify's Database:
* As a young start up we are getting a large influx of data early on and can see the upward trend having more data created through our product. 
* Musicans will keep adding music daily, and the VP of Product wants to introduce the ability for amatuer musicans to upload music to sparkify so that will surely increase our database even further.  
* In order to fully optimize our analytical database for our analysts we need to think of the queries today and the future queries we will write. 
* This includes potentially our queries through out BI tools when creating dashboards that can run a series of queries at once to create the dashboard. 



## Step 1. Creating a Redshift Cluster and an IAM Role Script.

* The first step was to programmtically create a redshift cluster. 
* This is can be done in the AWS console but it was pretty easy mimicing past projects.
* I intially used the same details in the dwh.cfg file and the same ipynb we used intially. 


## Step 2. Removing the Credientals for Creating Redshift Table
* In this step I removed those details from the dwh.cfg file and added a few details the the [AWS], [CLUSTER], [IAM] role portions. 
* The intial pieces used to create the cluster because they are no longer needed when running the create tables and ETL script.
* only portions 

## Step 3. Create sql_queries.py Python Script.
* First we create our staging tables that includes all the raw song and event data. This makes it easier to model smaller pieces of the data to have higher performance querying and we still can have the raw staged tables to create ones in the future
* Creating the tables I needed to idenitfy sort keys and dist where I could the indivdual final tables.

## Step 3.1 sort and dist key choices. 
* Sort key are pretty self explantory I chose unqiue indefier for each table whenever I chose a sort key. 
* Dist keys I chose for each table based on a idea of how they would be queried and joined usually by the user_id or song_id, artist_id in future queires and to improve the performance on how these tables will be commonly joined. 
* This is honestly just a test of how it might play it to be honest I feel like this is a place where I the data engineer need to talk to the Data Analysts and Scientists and understand their business usage and potenitally stakeholders to understand what is needed for their queries or how they will build our their dashboards. 



### Songplays Table
    * songplay_id integer IDENTITY(0,1) PRIMARY KEY sortkey
### Users Table
    * user_id integer PRIMARY KEY distkey
### Songs Table
    * artist_id varchar distkey,
### Artists Tabe
    *artist_id varchar NOT NULL SORTKEY
### Time Table
    *start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY
    


## Step 4. Run the Create_Tables.py Script
## Step 5. Run the etl.py Script. 


## Step 6. Test Queries in Redshift Console. 

### Query 1: Testing a Join between two tables
with songplays as (

SELECT * FROM public.songplay_table
  
),

users as (
  
SELECT * FROM public.users
  
)

select * from songplays 
left join users on songplays.user_id = users.user_id
limit 100;





### Query 2 Targeted Marketing: 
* I want to understand where our most engaged users are and potentially have a targeted marketing campaign in the same area to get the same sort of users that are highly engaged or better understand what might be different in these highly engaged users. 



with songplays as (

SELECT * FROM public.songplay_table
  
),

users as (
  
SELECT * FROM public.users
  
),

joined as (
select 
  	songplay_id, 
  	users.user_id, 
  	songplays.location, 
  	first_name, 
  	last_name, 
  	gender from songplays 
left join users on songplays.user_id = users.user_id

)

SELECT 
	distinct(user_id), 
    first_name, 
    last_name, 
    count(songplay_id), 
    location
from joined
Group by 1,2,3,5;


#### Solved Major Issues:
* Timestamp --> Had to look at the knowledge base and online resources to understand why my timestamp was not working.
* This was an extremely helpful resource:  https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift/45197994
* staging_events_copy/staging_songs_copy --> Was having trouble making my copy statements working correctly even after copying we did from pervious examples. It was importance to make sure it knew it was reading off a JSON file in order to properly work. 

## PLEASE NOTE I DELETED MY AWS KEY AND PASSWORD FOR SAFETY AND SECURITY REASONS AFTER COMPLETING THIS PROJECT. 