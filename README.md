1) [Pipeline](https://github.com/CTBustalinio/DSCI6007-student/blob/master/8.2%20-%20Final%20Project/project_files/Process%20Map.png)   


2) Data was streamed from[MeetUp RSVP Firehose](http://stream.meetup.com/2/rsvps)
using this [code](https://github.com/CTBustalinio/DSCI6007-student/blob/master/8.2%20-%20Final%20Project/project_files/meetup_firehose.py)   


3) Sample file in [S3 data repo](https://s3.amazonaws.com/meetupstream/2017/03/03/02/MeetUpStream-2-2017-03-03-02-13-14-816f12fc-020e-4802-80c0-4d5418668237)   


4) Pyspark was used to extract necessary data fields using this [code](https://github.com/CTBustalinio/DSCI6007-student/blob/master/8.2%20-%20Final%20Project/project_files/project_create_3NF.ipynb)   


5) Spark DataFrames, were converted to Pandas DataFrames, to csv, in 3NF. CSV files:   
  [RSVP](https://s3.amazonaws.com/meetupstream/csv+3NF/rsvp.csv)   
  [Event](https://s3.amazonaws.com/meetupstream/csv+3NF/event.csv)   
  [Group](https://s3.amazonaws.com/meetupstream/csv+3NF/group.csv)   
  [Group Topics](https://s3.amazonaws.com/meetupstream/csv+3NF/group_topics.csv)   
  [Member](https://s3.amazonaws.com/meetupstream/csv+3NF/member.csv)   
  [Venue](https://s3.amazonaws.com/meetupstream/csv+3NF/venue.csv)   


The .csv files were connected to [Tableau frontend](https://public.tableau.com/profile/camille.bustalinio#!/vizhome/DataScienceMeetUpsinSanFrancisco/DataScienceEventsperCity)

Limitations:   
Tableau Public limits data to 15M rows   
Tableau can connect to Amazon EMR and Spark SQL but wasn't able to configure  
