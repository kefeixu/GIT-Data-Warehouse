-- enter your_bucket and your_iam_role in 
-- copy staging_events ... and copy staging_songs ...

-- create staging table
drop table if exists staging_events;
create table staging_events (
                                    artist varchar(250),
                                    auth varchar(15),
                                    firstName varchar(25),
                                    gender varchar(10),
                                    itemInSession smallint,
                                    lastName varchar(25), 
                                    length float,
                                    level varchar(10),
                                    location varchar(250),
                                    method varchar(10),
                                    page varchar(25),
                                    registration float,
                                    sessionId int,
                                    song varchar(200),
                                    status smallint,
                                    ts bigint,
                                    userAgent varchar(250),
                                    userId varchar(10));
                                    
drop table if exists staging_songs;
create table staging_songs(
                                    artist_id varchar(50),
                                    artist_latitude float8,
                                    artist_location varchar(250),
                                    artist_longitude float8,
                                    artist_name varchar(250),
                                    duration float8,
                                    num_songs smallint,
                                    song_id varchar(50),
                                    title varchar(200),
                                    year smallint);
 

-- copy data to staging table
copy staging_events from 's3://udacity-dend/log_data/'
          credentials 'aws_iam_role= your_iam_role'
          region 'us-west-2'
          json 's3://your_bucket/log_json_path.json';
          
copy staging_songs from 's3://udacity-dend/song_data/'
          credentials 'aws_iam_role=your_iam_role'
          region 'us-west-2'
          json 's3://your_bucket/song_json_path.json';

-- validate data in staging table
-- get unique song, artist in event staging and song staging tables
drop table if exists song_in_log_staging;
create table song_in_log_staging
distkey(song)
sortkey(song)
as
select distinct trim(lower(song)) as song, trim(lower(artist)) as artist
from public.staging_events
where page = 'NextSong';

drop table if exists song_in_song_staging;
create table song_in_song_staging
distkey(song)
sortkey(song)
as
select distinct lower(trim(title))as song, lower(trim(artist_name)) as artist
from public.staging_songs;

drop table if exists song_in_both_staging_2;
create table song_in_both_staging_2 as
select a.song, a.artist
from song_in_log_staging as a, song_in_song_staging as b
where a.song = b.song and a.artist = b.artist;


select count(*) as song_in_log
from song_in_log_staging;
select count(*) as song_in_song
from song_in_song_staging;
select count(*) as song_in_both_staging_2
from song_in_both_staging_2

