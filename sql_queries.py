import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender char(1),
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sessionId integer,
    song varchar,
    status integer,
	ts bigint,
    userAgent varchar,
	userId integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs integer,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration real,
    year integer
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int NOT NULL PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender char, 
    level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL PRIMARY KEY, 
    song_title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration real);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL PRIMARY KEY, 
    artist_name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region 'us-west-2'
    json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    region 'us-west-2' json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select cast(TIMESTAMP 'epoch' + a.ts/1000 * interval '1 second' as timestamp) as start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent
from staging_events as a 
inner join staging_songs as b on a.song = b.title
where a.page = 'NextSong';
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level) 
select a.userId, a.firstName, a.lastName, a.gender, a.level
from staging_events a
inner join (
select userId, max(ts) as ts 
from staging_events 
group by userId, page 
having page = 'NextSong'
) b on a.userId = b.userId and a.ts = b.ts
where a.page = 'NextSong';
""")

song_table_insert = ("""
insert into songs(song_id, song_title, artist_id, year, duration) 
select song_id, title, artist_id, year, duration  from staging_songs;
""")

artist_table_insert = ("""
insert into artists(artist_id, artist_name, location, latitude, longitude) 
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude  from staging_songs;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday) 
select distinct cast(TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as timestamp) as start_time, 
extract(hour from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as hour, 
extract(day from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as day, 
extract(week from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as week, 
extract(month from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as month, 
extract(year from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as year, 
extract(dayofweek from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as weekday 
from staging_events where page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
