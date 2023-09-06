import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1),
    artist VARCHAR(255),
    auth VARCHAR(255),
    first_name VARCHAR(255),
    gender VARCHAR(1),
    item_in_session INT,
    last_name VARCHAR(255),
    length NUMERIC,
    level VARCHAR(100),
    location VARCHAR(255),
    method VARCHAR(10),
    page VARCHAR(50),
    registration NUMERIC,
    session_id INT,
    song VARCHAR(255),
    status INT, --HTTP Status code
    ts NUMERIC,
    user_agent TEXT,
    user_id VARCHAR(255),
    PRIMARY KEY (event_id)
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR(255),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INT,
    PRIMARY KEY (song_id)
)
""")

# Fact table
songplay_table_create = ("""
CREATE TABLE fact_songplays(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP REFERENCES dim_time(start_time), 
    user_id VARCHAR(255) REFERENCES dim_users(user_id), 
    level VARCHAR(100), 
    song_id VARCHAR(255) REFERENCES dim_songs(song_id), 
    artist_id VARCHAR(255) REFERENCES dim_artists(artist_id), 
    session_id INT, 
    location VARCHAR(255),
    user_agent TEXT,
    PRIMARY KEY(songplay_id)
)
""")

# Dimention tables
user_table_create = ("""
CREATE TABLE dim_users(
    user_id VARCHAR(255), 
    first_name VARCHAR(255), 
    last_name VARCHAR(255), 
    gender VARCHAR(1), 
    level VARCHAR(100),
    PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE dim_songs(
    song_id VARCHAR(255),
    title VARCHAR(255), 
    artist_id VARCHAR(255) REFERENCES dim_artists(artist_id), 
    year INT, 
    duration DOUBLE PRECISION,
    PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE dim_artists(
    artist_id VARCHAR(255),
    name VARCHAR(255), 
    location VARCHAR(255),
    lattitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION,
    PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE dim_time(
    start_time TIMESTAMP, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT,
    PRIMARY KEY (start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON {}
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON 'auto'
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES
# NOTE: Use DISTINCT to remove duplicate records
songplay_table_insert = ("""
INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM staging_events as e
LEFT JOIN songs_staging as s ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page= 'NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    e.user_id,
    e.first_name,
    e.last_name,
    e.gender,
    e.level
FROM staging_events as e
WHERE e.page= 'NextSong'
""")

song_table_insert = ("""
INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    s.song_id
    s.title
    s.artist_id
    s.year
    s.duration
FROM songs_staging as s
""")

artist_table_insert = ("""
INSERT INTO dim_artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    s.artist_id,
    s.artist_name,
    s.artist_location,
    s.artist_latitude,
    s.artist_longitude
FROM songs_staging as s
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    time.start_time,
    EXTRACT(HOUR FROM time.start_time) AS hour,
    EXTRACT(DAY FROM time.start_time) AS day,
    EXTRACT(WEEK FROM time.start_time) AS week,
    EXTRACT(MONTH FROM time.start_time) AS month,
    EXTRACT(YEAR FROM time.start_time) AS year,
    EXTRACT(weekday FROM time.start_time) AS weekday
FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time
    FROM staging_events as e
    WHERE e.page= 'NextSong') as time
""")

# QUERY LISTS

# We will creat tables which not had fk first and creat tables had fk last.
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        artist_table_create, time_table_create, song_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert,
                        time_table_insert, song_table_insert, songplay_table_insert]
