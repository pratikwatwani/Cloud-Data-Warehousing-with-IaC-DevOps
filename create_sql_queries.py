import configparser
import logging


logging.basicConfig(format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging = logging.getLogger(__name__)

# Setup parameters
configFile = 'dwh.cfg'
config = configparser.ConfigParser()
config.read(configFile)

log_data = config.get('S3','log_data')
song_data = config.get('S3','song_data')
log_jsonpath = config.get('S3','log_jsonpath')
dwh_role_arn = config.get('IAM_ROLE','dwh_role_arn')


# Drop staging data tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# Drop tables
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"



# Create staging area tables
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth VARCHAR(20),
    firstName TEXT,
    gender CHAR(1),
    itemInSession INT,
    lastName TEXT,
    length DECIMAL,
    level VARCHAR(4),
    location TEXT,
    method VARCHAR(3),
    page TEXT,
    registration DECIMAL,
    sessionId INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId INT);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id TEXT,
    artist_latitude DECIMAL,
    aritist_longitude DECIMAL, 
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration DECIMAL,
    year INT);
""")


# Creating Fact and Dimension Tables
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time BIGINT,
    user_id INT,
    level VARCHAR(4), 
    song_id TEXT,
    artist_id TEXT, 
    session_id INT, 
    location TEXT, 
    user_agent TEXT,
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
    (user_id INT PRIMARY KEY, 
    firstName TEXT, 
    lastName TEXT,
    gender CHAR(1), 
    level VARCHAR(4));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
    (song_id VARCHAR(40) PRIMARY KEY, 
    title TEXT, 
    artist_id TEXT,
    year INT, 
    duration DECIMAL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
    (artist_id TEXT PRIMARY KEY, 
    name TEXT, 
    location TEXT,
    latitude DECIMAL, 
    longitude DECIMAL);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
    (start_time TIMESTAMP PRIMARY KEY, 
    hour INT, 
    day INT,
    week INT, 
    month INT, 
    year INT, 
    weekday INT);
""")


# Copying data to staging area
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2'
""").format(log_data, dwh_role_arn)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2'
""").format(song_data, dwh_role_arn)



# Copying data to fact and dimension tables
songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT ts, userId, level, song_id, artist_id, sessionId, location, userAgent
    FROM (
            SELECT se.ts, se.userId, se.level, sa.song_id, sa.artist_id, se.sessionId, se.location, se.userAgent
            FROM staging_events se
            JOIN
            (SELECT songs.song_id, artists.artist_id, songs.title, artists.name,songs.duration
            FROM songs
            JOIN artists
            ON songs.artist_id = artists.artist_id) AS sa
            ON (sa.title = se.song
            AND sa.name = se.artist
            AND sa.duration = se.length)
    WHERE se.page = 'NextSong');
""")

user_table_insert = ("""INSERT INTO users (user_id, firstName, lastName, gender, level)
    SELECT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude)
    FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, 
    DATE_PART (HOUR, stats.start_time), 
    DATE_PART (DAY, stats.start_time),
    DATE_PART (WEEK, stats.start_time), 
    DATE_PART (MONTH, stats.start_time),
    DATE_PART (YEAR, stats.start_time), 
    DATE_PART (WEEKDAY, stats.start_time) 
    FROM
    (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_Events) as stats;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]