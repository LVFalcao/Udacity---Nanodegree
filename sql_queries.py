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
staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstname VARCHAR,
        gender VARCHAR,
        iteminsession INT,
        lastname VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionid INT,
        song VARCHAR,
        status INT,
        ts INT,
        useragent VARCHAR,
        userid INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT 
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id  INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL DISTKEY,
        level VARCHAR, 
        song_id INT NOT NULL,
        artist_id INT NOT NULL,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
     SORTKEY (start_time, song_id, artist_id);
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INT NOT NULL PRIMARY KEY DISTKEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        user_id VARCHAR NOT NULL
    )
    SORTKEY (user_id, level);
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id INT NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,         
        year INT NOT NULL,
        duration FLOAT
    )
     SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
     SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP  NOT NULL PRIMARY KEY DISTKEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday VARCHAR(10) NOT NULL
    )
     SORTKEY (start_time);
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY {} 
    FROM {} 
    IAM_ROLE '{}'
    JSON {};
""").format('staging_events', config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY {} 
    FROM {}        
    IAM_ROLE '{}'          
    JSON 'auto';     
""").format('staging_songs', config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))



# FINAL TABLES
songplay_table_insert = ("""
      INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
      SELECT DISTINCT
          events.ts             AS start_time, 
          events.userId         AS user_id, 
          events.level          AS level, 
          songs.song_id         AS song_id, 
          songs.artist_id       AS artist_id, 
          events.sessionId      AS session_id, 
          events.location       AS location, 
          events.userAgent      AS user_agent
      FROM staging_events events
        INNER JOIN staging_songs songs   
            ON (events.song = songs.title AND events.artist = songs.artist_name)
     WHERE events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM stage_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
    SELECT start_time, 
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time), 
        extract(month from start_time),
        extract(year from start_time), 
        extract(dayofweek from start_time)
    FROM songplays;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
