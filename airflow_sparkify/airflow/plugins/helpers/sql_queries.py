class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;""")

    user_table_insert = ("""
        SELECT DISTINCT userId,
                        firstName,
                        lastName,
                        gender,
                        level
        FROM staging_events
        WHERE userId is NOT NULL """)

    song_table_insert = ("""
        SELECT DISTINCT song_id,
                       title,
                       artist_id,
                       year,
                       duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
        SELECT DISTINCT artist_id,
                        artist_name,
                        artist_location,
                        artist_latitude,
                        artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
        SELECT DISTINCT start_time,
               EXTRACT(hour FROM start_time),
               EXTRACT(day FROM start_time),
               EXTRACT(week FROM start_time),
               EXTRACT(month FROM start_time),
               EXTRACT(year FROM start_time),
               EXTRACT(weekday FROM start_time)
        FROM songplays
            WHERE start_time IS NOT NULL;

    """)

    # CREATE TABLES

    staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
    (
        id INT IDENTITY(1,1),
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song varchar DISTKEY,
        status INT,
        ts VARCHAR SORTKEY,
        userAgent VARCHAR,
        userId INT
    );""")

    staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (
        id INT IDENTITY(1,1),
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR DISTKEY,
        duration FLOAT,
        year INT
    );""")

    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
        (
            songplay_id VARCHAR NOT NULL PRIMARY KEY ,
            start_time TIMESTAMP NOT NULL SORTKEY,
            user_id INT NOT NULL,
            level VARCHAR, 
            song_id VARCHAR DISTKEY,
            artist_id VARCHAR,
            session_id VARCHAR,
            location VARCHAR, 
            user_agent VARCHAR
        );
        """)

    user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS users 
    (
        user_id INT PRIMARY KEY NOT NULL SORTKEY, 
        first_name VARCHAR, 
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
        );
        """)

    song_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR PRIMARY KEY NOT NULL DISTKEY,
        title VARCHAR,
        artist_id VARCHAR SORTKEY,
        year INT,
        duration FLOAT
    );
    """)

    artist_table_create = (""" 
    CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id VARCHAR PRIMARY KEY NOT NULL SORTKEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT, 
        longitude FLOAT
    );
    """)

    time_table_create = (""" 
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP PRIMARY KEY NOT NULL SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT, 
        weekday INT
    );
    """)