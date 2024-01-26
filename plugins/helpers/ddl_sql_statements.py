class SqlQueriesDDL:
    create_staging_events_table = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist          VARCHAR(254),
            auth            VARCHAR(254),
            firstName       VARCHAR(254),
            gender          CHAR(1),
            itemInSession   INTEGER,
            lastName        VARCHAR(254),
            length          FLOAT,
            level           CHAR(4),
            location        VARCHAR(254),
            method          VARCHAR(254),
            page            VARCHAR(254),
            registration    FLOAT,
            sessionId       INTEGER,
            song            VARCHAR(254),
            status          INTEGER,
            ts              BIGINT,
            userAgent       VARCHAR(254),
            userId          INTEGER
        );
    """

    create_staging_songs_table = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id       VARCHAR(254),
            artist_latitude FLOAT,
            artist_location VARCHAR(254),
            artist_longitude FLOAT,
            artist_name     VARCHAR(254),
            duration        FLOAT,
            num_songs       INTEGER,
            song_id         VARCHAR(254),
            title           VARCHAR(254),
            year            INTEGER
        );
    """

    create_artists_table = """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id       INTEGER         NOT NULL,
            name            VARCHAR(254)    NOT NULL,
            location        VARCHAR(254)    NULL,
            latitude        FLOAT           NULL,
            longitude       FLOAT           NULL,
            UNIQUE          (artist_id),
            PRIMARY KEY     (artist_id)
        )
        DISTSTYLE ALL
        SORTKEY (name);
    """

    create_songs_table = """
        CREATE TABLE IF NOT EXISTS songs (
            song_id         INTEGER         NOT NULL,
            title           VARCHAR(254)    NOT NULL,
            artist_id       INTEGER         NOT NULL,
            year            INTEGER         NULL,
            duration        FLOAT           NULL,
            PRIMARY KEY     (song_id),
            UNIQUE          (song_id),
            FOREIGN KEY     (artist_id)     REFERENCES  artists (artist_id)
        )
        DISTSTYLE ALL
        SORTKEY (title, year);
    """

    create_time_table = """
        CREATE TABLE IF NOT EXISTS time (
            start_time      TIMESTAMP       NOT NULL,
            year            INTEGER         NOT NULL,
            month           INTEGER         NOT NULL,
            day             INTEGER         NOT NULL,
            hour            INTEGER         NOT NULL,
            week            INTEGER         NOT NULL,
            weekday         INTEGER         NOT NULL,
            UNIQUE          (start_time),
            PRIMARY KEY     (start_time)
        )
        DISTSTYLE EVEN
        SORTKEY (start_time);
    """

    create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            user_id         INTEGER         NOT NULL,
            first_name      VARCHAR(254)     NOT NULL,
            last_name       VARCHAR(254)     NOT NULL,
            gender          CHAR(1)         NOT NULL,
            level           CHAR(4)         NOT NULL,
            UNIQUE          (user_id),
            PRIMARY KEY     (user_id)
        )
        DISTSTYLE ALL
        SORTKEY (last_name, first_name, gender, level);
    """

    create_songplays_table = """
        CREATE TABLE IF NOT EXISTS songplays (
            session_id      INTEGER         NOT NULL,
            songplay_id     INTEGER         NOT NULL,
            start_time      TIMESTAMP       NOT NULL,
            artist_id       INTEGER         NOT NULL,
            song_id         INTEGER         NOT NULL,
            user_id         INTEGER         NOT NULL,
            level           CHAR(4)         NOT NULL,
            location        VARCHAR(254)    NOT NULL,
            user_agent      VARCHAR(254)    NOT NULL,
            PRIMARY KEY (session_id, songplay_id),
            UNIQUE (session_id, songplay_id),
            FOREIGN KEY (start_time)        REFERENCES  time (start_time),
            FOREIGN KEY (user_id)           REFERENCES  users (user_id),
            FOREIGN KEY (artist_id)         REFERENCES  artists (artist_id),
            FOREIGN KEY (song_id)           REFERENCES  songs (song_id)
        )
        DISTSTYLE EVEN
        SORTKEY (start_time);
    """
