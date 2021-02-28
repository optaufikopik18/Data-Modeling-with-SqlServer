songplays_create_table = "create table songplays(songplay_id bigint identity(1,1) primary key, start_time datetime, user_id bigint, level varchar(10), song_id varchar(25), artist_id varchar(25), session_id bigint, location varchar(250), user_agent varchar(250))"
users_create_table = "create table users(user_id bigint, first_name varchar(250), last_name varchar(250), gender char(1), level varchar(15))"
songs_create_table = "create table songs(song_id varchar(25), title varchar(250), artist_id varchar(25), year int, duration numeric(18,5))"
artists_create_table = "create table artists(artist_id varchar(25), name varchar(250), location varchar(250), latitude numeric(18,5), longitude numeric(18,5))"
time_create_table = "create table time(start_time datetime, hour int, day int, week int, month int, year int, weekday int)"

songplays_drop_table = "drop table if exists songplays"
users_drop_table = "drop table if exists users"
songs_drop_table = "drop table if exists songs"
artists_drop_table = "drop table if exists artists"
time_drop_table = "drop table if exists time"

songplays_insert_data = "insert into songplays values(?,?,?,?,?,?,?,?)"
users_insert_data = "insert into users values(?,?,?,?,?)"
songs_insert_data = "insert into songs values(?,?,?,?,?)"
artists_insert_data = "insert into artists values(?,?,?,?,?)"
time_insert_data = "insert into time values(?,?,?,?,?,?,?)"

create_table_queries = [songplays_create_table, users_create_table, songs_create_table, artists_create_table, time_create_table]
drop_table_queries = [songplays_drop_table, users_drop_table, songs_drop_table,artists_drop_table, time_drop_table]


