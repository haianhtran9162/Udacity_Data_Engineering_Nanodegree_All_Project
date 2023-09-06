# Document Process

## Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

As an introduction part said the purpose of this startup want to understand what song users listening to, and i think the vision here is build one recommend system for Sparkify music app.

The analytical goals is the information of list song played by each user like song name, artist, location, device,... very details.

## State and justify your database schema design and ETL pipeline.

About create query

<ol>
    <li>I choose the serial type for "songplay_id" in songplays table because when i check the data, and not found the "songplay_id" in there to insert to db so simple i think the "songplay_id" need auto increase.</li>
    <li>I choose the bigint for "start_time" in songplays table because the "start_time" data is milliseconds not time and the value is bigger than int range so i choose bigint.</li>
    <li>I choose numeric for "duration" on songs table and "latitude", "longitude" on artists table because they had floating-point.</li>
    <li>About primary key, i choose: "songplay_id", "user_id", "song_id", "artist_id", "start_time" because they are unique.</li>
</ol>

About insert query

<ol>
    <li>I use "ON CONFLICT(col) DO NOTHING" for insert query into songs, artists, time for remove the duplicate row had same primary key.</li>
    <li>Insert about users is a little more special, because the user had level account so if duplicate we need update the level of the account. So I use "ON CONFLICT(user_id) DO UPDATE SET level = excluded.level" for update level only for this table.</li>
    <li>About songplays I not use "ON CONFLICT" because the primary key of this table is serial.</li>
</ol>

## How to run

Step 1: Run creat_tables.py to creat database and table
```
python create_tables.py
```
or
```
python3 create_tables.py
```
Step 2: Run etl.py to run ETL Pipeline
```
python etl.py
```
or
```
python3 etl.py
```

Finish

## Folder structure

- **data** data folder: user log data and song information data.
- **creat_table.py** script with flow connect db -> drop table -> creat table. You can run this file to reset your db.
- **etl.ipynb** the notebook give details abour ETL pipeline.
- **etl.py** the ETL pipeline.
- **README.md** information about the project.
- **sql_queries.py** had all sql query db project use.
- **test.ipynb** the notebook test the connect db, and check the data in the table after insert.
