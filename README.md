# Music Streaming Analysis Using Spark Structured APIs

## Overview

This project analyzes user listening behavior and music trends using Apache Spark Structured APIs.
By processing structured data from a fictional music streaming platform, we gain insights into:

-User genre preferences

-Song popularity

-Listener engagement and loyalty

-Nighttime listening habits

The assignment demonstrates the use of Spark for data loading, cleaning, aggregation, joins, and transformations to extract meaningful insights from structured CSV datasets.
## Dataset Description
Two CSV files are used as input:

**1. listening_logs.csv**
Contains records of user activity:

*user_id – Unique ID of the user

*song_id – Unique ID of the song

*timestamp – Date and time when the song was played (e.g., 2025-03-23 14:05:00)

*duration_sec – Duration in seconds for which the song was played

**2. songs_metadata.csv**
Contains metadata about the songs:

*song_id – Unique ID of the song

*title – Title of the song

*artist – Name of the artist

*genre – Genre of the song (Pop, Rock, Jazz, etc.)

*mood – Mood category of the song (Happy, Sad, Energetic, Chill)
## Repository Structure
```
.
├── input_generator.py        # Script to generate sample input datasets
├── main.py                   # Main Spark job containing all tasks
├── listening_logs.csv        # Input dataset: user listening logs
├── songs_metadata.csv        # Input dataset: songs metadata
├── outputs/                  # Directory containing task results
└── README.md                 # Project documentation
```
## Output Directory Structure
```
outputs/
├── user_favorite_genres/        # Task 1 results
├── avg_listen_time_per_song/    # Task 2 results
├── genre_loyalty_scores/        # Task 3 results
└── night_owl_users/             # Task 4 results
```
## Tasks and Outputs

**1. User’s Favorite Genre**

*Identifies the most-listened genre for each user.

*Output saved in: outputs/user_favorite_genres/

**2. Average Listen Time per Song**

*Computes the average duration (in seconds) for each song.

*Output saved in: outputs/avg_listen_time_per_song/

**3. Genre Loyalty Score**

*Calculates the proportion of plays in a user’s top genre.

*Filters users with a loyalty score > 0.8.

*Output saved in: outputs/genre_loyalty_scores/

**4. Night Owl Users**

*Identifies users who frequently listen between 12 AM and 5 AM.

*Output saved in: outputs/night_owl_users/
## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 datagen.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     python3 main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```
## Sample Datasets
**1. listening_logs.csv**
| user_id | song_id | timestamp           | duration_sec |
| ------- | ------- | ------------------- | ------------ |
| U1      | S1      | 2025-03-23 14:05:00 | 210          |
| U1      | S2      | 2025-03-23 22:45:00 | 180          |
| U2      | S3      | 2025-03-24 01:15:00 | 240          |
| U2      | S1      | 2025-03-24 12:30:00 | 200          |
| U3      | S2      | 2025-03-24 03:50:00 | 150          |
| U3      | S4      | 2025-03-24 18:10:00 | 300          |
| U4      | S5      | 2025-03-25 00:40:00 | 220          |
| U4      | S3      | 2025-03-25 16:05:00 | 190          |
**2. songs_metadata.csv**
| song_id | title             | artist       | genre | mood      |
| ------- | ----------------- | ------------ | ----- | --------- |
| S1      | Lost Stars        | Adam Levine  | Pop   | Happy     |
| S2      | Bohemian Rhapsody | Queen        | Rock  | Energetic |
| S3      | Take Five         | Dave Brubeck | Jazz  | Chill     |
| S4      | Shape of You      | Ed Sheeran   | Pop   | Happy     |
| S5      | Someone Like You  | Adele        | Pop   | Sad       |
## Sample Outputs
**Task 1: User Favorite Genres (output/user_favorite_genres/)**
| user_id | genre | play_count |
| ------- | ----- | ---------- |
| U1      | Pop   | 1          |
| U1      | Rock  | 1          |
| U2      | Pop   | 1          |
| U2      | Jazz  | 1          |
| U3      | Rock  | 1          |
| U3      | Pop   | 1          |
| U4      | Pop   | 1          |
| U4      | Jazz  | 1          |
**Task 2: Average Listen Time (output/avg_listen_time_per_song/)**
| song_id | avg_duration_seconds | 
| ------- | -------------------- | 
| S1      | 205.0                |
| S2      | 165.0                | 
| S3      | 215.0                | 
| S4      | 300.0                |
| S5      | 220.0                |
**Task 3: Genre Loyalty Scores (output/genre_loyalty_scores/)**
| user_id  | loyalty_score |
| -------- | ------------- |
| *(none)* | *(empty)*     |
**Task 4: Night Owl Users (output/night_owl_users/)**
| user_id |
| ------- | 
| U2      | 
| U3      |
| U4      | 

## Errors and Resolutions
1. Header Issues

Error: Spark reads column names as data (e.g., first row becomes data).

Cause: header=True not set.

Resolution:
```
df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
```
2. Schema Mismatches

Error: Duration column (duration_sec) gets read as string instead of int.

Cause: Missing inferSchema=True.

Resolution:
```
df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
df.printSchema()
```
## Conclusion
This project showed how Spark Structured APIs can analyze music streaming data effectively. We identified users’ favorite genres, calculated average listen times, measured genre loyalty, and detected night owl listeners. The exercise highlights Spark’s strength in handling joins, aggregations, and time-based analysis, and can be extended to larger datasets for deeper insights.
