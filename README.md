# Cloud Data Warehousing with IaC DevOps

## Description
Building an ETL pipeline that extracts data from Amazon Web Services S3, stages them in Redshift Cluster, and transforms data into a set of dimensional tables for the analytics teams. The Redshift cluster is created and maintained using the Infrastructure as Code paradigm and policies.

## Directory Structure

    ├── README.md
    ├── dwh.cfg
    ├── run.sh
    ├── analysis
        └── Exploratory Data Analysis.ipynb
        └── Performance Measure.ipynb
    ├── modules
        └── create_database.py
        └── create_tables.py
        └── sql_queries.py
        └── etl.py
        └── __init__.py

## Running instructions
There is a shell script in the root directory that will call upon the required python scripts. Just use the shell file to run the program, logging will aid you in every step of execution.

Run: `sh run.sh`

## Exploratory Data Analysis
### Data points
<table>
    <tr>
        <th>table</th>
        <th>count</th>
    </tr>
    <tr>
        <td>time</td>
        <td>16112</td>
    </tr>
    <tr>
        <td>staging events</td>
        <td>8056</td>
    </tr>
    <tr>
        <td>songplays</td>
        <td>320</td>
    </tr>
    <tr>
        <td>staging songs</td>
        <td>14896</td>
    </tr>
    <tr>
        <td>users</td>
        <td>7770</td>
    </tr>
    <tr>
        <td>artists</td>
        <td>14896</td>
    </tr>
    <tr>
        <td>songs</td>
        <td>14896</td>
    </tr>
</table>

### Queries
#### 1. Streaming by Location (Top 10)
<table>
    <tr>
        <th>location</th>
        <th>count</th>
    </tr>
    <tr>
        <td>San Francisco-Oakland-Hayward, CA</td>
        <td>41</td>
    </tr>
    <tr>
        <td>Portland-South Portland, ME</td>
        <td>31</td>
    </tr>
    <tr>
        <td>Lansing-East Lansing, MI</td>
        <td>29</td>
    </tr>
    <tr>
        <td>Waterloo-Cedar Falls, IA</td>
        <td>20</td>
    </tr>
    <tr>
        <td>Tampa-St. Petersburg-Clearwater, FL</td>
        <td>18</td>
    </tr>
    <tr>
        <td>Sacramento--Roseville--Arden-Arcade, CA</td>
        <td>17</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>17</td>
    </tr>
    <tr>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
        <td>15</td>
    </tr>
    <tr>
        <td>Lake Havasu City-Kingman, AZ</td>
        <td>13</td>
    </tr>
    <tr>
        <td>Janesville-Beloit, WI</td>
        <td>11</td>
    </tr>
</table>

#### 2. Streaming by Platform
<table>
    <tr>
        <th>platforms</th>
        <th>count</th>
    </tr>
    <tr>
        <td>Macintosh</td>
        <td>139</td>
    </tr>
    <tr>
        <td>Windows NT 5.1</td>
        <td>53</td>
    </tr>
    <tr>
        <td>X11</td>
        <td>46</td>
    </tr>
    <tr>
        <td>Windows NT 6.1</td>
        <td>40</td>
    </tr>
    <tr>
        <td>Windows NT 6.3</td>
        <td>32</td>
    </tr>
    <tr>
        <td>iPhone</td>
        <td>9</td>
    </tr>
    <tr>
        <td>compatible</td>
        <td>1</td>
    </tr>
</table>

#### 3. Total album length for each artist (Top 10)
<table>
    <tr>
        <th>artist</th>
        <th>total_album_minutes</th>
    </tr>
    <tr>
        <td>Jean Grae</td>
        <td>3006</td>
    </tr>
    <tr>
        <td>Freddie Hubbard</td>
        <td>2882</td>
    </tr>
    <tr>
        <td>Aphex Twin</td>
        <td>2657</td>
    </tr>
    <tr>
        <td>Enigma</td>
        <td>2644</td>
    </tr>
    <tr>
        <td>Madonna</td>
        <td>2513</td>
    </tr>
    <tr>
        <td>Opeth</td>
        <td>2479</td>
    </tr>
    <tr>
        <td>Mortiis</td>
        <td>2445</td>
    </tr>
    <tr>
        <td>Herbie Hancock</td>
        <td>2421</td>
    </tr>
    <tr>
        <td>Simple Minds</td>
        <td>2343</td>
    </tr>
    <tr>
        <td>Stephan Micus</td>
        <td>2298</td>
    </tr>
</table>

#### 4. Streaming by week
<table>
    <tr>
        <th>week</th>
        <th>count</th>
    </tr>
    <tr>
        <td>44</td>
        <td>972</td>
    </tr>
    <tr>
        <td>45</td>
        <td>2982</td>
    </tr>
    <tr>
        <td>46</td>
        <td>4564</td>
    </tr>
    <tr>
        <td>47</td>
        <td>4074</td>
    </tr>
    <tr>
        <td>48</td>
        <td>3520</td>
    </tr>
</table>

#### 5. Service type weight
<table>
    <tr>
        <th>level</th>
        <th>count</th>
    </tr>
    <tr>
        <td>paid</td>
        <td>262</td>
    </tr>
    <tr>
        <td>free</td>
        <td>58</td>
    </tr>
</table>

#### 6. Type(Level) of user by Gender
<table>
    <tr>
        <th>gender</th>
        <th>level</th>
        <th>sum</th>
    </tr>
    <tr>
        <td>F</td>
        <td>paid</td>
        <td>10972270</td>
    </tr>
    <tr>
        <td>F</td>
        <td>free</td>
        <td>318801</td>
    </tr>
    <tr>
        <td>M</td>
        <td>free</td>
        <td>386760</td>
    </tr>
    <tr>
        <td>M</td>
        <td>paid</td>
        <td>993345</td>
    </tr>
</table>

#### 7. Top artists by each location (Top 10)
<table>
    <tr>
        <th>location</th>
        <th>artist_name</th>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Dr. Alban</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>The Smiths</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Binary Star</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>The Human League</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>The Presets</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Limi-T 21</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Goldfrapp</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Arctic Monkeys</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Fergie</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>Jack Johnson</td>
    </tr>
</table>

#### 8. Average album length over years
<table>
    <tr>
        <th>year</th>
        <th>average_album_length</th>
    </tr>
    <tr>
        <td>0</td>
        <td>251.89</td>
    </tr>
    <tr>
        <td>1927</td>
        <td>185.00</td>
    </tr>
    <tr>
        <td>1944</td>
        <td>142.00</td>
    </tr>
    <tr>
        <td>1952</td>
        <td>133.00</td>
    </tr>
    <tr>
        <td>1954</td>
        <td>234.67</td>
    </tr>
</table>
    
## Query Performance
### Loading Time Comparison (in seconds)

    Table         |    W/ Distribution Style     |     W/O Distribution Style
    ------------------------------------------------------------------------------
    staging_events            15.15                            1.92 
    staging_songs            169.99                           181.38 
    songs                     0.73                             0.79 
    users                     0.65                             0.67 
    artists                   0.67                             0.68 
    time                      0.51                             0.78 
    songplays                 6.08                             1.21 

### Execution Time Comparison (in m seconds)

    Table         |    W/ Distribution Style     |     W/O Distribution Style
    ------------------------------------------------------------------------------
    QUERY 1                    261                             437
    QUERY 2                    269                             415
    QUERY 3                    276                             556 
    QUERY 4                    256                             4680
    QUERY 5                    263                             360
    QUERY 6                    264                             508 
    QUERY 7                    285                             564
    QUERY 8                    251                             413
        

    
    
