#FTM 

FTM is an ETL(Extract,Transform and Load) program written in python. It extracts campaign contribution data for state senators and legislators from the Follow The Money API and stores the data in a datawarehouse.

##Getting Started

###Prerequisites

- Python 2.7
- mySQL
- 15mb of disk-space

For FTM to run the following python modules are required.

- MySQLdb
- json
- datetime
- time
- re
- urllib
- pickle
- pydoc*

###Installation

####Windows 7 & OS X

1. Install Python 2.7(https://www.python.org/downloads/)
2. If hosting locally install mySQL Server 5.7(http://dev.mysql.com/downloads/mysql/)
3. Install pre-requisite modules using pip install moduleName

#### Setup

1. Sign up for a free account at http://www.followthemoney.org/ and edit the file api.txt with your api-key.
2. If you have decided to locally host the mySQL database start the mySQL Server
2. Edit the database.txt file with your host address, username,password.The database should be left as atlas_ftm
3. Create the atlas_ftm database in your mySQL Installation
4. Create the following tables 
  1. cand_dim_candidate
    - Columns
      * Candidate_ID: int(11) Auto Increment, Primary Key
      * full_name: varchar(45)
      * first_name: varchar(45)
      * middle_name: varachar(45)
      * last_name: varchar(45)
      * timestamp: datetime
  2. cand_fact
    - Columns
      * Fact_ID: int(11) Auto Increment, Primary Key
      * Candidate_ID: int(11)
      * Cylce_ID: int(11)
      * Party_ID : int(11)
      * Geo_ID: int(11)
      * timestamp: datetime
      * contribution: int(11)
      * State: varchar(45)
      * Office_ID: int(11)
    - Indexes
      * Candidate_ID
      * Cycle_ID
      * Party_ID
      * Geo_ID
      * Contribution
  3. ftm_dim_cycle
    - Columns
      * Cycle_ID: int(11) Auto Increment, Primary Key
      * Cycle: int(11)
      * timestamp: datetime
      * Cycle_type: varchar(45)
    - Indexes
      * cycle: cycle & cycle_type (unique)
  4. ftm_dim_geo
    - Columns
      * State: varchar(3)
      * District: varchar(45)
      * Geo_ID: int(11) Auto Increment, Primary Key
      * timestamp: datetime
    - Indexes
      * geo: State & District (unique)
  5. ftm_dim_office
    - Columns 
      * Office_ID: int(11) Auto Increment, Primary Key
      * Office: varchar(400)
      * Office_Code varchar(45)
      * timestamp: datetime
    - Indexes
      * office: office (unique)  
  6. ftm_dim_party
    - Columns
      * Party_ID: int(11) Auto Increment, Primary Key
      * timestamp: datetime
      * General_Party: varchar(45)
      * Specific_Party: varchar(45)
    - Indexes
      * party: General_Party & Specific_Party (unique)
  7. ftm_inc
    - Columns
      * Record_ID: int(11) Auto Increment, Primary Key
      * Full_Name: varchar(45)
      * First_Name: varchar(45)
      * Middle_Name: varchar(45)
      * Last_Name: varchar(45)
      * Geo_ID: int(11)
      * Cycle_ID: int(11)
      * Candidate_ID: int(11)
      * Office_ID: int(11)
      * Party_ID: int(11)
      * State: varchar(45)
      * contribution: int(11)
      * Cycle: int(11)
      * Office: varchar(400)
      * General_Party: varchar(45)
      * Specific_Party: varchar(45)
      * Cycle_Type:varchar(45)
      * District: int(11)
      * Office_Code: varchar(45)
      * Election_Status: varchar(45)
      * Incumbency_Status: varchar(45)
      * timestamp: datetime
    - Indexes
      * Cycle
      * Full_Name
      * State
      * Contribution
      * Cycle_ID
      * Geo_ID
  8. ftm_update
    - State: varchar(45) Primary Key
    - Update_Date: datetime
    - Page_Num: int(11)
    - Status: varchar(45)
    - Max_Page: int(11)

###Usage

To run the program type 
```
    python FollowTheMoneyETL.py
```

Then when prompoted "Lawmaker or Candidate" choose "Candidate"

Note: This program has been exclusivley tested on Windows 7 using the windows command line. There is no guarantee that it will function if run from the linux or OS X terminal. 

*pyDocs are not required for the program to run but are reccomendation to read the accompanying documentation. 


