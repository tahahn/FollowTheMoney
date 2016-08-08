#FTM 

FTM is a ETL(Extract,Transform and Load) program written in python. It extracts campaign contribution data for state senators and legislators from the Follow The Money API and houses the data in a datawarehouse.

##Getting Started

###Prerequisites

Python 2.7

For FTM to run the following Python Modules are required.

- MySQLdb
- json
- datetime
- time
- re
- urllib
- pickle
- pydoc*

*pydocs is not required for the program to run but is reccomended to read the documentation provided. 

Additionally the ETL houses the extracted data in a series of JSON files before uploading data to a databse. As a result 15mb of disk-space is required for storage of the JSON file and a mySQL databse is required. 



