#Title: Follow The Money ETL
#Author: Travis Hahn 
#Version: 0.02
#Date: 06/30/2016
'''
The Follow The Money ETL extracts data concerning contributions to lawmakers
and candidates at the state level using the Follow The Money API. Additionally
this module inserts the extracted data into a database allowing for front-end
access. 
'''

import MySQLdb,json,datetime,time,re,urllib,pickle

def data_extract(Entity,page,state):
  '''
  The Data-Extract function extracts the contribution data from the Follow The Money website
  via API calls

  Parameters:
    Entity: (String) The Entity the data to be collected is about (Either a Lawmaker or a Candidate)
    page: (int) the page number to be extracted
    state:(String) the state currently being inserted/updated
  '''
  api_key='eca19fed32fef9c993981829b74deb3b'
  p_number=str(page)
  api_dict = {
  'Lawmaker':'meh',
  'Candidate':'meh2'
  }
  api_url="http://api.followthemoney.org/?f-core=1&mode=json&y=2015,2016&APIKey="+str(api_key)+"&s="+state+"&gro=y,c-t-eid,law-oc,law-p&so=law-eid&sod=0&p="+p_number
  api_url2='http://api.followthemoney.org/?s='+state+'&f-core=1&c-exi=1&y=2015,2016&gro=c-t-id&APIKey='+api_key+'&mode=json&so=c-t-id&p='+p_number
  api_dict['Lawmaker']=api_url
  api_dict['Candidate']=api_url2
  #api_url="http://api.followthemoney.org/entity.php?eid=6688494&APIKey=eebe0a5bc8970cbab68104c1759e6cb6&mode=json"
  #print api_dict[Entity]
  response=urllib.urlopen(api_url2)
  data=json.loads(response.read())
  file_ad=state+'Cp'+p_number+".json"
  with open(file_ad,'w') as outfile:
    json.dump(data,outfile)
     


def NumToDIM(x):
  """
  Associates database dimensions with numerical values for iteration

  Parameters:
    x: (int) a number in a loop that is to be associated with a dimension ex(0->cycle)
  """
  return {
  0: 'cycle',
  1: 'geo',
  2: 'office',
  3: 'party'
  }.get(x)

def NumToState(x):
  """
  Associates state names with numerical values for iteration

  Parameters:
    x: (int) a number in a loop that is to be associated with a state ex(0->AL)
  """
  return {
  0: 'AL',
  1: 'AK',
  2:'AZ',
  3:'AR',
  4:'CA',
  5:'CO',
  6:'CT',
  7:'DE',
  8:'FL',
  9:'GA',
  10:'HI',
  11:'ID',
  12:'IL',
  13:'IN',
  14:'IA',
  15:'KS',
  16:'KY',
  17:'LA',
  18:'ME',
  19:'MD',
  20:'MA',
  21:'MI',
  22:'MN',
  23:'MS',
  24:'MO',
  25:'MT',
  26:'NE',
  27:'NV',
  28:'NH',
  29:'NJ',
  30:'NM',
  31:'NY',
  32:'NC',
  33:'ND',
  34:'OH',
  35:'OK',
  36:'OR',
  37:'PA',
  38:'RI',
  39:'SC',
  40:'SD',
  41:'TN',
  42:'TX',
  43:'UT',
  44:'VT',
  45:'VA',
  46:'WA',
  47:'WV',
  48:'WI',
  49:'WY',
  }.get(x)

def statetonum(x):
   return {
    'AL':0,
    'AK':1,
    'AZ':2,
    'AR':3,
    'CA':4,
    'CO':5,
    'CT':6,
    'DE':7,
    'FL':8,
    'GA':9,
    'HI':10,
    'ID':11,
    'IL':12,
    'IN':13,
    'IA':14,
    'KS':15,
    'KY':16,
    'LA':17,
    'ME':18,
    'MD':19,
    'MA':20,
    'MI':21,
    'MN':22,
    'MS':23,
    'MO':24,
    'MT':25,
    'NE':26,
    'NV':27,
    'NH':28,
    'NJ':29,
    'NM':30,
    'NY':31,
    'NC':32,
    'ND':33,
    'OH':34,
    'OK':35,
    'OR':36,
    'PA':37,
    'RI':38,
    'SC':39,
    'SD':40,
    'TN':41,
    'TX':42,
    'UT':43,
    'VT':44,
    'VA':45,
    'WA':46,
    'WV':47,
    'WI':48,
    'WY':49,
  }.get(x)


def Update(Type,addr,host,user,passwd,db):
  """
  The Update function updates the Incubator Table in an incremental fashion 

  Paramater: 
    Type: (String) Lawmaker or Candidate
    addr: (String) File Address in Lawmaker Format(Ex. ALp0.json) or Candidate Format(Ex. ALCp0.json)
    host: (String) MySQL Server Location
    User: (String) MySQL username
    passwd: (String) MySQL password
    db: (String) MySQL Database Name 
  """

  row_updates=0
  Time = time.time()
  with open(addr) as f:
    data=json.load(f)
    for item in data['records']:
      conn = MySQLdb.connect(host,user,passwd,db)
      cursor = conn.cursor()
      if Type=='Candidate':
        Cand_Mname="n/a"
        #Create Variables for Candidate Specific Elements
        Name=item['Candidate']['Candidate'].split(',')
        if len(Name)>1:
          Cand_Frname=Name[1].strip().upper()
          Cand_Lname=Name[0].upper()
          Cand_Fname=(Name[0]+","+Name[1]).upper()
        else:
          Cand_Fname=Name[0]
          Cand_Frname=""
          Cand_Lname=""


        

        year=item['Election_Year']['Election_Year']
        State=item['Election_Jurisdiction']['Election_Jurisdiction']
        office=normalizeOffice(item['Office_Sought']['Office_Sought'])
        Election_Type=item['Election_Type']['Election_Type']
        Election_Status=item['Election_Status']['Election_Status']
        Incumbency_Status=item['Incumbency_Status']['Incumbency_Status']
        money=item['Total_$']['Total_$']
        party=item['General_Party']['General_Party']
       
        if "'" in Cand_Fname:
          Cand_Frname=Cand_Frname.replace("'"," ")
          Cand_Fname=Cand_Fname.replace("'"," ")
          Cand_Lname=Cand_Lname.replace("'"," ")
        if '"' in Cand_Fname or '"' in Cand_Lname:
          Cand_Frname=Cand_Frname.replace('"',' ')
          Cand_Fname=Cand_Fname.replace('"',' ')
          Cand_Lname=Cand_Lname.replace('"',' ')
        #If statements to remove apostrophers and quotation marks from Candidates names
        #This ensures that the SQL Statement will execute properly.

        SQL='SELECT COUNT(*) FROM ftm_inc WHERE First_Name="'+Cand_Frname+'"'+' AND Last_Name="'+Cand_Lname+'"'+" AND State='"+State+"'"+" AND Cycle="+str(year)+" AND Office='"+office+"'"+" AND General_Party='"+party+"'"+"AND Cycle_Type='"+Election_Type+"'"+" AND contribution="+str(money)+";"
        #SQL statement to check if the Lawmaker from the file is already in the Incubator Table 
      if Type=='Lawmaker':
        table='lm_dim_lawmaker'
        Time = time.time()
        Fname=item['Lawmaker']['Lawmaker']
        Lname=Fname.split(',')[0]
        Frname1=Fname.split(',')[1]
        Frname=Frname1.split()[0]
        ID= item['Lawmaker']['id']
        Election_Type='Standard'
        Election_Status=None
        Incumbency_Status=None
        office = item['General_Office']['General_Office']
        party=item['General_Party']['General_Party']
        #party2=item['Specific_Party']['Specific_Party']
        party2='NA'
        money = item['Total_$']['Total_$']
        money=int(round(float(money)))
        year = item['Election_Year']['Election_Year']
        State="AL"
        #State=item['Election_Jurisdiction']['Election_Jurisdiction']
        #Change to this once API Access is regained
        #add gro=s to request parameters
        Mname=''
        SQL="SELECT COUNT(*) FROM ftm_inc WHERE First_Name='"+Frname+"'"+" AND Last_Name='"+Lname+"'"+"AND State='"+State+"'"+" AND Office='"+office+"'"+" AND General_Party='"+party+"'"+" AND Cycle="+str(year)+" AND Cycle_Type='"+Election_Type+"'"+" AND contribution="+str(money)
        #SQL statement to check if the Candidate from the file is already in the Incubator Table 
      cursor.execute(SQL)
      results= cursor.fetchall()
      conn.commit()
      conn.close()
      #print Cand_Frname+" "+str(results[0][0])
      #results[0][0]=0
      #results=1
      if results[0][0]>0:
        print Cand_Fname+"checked"
      if results[0][0]==0:
        conn = MySQLdb.connect(host,user,passwd,db)
        cursor = conn.cursor()
        Incubator="""INSERT INTO ftm_inc(State,Cycle,Cycle_Type,Election_Status,Incumbency_Status,Full_Name,First_Name,Middle_Name,Last_Name,contribution,Office,General_Party,timestamp) VALUES('%s',%s,'%s','%s','%s','%s','%s','%s','%s',%s,'%s','%s','%s')"""
        #print Incubator%(State,year,Election_Type,Election_Status,Incumbency_Status,Cand_Fname,Cand_Frname,Cand_Mname,Cand_Lname,money,office,party,datetime.datetime.fromtimestamp(Time).strftime('%Y-%m-%d'))
        cursor.execute(Incubator%(State,year,Election_Type,Election_Status,Incumbency_Status,Cand_Fname,Cand_Frname,Cand_Mname,Cand_Lname,money,office,party,datetime.datetime.fromtimestamp(Time).strftime('%Y-%m-%d')))
        print Cand_Fname+"inserted"
        #If the Candidate or Lawmaker is not already in the Incubator Table they are then inserted
        #This can be done more efficiently with a unique index and/or an upsert SQL Function(Return to this if time allows)
        conn.commit()
        conn.close()
        row_updates+=1
      with open('log.txt','a') as f:
        f.write(str(row_updates)+ " Rows Updated on "+datetime.datetime.fromtimestamp(Time).strftime('%Y-%m-%d %H:%M:%S')+"\n")
        #Logs the number of rows updated
    print 'updated'


def database2(host,user,passwd,db,type1):
  '''
  The Second database function populates the fact and dimension tables from the incubator table 

  Parameters:
    Host: (String) URL of the mySql Server
    User:(String) Username for MySql Server
    passwd: (String) password for MySQL Server
    db: (String) MySQL Database Name
    type1: (String) Entitity type, Lawmaker or Candidate
  '''
  y=0
  for i in range (0,5):
    conn = MySQLdb.connect(host,user,passwd,db)
    cursor = conn.cursor()
    x = str(NumToDIM(i))
    if type1=='Lawmaker' and i==4:
      table='lm_dim_lawmaker'
      params=['Lawmaker_ID','full_name','first_name','middle_name','last_name']
      #parameters for Lawmaker SQL Statement
    elif type1=='Candidate' and i==4:
      table='cand_dim_candidate'
      params=['Candidate_ID','full_name','first_name','middle_name','last_name']
      #parameters for Candidate SQL Statements
    elif x=='cycle':
      table='ftm_dim_'+x
      params=['cycle_ID','cycle','cycle_type',]
      #parameters for Cycle SQL Statements 
    elif x=='geo':
      table='ftm_dim_'+x
      params=['Geo_ID','State','District']
      #parameters for Geography SQL Statements 
    elif x=='office':
      table = 'ftm_dim_'+x
      params=['Office_ID','Office','Office_Code']
      #parameters for Office SQL Statements 
    elif x =='party':
      table='ftm_dim_'+x
      params=['Party_ID','General_Party','Specific_Party']
      #parameters for Party SQL statements 
      
    join=params[1]
    pk=params[0]
    params3 =list()
    params2=''
    params4=''
    for i in range(1,len(params)):
      if(i==len(params)-1):
        params2+=params[i]
      else:
        params2+=params[i]+','
      #formats parameters with commas to ensure correct SQL
      params3.append("ftm_inc."+params[i])
      
    for x in range(0,len(params3)):
      if(x==len(params3)-1):
        params4+=params3[x]
      else:
        params4+=params3[x]+','
         #formats parameters with commas to ensure correct SQL
    if(type1=='Lawmaker'):
      #print params
      SQL="INSERT IGNORE INTO "+table+' ('+params2+')'+'\n\t SELECT DISTINCT '+params4+"\n\t\t FROM ftm_inc \n\t\t LEFT JOIN "+table+" ON ftm_inc."+join+'='+table+'.'+join+'\n\t\t WHERE ftm_inc.'+pk+' IS NULL AND Election_Status IS NULL'# AND ftm_inc.'+params[1]+" IS NOT NULL AND ftm_inc."+params[2]+" IS NOT NULL"
      #Inserts dimension data into dimensionn tables ensuring that there are no duplicates and that the dimension data is associated with lawmakers
      SQL2="UPDATE IGNORE ftm_inc JOIN "+table+' ON ftm_inc.'+join+'='+table+'.'+join+' SET ftm_inc'+'.'+pk+'='+table+'.'+pk+" WHERE Election_Status IS NULL"
      #Set's the dimension ID's on the incubator table to the auto-incremented ID's on the dimension table
      SQL3="UPDATE "+table+" SET timestamp=now()"
      #Sets the time in which the dimensions were added to the dimension table 
      SQL4="""INSERT IGNORE INTO lm_fact (contributions) \n\t SELECT DISTINCT contribution FROM ftm_inc WHERE Election_Status IS NULL"""
      #Inserts the contributions associated with each Lawmaker
      SQL5="UPDATE lm_fact JOIN ftm_inc ON lm_fact.contributions=ftm_inc.contribution SET lm_fact."+pk+"=ftm_inc."+pk
      #Sets the dimension ID's on the Lawmaker Fact Table to the auto-incremented ID's on the dimension table 
      SQL6="UPDATE lm_fact SET timestamp=now()"
      #Sets timestamp for when Lawmakers were added to the fact-table 
      SQL7="UPDATE IGNORE lm_fact JOIN ftm_dim_geo ON ftm_dim_geo.Geo_ID=lm_fact.Geo_ID SET lm_fact.State=ftm_dim_geo.State"
      #Set the State associated with the ID on the Geography Dimension table
      cursor.execute(SQL)
      cursor.execute(SQL2)
      cursor.execute(SQL3)
      cursor.execute(SQL4)
      cursor.execute(SQL5)
      cursor.execute(SQL6)
      cursor.execute(SQL7)
      conn.commit()
      conn.close()
     
    if(type1=='Candidate'):
      SQL="INSERT IGNORE INTO "+table+' ('+params2+')'+'\n\t SELECT DISTINCT '+params4+"\n\t\t FROM ftm_inc \n\t\t LEFT JOIN "+table+" ON ftm_inc."+join+'='+table+'.'+join+'\n\t\t WHERE ftm_inc.'+pk+' IS NULL AND Election_Status IS NOT NULL'# AND ftm_inc.'+params[1]+" IS NOT NULL AND ftm_inc."+params[2]+" IS NOT NULL"
      #Inserts dimension data into dimensionn tables ensuring that there are no duplicates and that the dimension data is associated with lawmakers
      #Inserts dimension data into dimensionn tables ensuring that there are no duplicates and that the dimension data is associated with candidates
      SQL2="UPDATE IGNORE ftm_inc JOIN "+table+' ON ftm_inc.'+join+'='+table+'.'+join+' SET ftm_inc'+'.'+pk+'='+table+'.'+pk+" WHERE Election_Status IS NOT NULL"
      #Set's the dimension ID's on the incubator table to the auto-incremented ID's on the dimension table
      SQL3="UPDATE "+table+" SET timestamp=now()"
      #Sets the time in which the dimensions were added to the dimension table 
      SQL4="INSERT IGNORE INTO cand_fact (contribution) \n\t SELECT DISTINCT contribution FROM ftm_inc WHERE Election_Status IS NOT NULL"
      #Inserts the contributions associated with each Candidate
      SQL5="UPDATE cand_fact JOIN ftm_inc ON cand_fact.contribution=ftm_inc.contribution SET cand_fact."+pk+"=ftm_inc."+pk+" WHERE cand_fact.Candidate_ID IS NULL"
      #Sets the dimension ID's on the Lawmaker Fact Table to the auto-incremented ID's on the dimension table 
      SQL6="UPDATE cand_fact SET timestamp=now()"
      #Sets timestamp for when Candidates were added to the fact-table 
      SQL7="UPDATE IGNORE cand_fact JOIN ftm_dim_geo ON cand_fact.Geo_ID=ftm_dim_geo.Geo_ID SET cand_fact.State=ftm_dim_geo.State WHERE cand_fact.state"
      #Set the State associated with the ID on the Geography Dimension table
      cursor.execute(SQL)
      print SQL
      cursor.execute(SQL2)
      print SQL2
      cursor.execute(SQL3)
      print SQL3
      cursor.execute(SQL4)
      print SQL4
      print SQL5
      cursor.execute(SQL5)
      
      cursor.execute(SQL6)
      print SQL6
      cursor.execute(SQL7)
      print SQL7
      
  
     
    
    
 

      
      conn.commit()
      conn.close()
      
def transform(addr,State):
  '''
  Function that extracts variables relevant to lawmakers from the JSON files
  and passes them onto the database function. Only run during the
  Historical load phase of the ETL.

  Parameters:
    addr: (String) Address of Lawmaker JSON file (Format ALp0.json)
    State: (String) Name of State (Ex. Alabama)
  '''
  with open(addr) as data_file:
    data=json.load(data_file)
    for item in data['records']:
      Time = time.time()
      Law_Fname=item['Lawmaker']['Lawmaker']
      print Law_Fname
      Law_Lname=Law_Fname.split(',')[0]
      Law_Frname1=Law_Fname.split(',')[1]
      Law_Frname=Law_Frname1.split()[0]
      ID= item['Lawmaker']['id']
      e_type='General'
      #Lawmakers have been elected so all of their Elections were General Elections
      office = item['General_Office']['General_Office']
      Office_Code(office)
      #Adds the relevant office code to the office dimension table 
      party1=item['General_Party']['General_Party']
      #party2=item['Specific_Party']['Specific_Party']
      #Once API_Access is gained Uncomment this 
      #Original API call before the cutoff didn't request specific parties
      party2='NA'
      money = item['Total_$']['Total_$']
      year = item['Election_Year']['Election_Year'] 
      #The Lawmaker API call does not include information about districts 
      database('Lawmaker',State,None,year,e_type,None,None,Law_Fname,Law_Frname,'n/a',Law_Lname,office,money,party1,party2,datetime.datetime.fromtimestamp(Time).strftime('%Y-%m-%d'))
      

def database(Entity,State,District,Cycle,Type,Election_Status,Incumbency_Status,Full_Name,First_Name,Middle_Name,Last_Name,Office,Total_Contribution,General_Party,Specific_Party,date):#Party_S,date):
  '''
  The database function populates the incubator table during the
  historical load portion of the ETL. It is only to be run once. 

  Parameters: 
    State: (String) Name of State in the United States (ex. Alabama)
    District: (int) Numerical value of US State Senate or House District (ex. 007)
    Cycle: (int) Numerical value of US Election Cycle (ex. 2010)
    Type: (String) Type of Election (Ex. Primary Election)
    Election_Status:(String) Status of the Election, wheather the candidate won or lsot
    Incumbency_Status: (String) The candidates position relative to the seat, wheather they are running for an open seat are an incumbent or challenger
    Full_Name: (String) Full Name of the Candidate or Lawmaker Format: LAST NAME, (MIDDLE NAME) FIRST NAME
    First_Name: (String) First Name of Candidate or Lawmaker Format: FIRST NAME
    Middle_Name: (String) Middle Name of Candidate or Lawmaker Format: MIDDLE NAME
    Last_Name: (String) Last Name of Candidate or Lawmakaer Format: LAST NAME
    Office: (String) The Office the Lawmaker has or the candidate is running for (Ex. Lieutenant Governor, State Senate)
    Total_Contribution: (int) The amount of money contributed to the candidate or lawmaker
    General_Party: (String) The General identifier for a political party (ex. Democrats, 3rd Party)
    Specific_Party: (String) The Specific identifier for a political party (ex. Democrats, Libertarians)
    date: (datetime) The timestamp for the date of insertion into the incubator table Format: Year-Month-Day

    '''
  
  #host='atlasproject-stage.cslkfacnmbbr.us-east-1.rds.amazonaws.com'
  host='localhost'
  #user='travis'
  #passwd='atlastravis'
  user='tahahn'
  passwd='travis2dkk'
  db='atlas_ftm'
  conn = MySQLdb.connect(host,user,passwd,db)
  cursor = conn.cursor()
  if Entity=='Candidate':
    conn = MySQLdb.connect(host,user,passwd,db)
    cursor = conn.cursor()
    if District is None:
      Incubator="""INSERT INTO ftm_inc(State,Cycle,Cycle_Type,Election_Status,Incumbency_Status,Full_Name,First_Name,Middle_Name,Last_Name,contribution,Office,General_Party,Specific_Party,timestamp) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      cursor.execute(Incubator,(State,Cycle,Type,Election_Status,Incumbency_Status,Full_Name,First_Name,Middle_Name,Last_Name,Total_Contribution,Office,General_Party,Specific_Party,date))
    else:
      Incubator="""INSERT INTO ftm_inc(State,Cycle,Cycle_Type,District,Election_Status,Incumbency_Status,Full_Name,First_Name,Middle_Name,Last_Name,contribution,Office,General_Party,Specific_Party,timestamp) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      cursor.execute(Incubator,(State,Cycle,Type,District,Election_Status,Incumbency_Status,Full_Name,First_Name,Middle_Name,Last_Name,Total_Contribution,Office,General_Party,Specific_Party,date))

    #inserts candidate with fields relevant to candidates into the incubator table 
    conn.commit()
    conn.close()
  if Entity=='Lawmaker':
    conn = MySQLdb.connect(host,user,passwd,db)
    cursor = conn.cursor()
    Incubator="""INSERT INTO ftm_inc(State,Cycle,Cycle_Type,Full_Name,First_Name,Middle_Name,Last_Name,contribution,Office,General_Party,Specific_Party,timestamp) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.execute(Incubator,(State,Cycle,Type,Full_Name,First_Name,Middle_Name,Last_Name,Total_Contribution,Office,General_Party,Specific_Party,date))
    #inserts lawmaker with fields relevant to lawmakers into the incubator table 
    conn.commit()
    conn.close()
      
  
def transform_Candidate(Addr):
  '''
  Function that extracts data relevant to Candidates from the JSON files
  and passes them onto the database function. Only run during the
  Historical load phase of the ETL.

  Parameters: 
    Adrr: (String) Address of Candidate JSON file Format: StateCp#.json (ex. ALCp0.json)
  '''
  print Addr
  with open(Addr) as f:
    data= json.load(f)
  Time = time.time()
  for item in data['records']:
    #Extracts data relevant to Candidates
    Name=item['Candidate']['Candidate'].split(',')
    if len(Name)>1:
      Cand_Frname=Name[1].strip().upper()
      Cand_Lname=Name[0].upper()
      Cand_Fname=(Name[0]+","+Name[1]).upper()
      print Cand_Fname
    else:
      Cand_Fname=Name[0]
      Cand_Frname=""
      Cand_Lname=""
      print Cand_Fname
      
    
    
    #Cand_Lname=Name.split(',')[0].upper()
    #Cand_Frname=Name.split(',')[1].strip().upper()
    #Cand_Mname=''
    #Cand_Fname= Name.upper()
    #print Name
    
    Specific_Party=item['Specific_Party']['Specific_Party']
    General_Party=item['General_Party']['General_Party']
    Office_Sought=item['Office_Sought']['Office_Sought']
    Year = item['Election_Year']['Election_Year']
    Type = item['Election_Type']['Election_Type']
    digit=re.search("\d", Office_Sought)#Finds location of first digit in string
    Office=normalizeOffice(Office_Sought)#Standardizes names 
    District = None
    if digit:
     District=Office_Sought[digit.start():]
     #In the API Candidate Offices are formated HOUSE 07 or SENATE 22
     #This sub-method finds the District number to insert into the database
    #print Office

    Office_Code(Office)
    #The lawmaker API formates Office as State House/Assembly and State Senate
    #This sub-method sets the Office of the Candidate to the Lawmaker Standard
    Office_Code(Office)
    #This sub-method ensures that the office code associated with the candidates office
    #is in the Office Dimension Table 
    State= item['Election_Jurisdiction']['id']
    Election_Type=item['Election_Type']['Election_Type']
    Election_Status = item['Election_Status']['Election_Status']
    Incumbency_Status=item['Incumbency_Status']['Incumbency_Status']
    contribution = item['Total_$']['Total_$']
    database('Candidate',State,District,Year,Election_Type,Election_Status,Incumbency_Status,Cand_Fname,Cand_Frname,'n/a',Cand_Lname,Office,contribution,General_Party,Specific_Party,datetime.datetime.fromtimestamp(Time).strftime('%Y-%m-%d'))
   

def  state_cycle(daily_api_calls,api_call_limit,start,startPage,pages,type2,update1,skip):
  '''
  The purpose of the method is to cycle through the states 
  keeping track of the last page utilized and ensuring the 
  daily API call limit is not reached

  Parameters:
    daily_api_calls: (int) the number of times the API has been called 
    api_call_limit: (int) the limit of the number of times the API can be called in a day
    start: (int) the number of the state to start on (ex. State #3=Arkansas)
    startPage: (int) the page number to start on (ex. page 0) 
    pages: (dict) dictionary containing the maximum amount of pages associated with each state (ex. Alabama:4)
    type2: (String) type of entity being inserted or updated Values: Lawmaker or Candidate
    update1: (boolean) variable that indicates if the program is historically loading data or incrementally updating

  '''
  Week=datetime.date.today().isocalendar()[1]
  #print daily_api_calls
  host='localhost'
  user='tahahn'
  passwd='travis2dkk'
  db='atlas_ftm'
  #Set this to 50 After Testing Finishes
  #print 'test'
  for y in range (start,50):
    print NumToState(y)
    print ""
    Time = time.time()
    conn = MySQLdb.connect(host,user,passwd,db)
    cursor = conn.cursor()
    SQL="SELECT Max_Page From ftm_update WHERE State='%s'"%NumToState(y)
    cursor.execute(SQL)
    conn.commit()
    conn.close()
    results= cursor.fetchall()
    daily_api_calls=int(daily_api_calls)
    for x in range(startPage,pages[NumToState(y)]+1):
      if daily_api_calls<=api_call_limit and x!=skip:
        print x
        print daily_api_calls
        conn = MySQLdb.connect(host,user,passwd,db)
        cursor = conn.cursor()
        SQL="INSERT INTO ftm_update(State,Update_Date,Page_Num) VALUES(%s,%s,%s) on duplicate key update State=%s,page_Num=%s"
        #SQL Statement that inserts State and Page # into the Update table if the State isn't in the table and
        #performs and update operation with the page number if it is
        SQL2="UPDATE ftm_update SET Max_Page=%s WHERE State='%s'"%(pages[NumToState(y)],NumToState(y))
        #This Sets the Max_page field in the Update table to the maxpage of the state
        #This statement should only be executed once when the State is initially reached
        SQL3="UPDATE ftm_update SET Status='Incomplete' WHERE State='%s'"%NumToState(y)
        #SQL Statement that sets the Update Status of a State to Incomplete
        #This is done only once when the first page of the State is reached
        #print SQL
        cursor.execute(SQL,(NumToState(y), datetime.datetime.fromtimestamp(Time).strftime('%Y-%m-%d %H:%M:%S'),x,NumToState(y),x))
        if(startPage==0):
          cursor.execute(SQL2)
          cursor.execute(SQL3)
        conn.commit()
        conn.close()
        Addr=NumToState(y)+'p'+str(x)+".json" #File address of data from Lawmaker API calls
        AddrC=NumToState(y)+'Cp'+str(x)+".json"#File address of data from Candidate API Calls
        #print Addr
        #print AddrC
        if(type2=='Lawmaker'and not update1):
          data_extract('Lawmaker',x,NumToState(y))#Extracts the data from the Lawmaker API
          transform(Addr,NumToState(y))#Inserts the data into the incubator database
          daily_api_calls+=1
          #print 'test'
        if(type2=='Candidate' and not update1):
          #data_extract('Candidate',x,NumToState(y))
          daily_api_calls+=1
          transform_Candidate(AddrC)
        if(type2=='Lawmaker' and update1):
          #print 'test'
          data_extract('Lawmaker',x,NumToState(y))#Extracts the data from the Lawmaker API
          Update('Lawmaker',Addr,host,user,passwd,db)
          daily_api_calls+=1
        if(update1 is True and type2=='Candidate'):
          #data_extract('Candidate',x,NumToState(y))
          Update('Candidate',AddrC,host,user,passwd,db) 
          daily_api_calls+=1
        
      with open ('date.txt') as f:
         date=f.readline()
      if daily_api_calls>api_call_limit and Week>int(date):
          with open('apicalls.txt','w') as f:
            f.write('0')
          daily_api_calls=0
          #If the API limit has been reached and the most recently accessed date is not the current date
          #The number of daily api calls is set to 0 both in the program and in the text_file 
      if daily_api_calls>api_call_limit and str(Today)==str(date):
        print "API LIMIT REACHED"
        exit()
        #If the number of api calls made in a day exceed the limit the program will shut-down


      #time.sleep(5)#For Testing Purposes Delete For Production
    conn = MySQLdb.connect(host,user,passwd,db)
    cursor = conn.cursor()
    cursor.execute("UPDATE ftm_update SET STATUS='Complete' WHERE STATE='%s'"%NumToState(y))
    #After a state has been iterated through when page_num=max_page the status is set to complete
    #This should occur after the loop above is iterated through so no if statement and further SQL should be necessary
    conn.commit()
    conn.close()
    #print daily_api_calls
    write_api_calls(daily_api_calls)
  database2(host,user,passwd,db,type2)
  return daily_api_calls
  #This method returns the number of API calls executed when the function was run as a way to pass the number of calls
  #to this function when it is run again to ensure that the daily limit is not exceeded
  #There is probobly a better way to implement this using global variables or recursion

def normalizeOffice(office):
  '''
  The purpose of this funciton is to standarize the office field
  in the incubator and office dimensional tables to the standard
  set in the Lawmaker API Calls 

  Parameters:
    office: (string) the name of the office sought by a candidate or inhabited by a lawmaker
  '''
  if 'US HOUSE' in office:
    return 'House of Representatives'
  elif 'HOUSE' in office and 'US' not in office:
    return "State House/Assembly"
  elif 'ASSEMBLY DISTRICT' in office:
    return 'State House/Assembly'
  elif 'EDUCATION' in office:
    return 'Board of Ed.'
  elif 'SUPREME' in office:
    return 'Supreme Court Seat'
  elif 'APPELLATE' in office:
    return "Apellate Court Seat"
  elif 'SENATE DISTRICT' in office:
    return 'State Senate'
  elif 'State Representative' in office:
    return "State House/Assembly"
  elif 'SENATE' in office and 'US' not in office:
    return "State Senate"
  elif 'GOVERNOR' in office:
    return 'Governor'
  elif 'LIEUTENANT GOVERNOR' in office:
    return 'Lt. Gov'
  elif 'HAWAIIAN AFFAIRS' in office:
    return 'Office of Hawaiian Affairs'
  elif 'PUBLIC REGULATION' in office:
    return 'Public Regulation Comissioner'
  elif "REGENTS" in office:
    return 'Board of Reagents Member'
  elif "SUPERINTENDENT OF PUBLIC" in office:
    return 'Superintendent of Public Instruction'
  elif "TRANSPORTATION COMMISSIONER" in office:
    return "Transportation Commissioner"
  elif "REGIONAL TRANSPORTATION" in office:
    return "Regional Transportation Commissioner"
  elif "SUPERIOR COURT" in office:
    return "Superior Court Seat"
  elif "PUBLIC SERVICE COMMISSIONER" in office:
    return 'Public Service Commissioner'
  else:
    return office.title()

def Office_Code(o):
  '''
  The purpose of this function is to populate the office_code field of the 
  incubator and office dimensional tables. 

  Note: Where applicable the office codes were taken from those that existed in the er_fact table

  Parameter:
    o: (string) the name of the office sought by a candidate or inhabited by a lawmaker

  '''
  Code=''
  if o=='State House/Assembly':
    Code='SLEG'
  if o=='State Senate':
    Code='SSN'
  if o=='Governor':
    Code='GOV'
  if o=='Lieutenant Governor':
    Code='LTGOV'
  if o=='Board of Ed.':
    Code='BOE'
  if o=='Supreme Court':
    Code='SSC'
  host='localhost'
  user='tahahn'
  passwd='travis2dkk'
  db='atlas_ftm'
  conn = MySQLdb.connect(host,user,passwd,db)
  cursor = conn.cursor()
  OFF="""UPDATE ftm_dim_office SET Office_Code='%s' WHERE Office='%s'"""
  OFF2="""UPDATE ftm_inc SET Office_Code='%s' WHERE Office='%s'"""
  #print OFF%(Code,o)
  cursor.execute(OFF%(Code,o))
  cursor.execute(OFF2%(Code,o))
  conn.commit()
  conn.close()


def get_maxPage(api_limit,maxpage,maxpageC,load_type):
  '''
  The purpose of the get_maxPage function is to 
  populate the two max_page dictionaries with data
  extracted from the Follow The Money API

  Parameters:
    api_limit: (int) the number of times the API can be called in a day
    maxpage: (dict) a dictionary that associates the maxPage Number with a State for Lawmakers
    maxpageC: (dict) a dictionary that associates the maxPage Number with a State for Candidates
    load_type: (int) either a 1 or a 0. 1 means that a historical load is occuring and a 0 means and update is occuring.
  '''
  if(load_type!=0):
    y=0
    #The range will be from 0 to 50 once testing begins
    #But with the limited data available only for Alabama the range is set from 0 to 1
    for x in range (0,50):
      print x
      if y<api_limit:
        data_extract('Lawmaker',0,NumToState(x))
        data_extract('Candidate',0,NumToState(x))
        #The first page of the lawmakers and candidates for each state is donwloaded 
        with open(NumToState(x)+'Cp0.json') as f2:
          data1= json.load(f2)
        max_cand=int(data1['metaInfo']['paging']['maxPage'])
        #The number of pages of candidates running in 2015-2016 and lawmakers elected in 2015-2016 is extracted
        #maxpage[NumToState(x)]=max_law
        maxpageC[NumToState(x)]=max_cand
        #The number of pages is then added to the dictionary of pages and states in the format state:maxPage (Ex. AL:4)
        y+=1
    pickle.dump(maxpageC, open( "maxpage.p", "wb" ) )
    #Writes the dictionary to a file to create a persistant version of the dictionary
  else:
    maxpageC=pickle.load(open("maxpage.p","rb"))
    #If the program is updating(ie. the Maxpage dictionary has already been set) then
    #the maximum number of pages for each state is loaded from the persistent file
    return maxpageC
   

def write_api_calls(api_call):
  '''
  Writes the number of API calls during the day to a text_file
  used to ensure that the ETL doesn't exceed the number of API calls allocated per day

  Parameters:
    api_call:(int) the current number of API calls performed
  '''
  with open('apicalls.txt','w') as f:
    f.write(str(api_call))

def write_day():
  '''
  Writes the current date to a text file, part of the process of ensuring that 
  the ETL doesn't exceed the number of API calls allocated per day


  Parameters:
    None

  '''
  today=datetime.date.today()
  Weeknum=today.isocalendar()[1]
  print Weeknum
  with open('date.txt','w') as f:
    f.write(str(Weeknum))


def getLastStatePage():
  '''
  Gets the last state and page updated and returns the numeric values
  for the next state and page to be updated
  '''
  conn = MySQLdb.connect(host,user,passwd,db)
  cursor = conn.cursor()
  SQL="SELECT State,Page_num FROM ftm_update WHERE Status='Incomplete' GROUP BY Update_Date"
  cursor.execute(SQL)
  results= cursor.fetchall()
  #Connects to the Update Table of the Database to see where the program left off
  #Returns the most recently incomplete page
  if len(results)==0:
    #If there are no incomplete states then another statement is run
    #to find the state and the page that should be updated next
    SQL="SELECT * FROM ftm_update GROUP BY update_date DESC;"
    cursor.execute(SQL)
    results=cursor.fetchall() 
    result2=list()
    if results[0][3]=='Complete' and results[0][0]=='WY':
      #If the top result is Wyoming and it is Complete 
      #Then a new array containing ('AL',0) will be returned as all states have been covered
      result2.append('AL')
      result2.append(0)
      conn = MySQLdb.connect(host,user,passwd,db)
      cursor = conn.cursor()
      SQL3="UPDATE ftm_update SET Status='Incomplete'"
      SQL4="UPDATE ftm_update SET Page_num=0"
      cursor.execute(SQL3)
      cursor.execute(SQL4)
      conn.commit()
      conn.close()
      #As this part of the function is resetting the update start point
      #it also resets the update table setting all states to incomplete
      results= cursor.fetchall()
      print result2
      return result2
      #If all states through wyominy are complete then the state
      #to be updated is set to Alabama and the page to 0
    else:
      result2.append(NumToState(statetonum(results[0][0])+1))
      result2.append(0)
      print result2
      return result2
      #If all the states through wyoming are not complete then
      #the numerical value of the next state and page are returned
  return results[0]

def getLastStatePageUser(State):
  '''
  Gets the last page updated for the state that has been defined by the user
  '''
  conn = MySQLdb.connect(host,user,passwd,db)
  cursor = conn.cursor()
  SQL="SELECT Page_num FROM ftm_update WHERE State='"+State+"'"
  cursor.execute(SQL)
  results= cursor.fetchall()
  return results[0][0]
  #Connects to the Update Table of the Database to see where the program left off
  #Returns the most recently incomplete page
 
def maxpageUpdate(maxpage_Cand,Page,State):
  '''
  The purpose of this function is to check to see if the number of pages
  for a certain state has changed. 

  Parameters:
    maxpage_cand: (dict) maxpage_Cand is the dictionary that contains the number of maxpages
    it can be read from the maxpage.p pickle file after getmaxpage has been run. 
    Page:(int) the page number that is currently being accessed as part of the update. (Ex. page 2)
    State: (String) the string value of a state (ex. 'AL')
  '''

  data_extract('Candidate',Page,State)#Downloads the pertinent JSON File assocaited with the State and Page
  addr=State+"Cp"+str(Page)+".json"#Address for the Json file that contains the maxPage Info
  with open(addr) as f:
    data1= json.load(f)
  if maxpage_Cand[State] != data1['metaInfo']['paging']['maxPage']:
    #If the maxpage extracted from the JSON file is not the same as the
    #maxpage in the dictionary then the persistent dictioanry will be updated. 
    temporary = dict()
    temporary[State]=Page
    Skip=temporary[State]
    #Skip is the value that will be returned. It is the page that has already
    #been downloaded as part of the checking process and will be skipped in the 
    #update process as to save an API call. 
    End=data1['metaInfo']['paging']['maxPage']+1#End is the numerical value of the new maxpage and will be added to the persitent dictionary
    maxpage_Cand[State]=End
    pickle.dump(maxpage_Cand, open( "maxpage.p", "wb" ) )
    return Skip
  else:
    return None  


if __name__ == "__main__":
  host='localhost'
  user='tahahn'
  passwd='travis2dkk'
  db='atlas_ftm'
  maxpage=dict()#Dictionary of MaxPages and States for Lawmakers
  maxpage_Cand=dict()#Dictionary of MaxPages and States for Candidates
  #get_maxPage(200,maxpage,maxpage_Cand)
  
  conn = MySQLdb.connect(host,user,passwd,db)
  cursor = conn.cursor()
  SQL="SELECT COUNT(*) FROM ftm_inc"
  cursor.execute(SQL)
  results= cursor.fetchall()
  daily_api_calls=0#Number of API Calls made in a day
  api_call_limit=999#Maximum number of API calls allowed in a day
  write_day()
  if results[0][0]==0:
    maxpage_Cand=get_maxPage(api_call_limit,maxpage,maxpage_Cand,0)
    state_cycle(daily_api_calls,api_call_limit,0,0,maxpage_Cand,'Candidate',False,900)
    #If the incubator table is empty the historical load process is initiated
  if results[0][0]>0:
    maxpage_Cand=get_maxPage(api_call_limit,maxpage,maxpage_Cand,0)
    #maxpage_Cand is a persistent dictionary that contains the number of pages that each state has
    #It is created and accessed using the get_maxPage function
    with open('apicalls.txt') as f:
      api_calls=f.readline()
    #The total number of API Calls made in a week are contained in the apicalls.txt file 
    StartInfo= getLastStatePage()
    #getLastStatePage() returns a list that contains the State and the page number to be accessed next
    State=StartInfo[0]
    Page= StartInfo[1]
   
    Entity=raw_input("Lawmaker or Candidate?")
    if(Entity=='Candidate'):
      State_input = raw_input("Do you want to Start the Updates with a particular state?") 
      if State_input =='Yes':
        State_Start=raw_input("Which State?(Please Enter State Abbrv. ex.Texas->TX)")
        Page2=getLastStatePageUser(State_Start)
        Skip=maxpageUpdate(maxpage_Cand,Page,State)
        #maxpageUpdate checks to see if the number of pages for the state being accessed has changed
        #If it has then one version of the state_cycle method is called if it hasn't another is called
        if Skip is not None:
          state_cycle(api_calls,api_call_limit,statetonum(State_Start),Page2,maxpage_Cand,'Candidate',True,Skip)
          #Skip is the page that will be skipped in the maxpage update. For more info about this look at the Readme
        else:
          state_cycle(api_calls,api_call_limit,statetonum(State_Start),Page2,maxpage_Cand,'Candidate',True,900)
          #Skip is set to 900 if no page is to be skipped as no State will likely have page 900.None or 0 could
          #also be plausible placeholders. 
      else:
        Skip=maxpageUpdate(maxpage_Cand,Page,State)
        print str(Skip)+"TEST"
        if Skip is not None:
          state_cycle(api_calls,api_call_limit,statetonum(State),0,maxpage_Cand,'Candidate',True,Skip)
        else:
         state_cycle(api_calls,api_call_limit,statetonum(State),Page,maxpage_Cand,'Candidate',True,900)


          #If the incubator table is not empty and the user inidcates that they want to update Candidates the Candidate fact and dimension tables
          #and the dimensions associated with those lawmakers are updated incrementall


     
       
       
      