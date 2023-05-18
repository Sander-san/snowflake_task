MASTER_TABLE = '''
CREATE TABLE MASTER_TABLE (
  id VARCHAR(1024),
  IOS_App_Id INTEGER,
  Title VARCHAR(512),
  Developer_Name VARCHAR(512),
  Developer_IOS_Id FLOAT,
  IOS_Store_Url VARCHAR(512),
  Seller_Official_Website VARCHAR(512),
  Age_Rating VARCHAR(32),
  Total_Average_Rating FLOAT,
  Total_Number_of_Ratings FLOAT,
  Average_Rating_For_Version FLOAT,
  Number_of_Ratings_For_Version INTEGER,
  Original_Release_Date DATE,
  Current_Version_Release_Date DATE,
  Price_USD FLOAT,
  Primary_Genre VARCHAR(64),
  All_Genres VARCHAR(256),
  Languages VARCHAR(256),
  Description TEXT
);
'''

RAW_TABLE = '''
create table RAW_TABLE as select * from MASTER_TABLE;
'''

RAW_STREAM = '''
create stream RAW_STREAM on table RAW_TABLE;
'''

STAGE_TABLE = '''
create table STAGE_TABLE as select * from MASTER_TABLE;
'''

STAGE_STREAM = '''
create stream STAGE_STREAM on table STAGE_TABLE;
'''

INSERT_FROM_RAW = '''
INSERT INTO STAGE_TABLE 
SELECT id, IOS_APP_ID, Title, Developer_Name, Developer_IOS_Id, IOS_Store_Url, Seller_Official_Website, 
Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, 
Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, 
Price_USD, Primary_Genre, All_Genres, Languages, Description
FROM RAW_STREAM 
WHERE METADATA$ACTION = 'INSERT';
'''

INSERT_FROM_STAGE = '''
INSERT INTO MASTER_TABLE 
SELECT id, IOS_APP_ID, Title, Developer_Name, Developer_IOS_Id, IOS_Store_Url, Seller_Official_Website, 
Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, 
Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, 
Price_USD, Primary_Genre, All_Genres, Languages, Description
FROM STAGE_STREAM 
WHERE METADATA$ACTION = 'INSERT';
'''
