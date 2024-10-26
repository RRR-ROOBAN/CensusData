
import pandas as pd
import credentials
import streamlit as st
import mysql.connector
import pymongo
import sqlalchemy
from sqlalchemy import create_engine




file_path="C:/Users/RoobanRaj/Downloads/census_2011.xlsx"

df=pd.read_excel(file_path)

df.rename(columns={
    'State name': 'State/UT',
    'District name': 'District',
    'Male_Literate': 'Literate_Male',
    'Female_Literate': 'Literate_Female',
    'Rural_Households': 'Households_Rural',    #Column Name change
    'Urban_Households': 'Households_Urban',
    'Age_Group_0_29': 'Young_and_Adult',
    'Age_Group_30_49': 'Middle_Aged',
    'Age_Group_50': 'Senior_Citizen',
    'Age not stated': 'Age_Not_Stated'
}, inplace=True)


def format_text(word):
    return ' '.join([i.capitalize() if i.lower() != 'and' else 'and' for i in word.split()])

df['State/UT'] = df['State/UT'].apply(format_text)  #Apply Capital Format

with open('C:/Users/RoobanRaj/Downloads/Telangana.txt', 'r') as file:
    content = file.read()  # Reads the entire file
    districts_from_file = [i.strip() for i in content.replace("\n", ",").split(",")]

#State Name Change
df.loc[df["District"].isin(districts_from_file), "State/UT"] = "Telangana"
df.loc[df["District"].isin(["Leh and Kargil"]), "State/UT"] = "Ladakh"



# 1. Male + Female = Population
df.loc[(df['Population'].isna()) & (df['Male'].isna() | df['Female'].isna()), 'Population'] = df['Workers'] + df['Non_Workers']
df.loc[df['Male'].isna(), 'Male'] = df['Population'] - df['Female']
df.loc[df['Female'].isna(), 'Female'] = df['Population'] - df['Male']
df.loc[df['Population'].isna(), 'Population'] = df['Male'] + df['Female']

# Update Population if Male or Female is missing
#df.loc[(df['Population'].isna()) & (df['Male'].isna() | df['Female'].isna()), 'Population'] = df['Workers'] + df['Non_Workers']
#df.loc[df['Male'].isna(), 'Male'] = df['Population'] - df['Female']

# 4. Literate_Male + Literate_Female = Literate
df.loc[df['Literate_Male'].isna(), 'Literate_Male'] = df['Literate'] - df['Literate_Female']
df.loc[df['Literate_Female'].isna(), 'Literate_Female'] = df['Literate'] - df['Literate_Male']
df.loc[df['Literate'].isna(), 'Literate'] = df['Literate_Male'] + df['Literate_Female']

# 7. Male_SC + Female_SC = SC
df.loc[df['Male_SC'].isna(), 'Male_SC'] = df['SC'] - df['Female_SC']
df.loc[df['Female_SC'].isna(), 'Female_SC'] = df['SC'] - df['Male_SC']
df.loc[df['SC'].isna(), 'SC'] = df['Male_SC'] + df['Female_SC']

# 10. Workers - Marginal_Workers = Main_Workers
df.loc[df['Main_Workers'].isna(), 'Main_Workers'] = df['Workers'] - df['Marginal_Workers']
df.loc[df['Marginal_Workers'].isna(), 'Marginal_Workers'] = df['Workers'] - df['Main_Workers']

# 12. Population - Workers = Non_Workers
df.loc[df['Non_Workers'].isna(), 'Non_Workers'] = df['Population'] - df['Workers']

# 13. Workers - Male_Workers = Female_Workers
df.loc[df['Female_Workers'].isna(), 'Female_Workers'] = df['Workers'] - df['Male_Workers']
df.loc[df['Male_Workers'].isna(), 'Male_Workers'] = df['Workers'] - df['Female_Workers']

# 15. Male_Workers + Female_Workers = Workers
df.loc[df['Workers'].isna(), 'Workers'] = df['Male_Workers'] + df['Female_Workers']

# 16. (Agricultural_Workers + Household_Workers + Other_Workers) - Workers = Cultivator_Workers
df.loc[df['Cultivator_Workers'].isna(), 'Cultivator_Workers'] = (df['Agricultural_Workers'] + df['Household_Workers'] + df['Other_Workers']) - df['Workers']

# 17. (Cultivator_Workers + Household_Workers + Other_Workers) - Workers = Agricultural_Workers
df.loc[df['Agricultural_Workers'].isna(), 'Agricultural_Workers'] = (df['Cultivator_Workers'] + df['Household_Workers'] + df['Other_Workers']) - df['Workers']

# 18. (Agricultural_Workers + Cultivator_Workers + Other_Workers) - Workers = Household_Workers
df.loc[df['Household_Workers'].isna(), 'Household_Workers'] = (df['Agricultural_Workers'] + df['Cultivator_Workers'] + df['Other_Workers']) - df['Workers']

# 19. (Agricultural_Workers + Household_Workers + Cultivator_Workers) - Workers = Other_Workers
df.loc[df['Other_Workers'].isna(), 'Other_Workers'] = (df['Agricultural_Workers'] + df['Household_Workers'] + df['Cultivator_Workers']) - df['Workers']

# 20 to 27. Religious Data (Hindus, Muslims, Christians, etc.)
df.loc[df['Hindus'].isna(), 'Hindus'] = df['Population'] - (df['Muslims'] + df['Christians'] + df['Sikhs'] + df['Buddhists'] + df['Jains'] + df['Others_Religions'] + df['Religion_Not_Stated'])
df.loc[df['Muslims'].isna(), 'Muslims'] = df['Population'] - (df['Hindus'] + df['Christians'] + df['Sikhs'] + df['Buddhists'] + df['Jains'] + df['Others_Religions'] + df['Religion_Not_Stated'])
df.loc[df['Christians'].isna(), 'Christians'] = df['Population'] - (df['Hindus'] + df['Muslims'] + df['Sikhs'] + df['Buddhists'] + df['Jains'] + df['Others_Religions'] + df['Religion_Not_Stated'])
df.loc[df['Sikhs'].isna(), 'Sikhs'] = df['Population'] - (df['Hindus'] + df['Muslims'] + df['Christians'] + df['Buddhists'] + df['Jains'] + df['Others_Religions'] + df['Religion_Not_Stated'])
df.loc[df['Buddhists'].isna(), 'Buddhists'] = df['Population'] - (df['Hindus'] + df['Muslims'] + df['Christians'] + df['Sikhs'] + df['Jains'] + df['Others_Religions'] + df['Religion_Not_Stated'])
df.loc[df['Jains'].isna(), 'Jains'] = df['Population'] - (df['Hindus'] + df['Muslims'] + df['Christians'] + df['Sikhs'] + df['Buddhists'] + df['Others_Religions'] + df['Religion_Not_Stated'])
df.loc[df['Others_Religions'].isna(), 'Others_Religions'] = df['Population'] - (df['Hindus'] + df['Muslims'] + df['Christians'] + df['Sikhs'] + df['Buddhists'] + df['Jains'] + df['Religion_Not_Stated'])
df.loc[df['Religion_Not_Stated'].isna(), 'Religion_Not_Stated'] = df['Population'] - (df['Hindus'] + df['Muslims'] + df['Christians'] + df['Sikhs'] + df['Buddhists'] + df['Jains'] + df['Others_Religions'])


# 28. Households_Rural + Households_Urban = Households
df.loc[df['Households'].isna(), 'Households'] = df['Households_Rural'] + df['Households_Urban']

# 29. Households - Households_Rural = Households_Urban
df.loc[df['Households_Urban'].isna(), 'Households_Urban'] = df['Households'] - df['Households_Rural']

# 30. Households - Households_Urban = Households_Rural
df.loc[df['Households_Rural'].isna(), 'Households_Rural'] = df['Households'] - df['Households_Urban']



# Age Group Formula (Updated Column Names)
df.loc[(df['Age_Not_Stated'].isna()) & (df['Young_and_Adult'].isna() | df['Middle_Aged'].isna() | df['Senior_Citizen'].isna()), 'Age_Not_Stated'] = df['Population'] - (df['Young_and_Adult'] + df['Middle_Aged'] + df['Senior_Citizen'])

df.loc[df['Young_and_Adult'].isna(), 'Young_and_Adult'] = df['Population'] - (df['Middle_Aged'] + df['Senior_Citizen'] + df['Age_Not_Stated'])

df.loc[df['Middle_Aged'].isna(), 'Middle_Aged'] = df['Population'] - (df['Young_and_Adult'] + df['Senior_Citizen'] + df['Age_Not_Stated'])

df.loc[df['Senior_Citizen'].isna(), 'Senior_Citizen'] = df['Population'] - (df['Young_and_Adult'] + df['Middle_Aged'] + df['Age_Not_Stated'])






# Education Formula
df.loc[(df['Total_Education'].isna()) & (df['Literate_Education'].isna() | df['Illiterate_Education'].isna()), 'Total_Education'] = df['Literate_Education'] + df['Illiterate_Education']
df.loc[df['Literate_Education'].isna(), 'Literate_Education'] = df['Total_Education'] - df['Illiterate_Education']
df.loc[df['Illiterate_Education'].isna(), 'Illiterate_Education'] = df['Total_Education'] - df['Literate_Education']

df.loc[df['Literate_Education'].isna(), 'Literate_Education'] = df['Below_Primary_Education'] + df['Primary_Education'] + df['Middle_Education'] + df['Secondary_Education'] + df['Higher_Education'] + df['Graduate_Education'] + df['Other_Education']

df.loc[df['Below_Primary_Education'].isna(), 'Below_Primary_Education'] = df['Literate_Education'] - (df['Primary_Education'] + df['Middle_Education'] + df['Secondary_Education'] + df['Higher_Education'] + df['Graduate_Education'] + df['Other_Education'])

df.loc[df['Primary_Education'].isna(), 'Primary_Education'] = df['Literate_Education'] - (df['Below_Primary_Education'] + df['Middle_Education'] + df['Secondary_Education'] + df['Higher_Education'] + df['Graduate_Education'] + df['Other_Education'])

df.loc[df['Middle_Education'].isna(), 'Middle_Education'] = df['Literate_Education'] - (df['Below_Primary_Education'] + df['Primary_Education'] + df['Secondary_Education'] + df['Higher_Education'] + df['Graduate_Education'] + df['Other_Education'])

df.loc[df['Secondary_Education'].isna(), 'Secondary_Education'] = df['Literate_Education'] - (df['Below_Primary_Education'] + df['Primary_Education'] + df['Middle_Education'] + df['Higher_Education'] + df['Graduate_Education'] + df['Other_Education'])

df.loc[df['Higher_Education'].isna(), 'Higher_Education'] = df['Literate_Education'] - (df['Below_Primary_Education'] + df['Primary_Education'] + df['Middle_Education'] + df['Secondary_Education'] + df['Graduate_Education'] + df['Other_Education'])

df.loc[df['Graduate_Education'].isna(), 'Graduate_Education'] = df['Literate_Education'] - (df['Below_Primary_Education'] + df['Primary_Education'] + df['Middle_Education'] + df['Secondary_Education'] + df['Higher_Education'] + df['Other_Education'])

df.loc[df['Other_Education'].isna(), 'Other_Education'] = df['Literate_Education'] - (df['Below_Primary_Education'] + df['Primary_Education'] + df['Middle_Education'] + df['Secondary_Education'] + df['Higher_Education'] + df['Graduate_Education'])

df.isna().sum().to_frame().T


df = df.fillna(0) #replace null values as 0

#df.isna().sum().to_frame().T

column_names = df.columns.tolist()


"""**MongoDb**"""
MongoDb_Crendiential=credentials.Mongo_DB

Mongo_Username=MongoDb_Crendiential['Username']
Mongo_Password=MongoDb_Crendiential['Password']
Mongo_NewDB=MongoDb_Crendiential['MongoDB']
Mongo_Collection=MongoDb_Crendiential['Collection']


Client=pymongo.MongoClient(f"mongodb+srv://{Mongo_Username}:{Mongo_Password}@cluster0.6saiv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0") #server

newdb=Client[Mongo_NewDB]  #Database

Collection=newdb[Mongo_Collection]

data_to_upload=df.to_dict(orient="records") #converting dataframe to dictionary

Collection.insert_many(data_to_upload) #upload covert data to mongoDB database

DBlist=[]
for i in Collection.find({},{"_id":0}):  #list of string
  DBlist.append(i)

df1=pd.DataFrame(DBlist) #convert list str to data frame

password = credentials.MYSQL_CREDENTIALS

mydb =mysql.connector.connect(**password)

MySql_User=password['user']
MySql_Password=password['password']
MySql_DataBase=password['database']
MySql_Host=password['host']
MySql_Port=password['port']
  






engine=sqlalchemy.create_engine(f"mysql+mysqlconnector://{MySql_User}:{MySql_Password}@{MySql_Host}:{MySql_Port}/{MySql_DataBase}")

def clean_column_name(name):
    # Replace spaces and periods with underscores
    name = name.replace(" ", "_").replace(".", "_")
    # Shorten the name to 64 characters if necessary
    if len(name) > 64:
        name = name[:64]
    return name

df1.columns = [clean_column_name(col) for col in df1.columns]

df1.to_sql("Census_Table",con=engine,if_exists="replace")



"""**SQL**"""

cursor = mydb.cursor()

# Establish a MySQL connection
def get_db_connection():
    return mysql.connector.connect(**password)
    

# Fetch data from the database using SQL query
def fetch_data(query):
    connection = get_db_connection()
    df3 = pd.read_sql(query, con=engine)  #Data Base to sql
    connection.close()
    return df3

# Display the result in console
def display_query_results(query, title):
    st.write(f"### {title}")
    result = fetch_data(query)
    st.dataframe(result)  # Display as a DataFrame for better formatting
    
# Streamlit app UI
st.title("Census Data Insights")


#1: Total Population of Each District
query_1 = """
    SELECT District, SUM(Population) as total_population
    FROM Census_Table
    GROUP BY district;
"""
display_query_results(query_1, "Total Population of Each District")

#2: Literate Males and Females in Each District
query_2 = """
    SELECT district, SUM(Literate_Male) AS total_literate_males, SUM(Literate_Female) AS total_literate_females
    FROM census_Table
    GROUP BY district;

"""
display_query_results(query_2, "Literate Males and Females in Each District")

#Percentage of Workers in Each District
query_3="""
  SELECT district,
       SUM(Male_Workers) + SUM(Female_Workers) AS total_workers,
       SUM(Population) AS total_population,
       (SUM(Male_Workers) + SUM(Female_Workers)) / SUM(Population) * 100 AS worker_percentage
FROM Census_Table
GROUP BY district;
"""
display_query_results(query_3,"Percentage of Workers in Each District")

#Households with access to LPG or PNG as Cooking fuel

query_4="""
SELECT district, SUM(LPG_or_PNG_Households) AS households_with_lpg_png
FROM Census_Table
GROUP BY district;

"""
display_query_results(query_4,"Households with access to LPG or PNG as Cooking fuel")

#Religious composition (Hindus, Muslims, Christians, etc.) of each district:

query_5="""
SELECT district,
       SUM(Hindus) AS Hindus_population,
       SUM(Muslims) AS Muslims_population,
       SUM(Christians) AS Christians_population,
       SUM(Sikhs) AS Sikhs_population,
       SUM(Buddhists) AS Buddhists_population,
       SUM(Jains) AS Jains_population
FROM census_Table
GROUP BY district;

"""
display_query_results(query_5,"Religious composition of each district")

#House Holds With Internet
query_6="""
SELECT district, SUM(Households_With_Internet) AS households_with_internet
FROM census_Table
GROUP BY district;

"""
display_query_results(query_6,"Households With Internet")

# Educational attainment distribution
query_7 = """
SELECT District,
       SUM(Below_Primary_Education) AS Below_Primary_Education,
       SUM(Primary_Education) AS Primary_Education,
       SUM(Middle_Education) AS Middle_Education,
       SUM(Secondary_Education) AS Secondary_Education,
       SUM(Higher_Education) AS Higher_Education,
       SUM(Graduate_Education) AS Graduate_Education
FROM Census_Table
GROUP BY District;
"""
display_query_results(query_7, "Educational attainment distribution")

# Households with access to various modes of transportation
query_8 = """
SELECT District,
       SUM(Households_with_Bicycle) AS Households_with_Bicycle,
       SUM(Households_with_Car_Jeep_Van) AS Households_with_Car_Jeep_Van,

       SUM(Households_with_Scooter_Motorcycle_Moped) AS Scooter_Motorcycle_Moped
FROM Census_Table
GROUP BY District;
"""
display_query_results(query_8, "Households with access to various modes of transportation")

# Condition of occupied census houses
query_9 = """
SELECT District,
       SUM(Condition_of_occupied_census_houses_Dilapidated_Households) AS Dilapidated_Households,
       SUM(Households_with_separate_kitchen_Cooking_inside_house) AS Households_with_Separate_Kitchen,
       SUM(Having_bathing_facility_Total_Households) AS Households_with_Bathing_Facility,
       SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Households_with_Latrine_Facility,
       SUM(Type_of_bathing_facility_Enclosure_without_roof_Households) AS Households_with_Enclosure_Without_Roof
FROM Census_Table
GROUP BY District;
"""
display_query_results(query_9, "Condition of occupied census houses")

# Household size distribution
query_10 = """
SELECT District,
       SUM(Household_size_1_person_Households) AS Household_size_1_person_Households,
       SUM(Household_size_2_persons_Households) AS Household_size_2_persons_Households,
       SUM(Household_size_3_to_5_persons_Households) AS Household_size_3_5_persons_Households,
       SUM(Household_size_6_8_persons_Households) AS Household_size_6_8_persons_Households,
       SUM(Household_size_9_persons_and_above_Households) AS Household_size_9_persons_Households
FROM Census_Table
GROUP BY District;
"""
display_query_results(query_10, "Household size distribution")

# Total households in each state
query_11 = """
SELECT `State/UT`, SUM(Households) AS Total_Households
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_11, "Total households in each state")

# Households with latrine facility within the premises in each state
query_12 = """
SELECT `State/UT`, SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Households_with_Latrine_Facility
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_12, "Households with latrine facility within the premises in each state")

# Average household size in each state
query_13 = """
SELECT `State/UT`, SUM(Households) / SUM(Population) AS Average_Household_Size
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_13, "Average household size in each state")

# Owned vs rented households in each state
query_14 = """
SELECT `State/UT`,
       SUM(Ownership_Owned_Households) AS Owned_Households,
       SUM(Ownership_Rented_Households) AS Rented_Households
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_14, "Owned vs rented households in each state")

# Distribution of different types of latrine facilities in each state
query_15 = """
SELECT `State/UT`,
       SUM(Type_of_latrine_facility_Pit_latrine_Households) AS Pit_Latrine_Households,
       SUM(Type_of_latrine_facility_Other_latrine_Households) AS Other_Latrine_Households,
       SUM(Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Hou) AS Night_Soil_Disposed_Households,
       SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_o) AS Flush_Latrine_Households
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_15, "Distribution of different types of latrine facilities in each state")

# Households with access to drinking water sources near the premises in each state
query_16 = """
SELECT `State/UT`,
       SUM(Location_of_drinking_water_source_Near_the_premises_Households) AS Drinking_water_near_premises,
       SUM(Location_of_drinking_water_source_Within_the_premises_Households) AS Drinking_water_within_premises
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_16, "Households with access to drinking water sources near the premises in each state")

# Average household income distribution in each state based on power parity
query_17 = """
SELECT `State/UT`,
       AVG(Power_Parity_Rs_45000_90000) AS Avg_Power_Parity_45000_90000,
       AVG(Power_Parity_Rs_90000_150000) AS Avg_Power_Parity_90000_150000,
       AVG(Power_Parity_Rs_45000_150000) AS Avg_Power_Parity_45000_150000,
       AVG(Power_Parity_Rs_150000_240000) AS Avg_Power_Parity_150000_240000,
       AVG(Power_Parity_Rs_240000_330000) AS Avg_Power_Parity_240000_330000,
       AVG(Power_Parity_Rs_150000_330000) AS Avg_Power_Parity_150000_330000
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_17, "Average household income distribution in each state based on power parity")

# Percentage of married couples with different household sizes in each state
query_18 = """
SELECT `State/UT`,
       (SUM(Household_size_1_person_Households) / SUM(Households)) * 100 AS Percentage_1_person_Households,
       (SUM(Household_size_2_persons_Households) / SUM(Households)) * 100 AS Percentage_2_persons_Households,
       (SUM(Household_size_3_to_5_persons_Households) / SUM(Households)) * 100 AS Percentage_3_5_persons_Households, # Enclose the column name in backticks
       (SUM(Household_size_6_8_persons_Households) / SUM(Households)) * 100 AS Percentage_6_8_persons_Households,
       (SUM(Household_size_9_persons_and_above_Households) / SUM(Households)) * 100 AS Percentage_9_persons_Households
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_18, "Percentage of married couples with different household sizes in each state")

# Households with Power Parity less than Rs. 45000 in each state
query_19 = """
SELECT `State/UT`, SUM(Power_Parity_Less_than_Rs_45000) AS Households_Below_Rs_45000
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_19, "Households with Power Parity less than Rs. 45000 in each state")

# Overall literacy rate in each state
query_20 = """
SELECT `State/UT`,
       (SUM(Literate) / SUM(Population)) * 100 AS Literacy_Rate
FROM Census_Table
GROUP BY `State/UT`;
"""
display_query_results(query_20, "Overall literacy rate in each state")

