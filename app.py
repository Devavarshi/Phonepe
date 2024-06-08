import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd
import os
import json
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlite3

engine = create_engine('mysql+pymysql://root:@localhost:3306/phonepe')

#SQL Queries and creating Tables
import mysql.connector
mydb = mysql.connector.connect(
 host="localhost",
 user="root",
 password="",
 database="phonepe",
 )
mycursor = mydb.cursor(buffered=True)


# Set page config
st.set_page_config(page_title="PhonePe Pulse", page_icon=":bar_chart:", layout="wide")

#Aggreated Transaction
path="C:/Users/varshinikarthik/Desktop/Varshini/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
aggreg_trans=os.listdir(path)

clm={'State':[], 'Year':[],'Quater':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in aggreg_trans:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transaction_type'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm)

Agg_Trans['State']=Agg_Trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
Agg_Trans['State']=Agg_Trans['State'].str.replace("-"," ")
Agg_Trans['State']=Agg_Trans['State'].str.title()
Agg_Trans['State']=Agg_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
Agg_Trans['Year'] = Agg_Trans['Year'].astype(int)

#Aggregated User
path1="C:/Users/varshinikarthik/Desktop/Varshini/Phonepe/pulse/data/aggregated/user/country/india/state/"
aggreg_user=os.listdir(path)

clm1={'State':[], 'Year':[],'Quater':[], 'brand':[], 'Transaction_count':[],'percentage':[]}

for i in aggreg_user:
    p_i=path1+i+"/"
    Agg_yr=os.listdir(p_i)
    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            E=json.load(Data)
            try:
                for z in E['data']['usersByDevice']:
                    Brand=z['brand']
                    Count=z['count']
                    percentage=z['percentage']
                    clm1['brand'].append(Brand)
                    clm1['Transaction_count'].append(Count)
                    clm1['percentage'].append(percentage)
                    clm1['State'].append(i)
                    clm1['Year'].append(j)
                    clm1['Quater'].append(int(k.strip('.json')))
            except:
                pass
Agg_User=pd.DataFrame(clm1)

#MAP Transaction
Agg_User['State']=Agg_User['State'].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
Agg_User['State']=Agg_User['State'].str.replace("-"," ")
Agg_User['State']=Agg_User['State'].str.title()
Agg_User['State']=Agg_User['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

path2="C:/Users/varshinikarthik/Desktop/Varshini/Phonepe/pulse/data/map/transaction/hover/country/india/state/"
aggreg_user=os.listdir(path2)

clm2={'State':[], 'Year':[],'Quater':[], 'District':[], 'Transaction_count':[],'Transaction_amount':[]}

for i in aggreg_user:
    p_i=path2+i+"/"
    Agg_yr=os.listdir(p_i)
    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            F=json.load(Data)
            
            for z in F['data']['hoverDataList']:
                    Name=z['name']
                    Count=z['metric'][0]['count']
                    Amount=z['metric'][0]['amount']
                    clm2['District'].append(Name)
                    clm2['Transaction_count'].append(Count)
                    clm2['Transaction_amount'].append(Amount)
                    clm2['State'].append(i)
                    clm2['Year'].append(j)
                    clm2['Quater'].append(int(k.strip('.json')))
                                          
#Succesfully created a dataframe
Map_Trans=pd.DataFrame(clm2)

Map_Trans['State']=Map_Trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
Map_Trans['State']=Map_Trans['State'].str.replace("-"," ")
Map_Trans['State']=Map_Trans['State'].str.title()
Map_Trans['State']=Map_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

#MAP User

path3="C:/Users/varshinikarthik/Desktop/Varshini/Phonepe/pulse/data/map/user/hover/country/india/state/"
map_user=os.listdir(path2)

clm3={'State':[], 'Year':[],'Quater':[], 'District':[], 'Registered_user':[],'Appopens':[]}

for i in aggreg_user:
    p_i=path3+i+"/"
    Agg_yr=os.listdir(p_i)
    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            F=json.load(Data)
            
            for z in F['data']['hoverData'].items():       
                    district=z[0]
                    registered_user=z[1]['registeredUsers']
                    appOpens=z[1]['appOpens']
                    clm3['District'].append(district)
                    clm3['Registered_user'].append(registered_user)
                    clm3['Appopens'].append(appOpens)
                    clm3['State'].append(i)
                    clm3['Year'].append(j)
                    clm3['Quater'].append(int(k.strip('.json')))
                                          
#Succesfully created a dataframe
Map_User=pd.DataFrame(clm3)

Map_User['State']=Map_User['State'].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
Map_User['State']=Map_User['State'].str.replace("-"," ")
Map_User['State']=Map_User['State'].str.title()
Map_User['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

#Top Transaction 

path4="C:/Users/varshinikarthik/Desktop/Varshini/Phonepe/pulse/data/top/transaction/country/india/state/"
top_trans=os.listdir(path4)

clm4={'State':[], 'Year':[],'Quater':[], 'Pincode':[], 'Transaction_count':[],'Transaction_amount':[]}

for i in aggreg_user:
    p_i=path4+i+"/"
    Agg_yr=os.listdir(p_i)
    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            E=json.load(Data)
            
            for z in E['data']['pincodes']:       
                    pincode=z['entityName']
                    count=z['metric']['count']
                    amount=z['metric']['amount']
                    clm4['Pincode'].append(pincode)
                    clm4['Transaction_count'].append(count)
                    clm4['Transaction_amount'].append(amount)
                    clm4['State'].append(i)
                    clm4['Year'].append(j)
                    clm4['Quater'].append(int(k.strip('.json')))
                                          
#Succesfully created a dataframe
top_trans=pd.DataFrame(clm4)

top_trans['State']=top_trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
top_trans['State']=top_trans['State'].str.replace("-"," ")
top_trans['State']=top_trans['State'].str.title()
top_trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

#Top User
path5='C:/Users/varshinikarthik/Desktop/Varshini/Phonepe/pulse/data/top/user/country/india/state/'

top_use=os.listdir(path5)

clm5={'State':[], 'Year':[],'Quater':[], 'Pincode':[], 'RegisteredUsers':[]}


for i in aggreg_user:
    p_i=path5+i+"/"
    Agg_yr=os.listdir(p_i)
    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            R=json.load(Data)
            
            for z in R['data']['pincodes']:       
                    pincode=z['name']
                    registeredUsers=z['registeredUsers']
                    clm5['Pincode'].append(pincode)
                    clm5['RegisteredUsers'].append(registeredUsers)
                    clm5['State'].append(i)
                    clm5['Year'].append(j)
                    clm5['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
top_user=pd.DataFrame(clm5)

top_user['State']=top_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & nicobar')
top_user['State']=top_user['State'].str.replace("-"," ")
top_user['State']=top_user['State'].str.title()
top_user['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')



# Custom CSS for styling
st.markdown(
    """
    <style>
    .main {
        background-color: #2A0944;
    }
    .navbar {
        background-color: #A160C6;
        padding: 10px;
        display: flex;
        justify-content: space-around;
        align-items: center;
    }
    .navbar a {
        color: #fff;
        text-decoration: none;
        padding: 14px 20px;
        font-size: 18px;
        border-radius: 5px;
    }
    .navbar a:hover {
        background-color: #6a1b9a;
    }
    .navbar a.active {
        background-color: #6a1b9a;
    }
    .header {
        text-align: center;
        color: 	#FFFFFF;
        margin: 20px 0;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Header
st.markdown('<h1 style="text-align: left;">PHONEPE PULSE </h1>', unsafe_allow_html=True)
st.write("The Heartbeat of India\'s Digital Revolution")

# Navigation bar
st.markdown(
    """
    <div class="navbar">
        <a href="#About project" id="about-link">About project</a>
        <a href="#home" id="home-link">Home</a>
        <a href="#geo-visualization" id="geo-link">Geo Visualization</a>
        <a href="#insights" id="insights-link">Insights</a>
        <a href="#Database" id="Database-link">Database</a>
    </div>
    """,
    unsafe_allow_html=True
)
# JavaScript for changing the page
st.markdown(
    """
    <script>
    document.querySelectorAll('.navbar a').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            document.querySelectorAll('.navbar a').forEach(a => a.classList.remove('active'));
            this.classList.add('active');
            var target = this.getAttribute('href').substring(1);
            document.querySelectorAll('.section').forEach(section => {
                section.style.display = 'none';
            });
            document.getElementById(target).style.display = 'block';
        });
    });
    </script>
    """,
    unsafe_allow_html=True
)

# Define sections
st.markdown('<div id="About Project" class="section;">', unsafe_allow_html=True)
st.write("## About")
st.write("Project name: Phonepe Pulse Data Visualization and Exploration:.")
st.write("Accomplished By Devavarshini")
st.markdown("Technology used in the project")
st.write("1,Python")
st.write("2.Plotly")
st.write("3.Pandas")
st.write("4.SQL")
st.write("5.SQLALchemy")
st.write("6.Streamlit")
st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div id="home" class="section">', unsafe_allow_html=True)
st.write("## Home")
# Header
st.title("Welcome to PhonePe Payment App")
st.write("Bringing you a seamless payment experience")

st.title("About PhonePe")

# Introduction
st.header("Introduction")
st.write("""
PhonePe is a leading digital payments platform in India, founded in December 2015. The company is revolutionizing the way people make payments, send money, and manage their finances.
""")

# Features
st.header("Features")
st.write("""
PhonePe offers a variety of features, including:
- **UPI Payments:** Fast and secure UPI payments that allow you to send and receive money instantly.
- **Recharge & Bill Payments:** Pay your utility bills, recharge your phone, DTH, and more with ease.
- **Financial Services:** Invest in mutual funds, buy insurance, and gold.
- **Merchant Payments:** Accept payments from customers via QR code, UPI, and other digital payment methods.
- **Shopping:** Shop from various categories directly from the PhonePe app.

PhonePe also supports a wide range of banks and financial institutions, making it accessible to a vast user base.
""")

# Security
st.header("Security")
st.write("""
PhonePe prioritizes the security of its users' data and transactions. It employs state-of-the-art security measures, including:
- **Data Encryption:** Ensuring that all transactions and user data are encrypted and secure.
- **Two-Factor Authentication:** Adding an extra layer of security for account access and transactions.
- **Fraud Detection:** Implementing advanced algorithms to detect and prevent fraudulent activities.

These security measures ensure that users can trust PhonePe with their financial transactions.
""")

# Achievements
st.header("Achievements")
st.write("""
Since its inception, PhonePe has achieved significant milestones:
- **200+ Million Users:** PhonePe has over 200 million registered users.
- **Billion Transactions:** The platform has processed billions of transactions since its launch.
- **Partnerships:** Collaborations with major banks, financial institutions, and businesses across India.

These achievements underscore PhonePe's position as a leader in the digital payments space.
""")

# Website link
st.header("Learn More")
st.write("For more information, visit the [official PhonePe website](https://www.phonepe.com/).")

# Footer
st.write("This information is sourced from the official PhonePe website.")

# Download PhonePe App
st.write("""
    ## Download PhonePe App
    Experience seamless payments with the PhonePe app.
""")
if st.button("Download Now"):
    st.write("Your download will begin shortly. Enjoy using PhonePe!")

st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div id="geo-visualization" class="section">', unsafe_allow_html=True)
st.write("## Geo Visualization")

Map_Trans['Year'] = Map_Trans['Year'].astype(int)
top_trans['Year'] = top_trans['Year'].astype(int)

#transaction Year analysis
def Transaction_analysis(df, Year):
    ta = df[df['Year'] == Year]
    agg_trans_state = ta.groupby('State')[['Transaction_count', 'Transaction_amount']].sum().reset_index()
    col1, col2 = st.columns(2)
    with col1:    
        fig_amount = px.bar(
                agg_trans_state,
                x="State",
                y="Transaction_amount",
                title=f"{Year} Transaction Amount",
                color_discrete_sequence=px.colors.sequential.Agsunset,
                height=580,
                width=520
            )
        st.plotly_chart(fig_amount)
    with col2:
        fig_count = px.bar(
                agg_trans_state,
                x="State",
                y="Transaction_count",
                title=f"{Year} Transaction Count",
                color_discrete_sequence=px.colors.sequential.Redor,
                height=580,
                width=520
            )
        st.plotly_chart(fig_count)

    # Load the geojson data for India states
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = response.json()
    
    # Create a sorted list of state names
    states = sorted([feature["properties"]["ST_NM"] for feature in data["features"]])
    col1, col2 = st.columns(2)
    with col1:
        # Choropleth map for Transaction Amount
        fig_india = px.choropleth(
                agg_trans_state,
                geojson=data,
                locations="State",
                featureidkey="properties.ST_NM",
                color="Transaction_amount",
                color_continuous_scale="rainbow",
                range_color=(agg_trans_state["Transaction_amount"].min(), agg_trans_state["Transaction_amount"].max()),
                hover_data=["State"],
                title="Transaction Amount",
                fitbounds="locations",
                height=600,
                width=600
            )
        fig_india.update_geos(visible=False)
        st.plotly_chart(fig_india)
    with col2:
        fig_india1 = px.choropleth(
                agg_trans_state,
                geojson=data,
                locations="State",
                featureidkey="properties.ST_NM",
                color="Transaction_count",
                color_continuous_scale="rainbow",
                range_color=(agg_trans_state["Transaction_count"].min(), agg_trans_state["Transaction_count"].max()),
                hover_data=["State"],
                title="Transaction Count",
                fitbounds="locations",
                height=600,
                width=600
            )
        fig_india1.update_geos(visible=False)
        st.plotly_chart(fig_india1)

    return ta

#Transaction Type analysis
def Transaction_analysis_by_type(df, State):
    # Filter the DataFrame by the selected state
    ta = df[df['State'] == State]

    # Check if 'Transaction_type' column exists
    if 'Transaction_type' not in ta.columns:
        st.error("The selected dataset does not contain 'Transaction_type' column.")
        return

    agg_trans_state_type = ta.groupby('Transaction_type')[['Transaction_count', 'Transaction_amount']].sum().reset_index()
    col1, col2 = st.columns(2)
    with col1:
        fig_amount = px.pie(
            data_frame=agg_trans_state_type,
            values="Transaction_amount",
            names="Transaction_type",
            title="Transaction Amount by Type",
            height=580,
            width=520,
            hole=0.1,
            color_discrete_sequence=px.colors.sequential.Agsunset
        )
        st.plotly_chart(fig_amount)
    with col2:
        fig_count = px.pie(
            data_frame=agg_trans_state_type,
            values="Transaction_count",
            names="Transaction_type",
            title="Transaction Count by Type",
            height=580,
            width=520,
            hole=0.1,
            color_discrete_sequence=px.colors.sequential.Redor
        )
        st.plotly_chart(fig_count)

#quater analysis
def Transaction_amount_count_Y_Q(df, quater):
    ta = df[df["Quater"] == quater]
    tacyg = ta.groupby("State")[["Transaction_count", "Transaction_amount"]].sum().reset_index()

    col1, col2 = st.columns(2)
    with col1:
        fig_ta = px.bar(
            tacyg,
            x="State",
            y="Transaction_amount",
            title=f"{ta['Year'].min()} YEAR {quater} QUARTER TRANSACTION AMOUNT",
            color_discrete_sequence=px.colors.sequential.Aggrnyl,
            height=650,
            width=600
        )
        st.plotly_chart(fig_ta)

    with col2:
        fig_co = px.bar(
            tacyg,
            x="State",
            y="Transaction_count",
            title=f"{ta['Year'].min()} YEAR {quater} QUARTER TRANSACTION COUNT",
            color_discrete_sequence=px.colors.sequential.Bluered_r,
            height=650,
            width=600
        )
        st.plotly_chart(fig_co)

    col1, col2 = st.columns(2)
    with col1:
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)
        states_name = [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name.sort()

        fig_india_1 = px.choropleth(
            tacyg,
            geojson=data1,
            locations="State",
            featureidkey="properties.ST_NM",
            color="Transaction_amount",
            color_continuous_scale="Viridis",
            range_color=(tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
            hover_name="State",
            title=f"{ta['Year'].min()} YEAR {quater} QUATER TRANSACTION AMOUNT",
            fitbounds="locations",
            height=600,
            width=600
        )
        fig_india_1.update_geos(visible=False)
        st.plotly_chart(fig_india_1)

    with col2:
        fig_india_2 = px.choropleth(
            tacyg,
            geojson=data1,
            locations="State",
            featureidkey="properties.ST_NM",
            color="Transaction_count",
            color_continuous_scale="Viridis",
            range_color=(tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
            hover_name="State",
            title=f"{ta['Year'].min()} YEAR {quater} QUARTER TRANSACTION COUNT",
            fitbounds="locations",
            height=600,
            width=600
        )
        fig_india_2.update_geos(visible=False)
        st.plotly_chart(fig_india_2)

    return ta

#Aggregared User wih Brand

Agg_User['Year'] = Agg_User['Year'].astype(int)
def Aggregated_User_Brand(df,Year):
    agguser=df[df["Year"] == Year]
    au=agguser.groupby("brand")[["Transaction_count","percentage"]].sum().reset_index()
    col1, col2 = st.columns(2)

    with col1:
        fig_tcount = px.bar(
                au,
                x="brand",
                y="Transaction_count",
                title=f"{Year}Transaction Count",
                hover_name="brand",
                color_discrete_sequence=px.colors.sequential.Agsunset,
                height=520,
                width=520
            )
        st.plotly_chart(fig_tcount)

    with col2:
        fig_count = px.bar(
                au,
                x="brand",
                y="percentage",
                title="Percentage",
                hover_name="brand",
                color_discrete_sequence=px.colors.sequential.Redor,
                height=520,
                width=520
            )
        st.plotly_chart(fig_count)
    return agguser    
#Aggregated user with Quater
def Aggre_user_plot_2(df,quater):
    au= df[df["Quater"] == quater]
    au.reset_index(drop= True, inplace= True)
    col1, col2 = st.columns(2)
    with col1:
        fig_bar= px.bar(data_frame=au, 
                      x= "brand", 
                      y="Transaction_count", 
                      hover_data= "percentage",
                      width=1000,
                      title=f"TRANSACTION COUNT PERCENTAGE by {quater} QUARTER ",
                      color_discrete_sequence= px.colors.sequential.PuBuGn_r)
        st.plotly_chart(fig_bar)
    return au
#Aggrregated User analysis with state

def Aggre_user_plot_3(df, state):
    agq = df[df["State"] == state]
    agq.reset_index(drop=True, inplace=True)

    aguqyg = pd.DataFrame(agq.groupby("brand")["Transaction_count"].sum())
    aguqyg.reset_index(inplace=True)

    fig_area = px.area(aguqyg, x="brand", y="Transaction_count", title=f"Transaction Count by Brand in {state}",
                       labels={"brand": "Brand", "Transaction_count": "Transaction Count"}, width=1000)
    st.plotly_chart(fig_area)
    return agq
#Map Analysis

def map_Transaction_District(df, state):
    # Filter data for the selected state
    maptr = df[df["State"] == state]
    ms = maptr.groupby("District")[["Transaction_count", "Transaction_amount"]].sum()
    ms.reset_index(inplace=True)
    col1, col2 = st.columns(2)
    with col1:
        fig_map_bar_1 = px.bar(ms, 
                            x="District", 
                            y="Transaction_amount", 
                            width=600, 
                            height=500, 
                            title=f"District Transaction Amount of {state}",
                            color_discrete_sequence=px.colors.sequential.Mint_r)
        
        # Display the first bar chart in Streamlit
        st.plotly_chart(fig_map_bar_1)
    with col2:
        # Create the second bar chart for transaction count
        fig_map_bar_2 = px.bar(ms, 
                            x="District", 
                            y="Transaction_count", 
                            width=600, 
                            height=500, 
                            title=f"District Transaction Count of {state}",
                            color_discrete_sequence=px.colors.sequential.Bluered_r)
        
        # Display the second bar chart in Streamlit
        st.plotly_chart(fig_map_bar_2)
    return maptr
Map_Trans['Year'] = Map_Trans['Year'].astype(int)
Map_User['Year'] = Map_User['Year'].astype(int)
top_trans['Year']=top_trans['Year'].astype(int)
top_user['Year']=top_user['Year'].astype(int)

#Map user using year analysing appopen and registered users
def map_user_plot_geo(df, year):
    mapyear = df[df["Year"] == year]
    mapyear.reset_index(drop=True, inplace=True)
    muyg = mapyear.groupby("State")[["Registered_user", "Appopens"]].sum()
    muyg.reset_index(inplace=True)
    
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    india_states_geojson = json.loads(response.content)

    fig_map_registered_user = px.choropleth(
        muyg,
        geojson=india_states_geojson,
        locations="State",
        featureidkey="properties.ST_NM",
        color="Registered_user",
        color_continuous_scale=px.colors.sequential.Viridis_r,
        title=f"{year} Registered Users by State",
        hover_data=["State", "Registered_user"],
        height=600,
        width=800
    )
    fig_map_registered_user.update_geos(fitbounds="locations", visible=False)

    fig_map_appopens = px.choropleth(
        muyg,
        geojson=india_states_geojson,
        locations="State",
        featureidkey="properties.ST_NM",
        color="Appopens",
        color_continuous_scale=px.colors.sequential.Viridis_r,
        title=f"{year} App Opens by State",
        hover_data=["State", "Appopens"],
        height=600,
        width=800
    )
    fig_map_appopens.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig_map_registered_user)
    st.plotly_chart(fig_map_appopens)
    return mapyear

def Map_User_regis(df, state):
    mapuser = df[df["State"] == state]
    mu = mapuser.groupby("District")[["Registered_user", "Appopens"]].sum().reset_index()
    col1, col2 = st.columns(2)
    with col1:
        fig_re = px.bar(
            mu,
            x="District",
            y="Registered_user",
            title=f"Registered Users in {state}",
            color_discrete_sequence=px.colors.sequential.YlGnBu_r,
            height=580,
            width=520
        )
        st.plotly_chart(fig_re)
    with col2:
        fig_app = px.bar(
            mu,
            x="District",
            y="Appopens",
            title=f"App Opens in {state}",
            color_discrete_sequence=px.colors.sequential.Oranges_r,
            height=580,
            width=520
        )
        st.plotly_chart(fig_app)
    return mapuser

def map_user_Quater(df, quater):
    mq= df[df["Quater"] == quater]
    mq.reset_index(drop= True, inplace= True)
    muyqg= mq.groupby("State")[["Registered_user", "Appopens"]].sum()
    muyqg.reset_index(inplace= True)

    fig_map_user_plot_1= px.line(muyqg, x= "State", y= ["Registered_user", "Appopens"], markers= True,
                                title= f"{df['Year'].min()}, {quater} QUARTER REGISTERED USER AND APPOPENS",
                                width= 1000,height=800,color_discrete_sequence= px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_map_user_plot_1)

    return mq

#top transaction analysis:
def top_user_year(df,year):
    tuy= df[df["Year"] == year]
    tuy.reset_index(drop= True, inplace= True)

    tuyg= pd.DataFrame(tuy.groupby(["State"])["RegisteredUsers"].sum())
    tuyg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuyg, x= "State", y= "RegisteredUsers", barmode= "group",
                            width=1000, height= 800, color_continuous_scale= px.colors.sequential.Burgyl)
    st.plotly_chart(fig_top_plot_1)

    return tuy

def top_user_state(df,state):
    tuys= df[df["State"] == state]
    tuys.reset_index(drop= True, inplace= True)

    tuysg= pd.DataFrame(tuys.groupby("Quater")["RegisteredUsers"].sum())
    tuysg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuys, x= "Quater", y= "RegisteredUsers",barmode= "group",
                           width=1000, height= 800,color= "Quater",hover_data="Pincode",
                            color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_plot_1)
    return tuys

tab1,tab2,tab3=st.tabs(["Agrregated analysis","Map Analysis","Top Analysis"])
#Map user analysis


#Stearmlit Aggregate
with tab1:
   
    method1=st.radio("Select the method",["Agrregated Transaction","Agrregated User"])
    
    if method1 == "Agrregated Transaction":
        col1,col2,col3,col4=st.columns(4)
        with col1:
         years = st.selectbox("Select the year", options=[2018, 2019, 2020, 2021, 2022])
        st.write(f"Selected Year: {years}")
        aggre_trans_y =pd.DataFrame(Transaction_analysis(Agg_Trans,years))
        
        col1,col2=st.columns(2)
        
        with col1:
            st.markdown ("Analysis using transaction type")
            states=st.selectbox("Select The State",aggre_trans_y["State"].unique())

        aggre_trans_t= Transaction_analysis_by_type(aggre_trans_y,states)
        
        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using Quater")
            Quaters=st.selectbox("select Quater",Agg_Trans["Quater"].unique())
        aggre_trans_q=Transaction_amount_count_Y_Q(aggre_trans_y, Quaters)

    elif method1 == "Agrregated User":
        col1,col2,col3,col4=st.columns(4)
        with col1:
         years = st.selectbox("Select the year", options=[2018, 2019, 2020, 2021, 2022],key="year1")
        st.write(f"Selected Year: {years}")
        Agg_User_Brand= Aggregated_User_Brand(Agg_User,years)
        
        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using Quater")
            Quaters=st.selectbox("select Quater",Agg_Trans["Quater"].unique(),key="Quater1")
        Agg_User_Quater= Aggre_user_plot_2(Agg_User_Brand,Quaters)
       
        col1,col2=st.columns(2)
        
        with col1:
            st.markdown ("Analysis using transaction type")
            states=st.selectbox("Select The State",Agg_Trans["State"].unique(),key="state1")
            Agg_User_State= Aggre_user_plot_3(Agg_User_Quater, states)
#Streamlit Map Analysis

with tab2:
    method2=st.radio("select the method",["Map Transaction", "Map User"])
    
    if method2 == "Map Transaction":
        col1,col2,col3,col4=st.columns(4)
        with col1:
         years = st.selectbox("Select the year", options=[2018, 2019, 2020, 2021, 2022, 2023,2024],key="year2")
        st.write(f"Selected Year: {years}")
        map_trans_Year= Transaction_analysis(Map_Trans,years)

        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using district data")
            states=st.selectbox("Select The State",Map_Trans["State"].unique(),key="state2")       
        map_trans_dist= map_Transaction_District(map_trans_Year, states) 

        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using Quater")
            Quaters=st.selectbox("select Quater",Map_Trans["Quater"].unique(),key="quater5")
        map_trans_q= Transaction_amount_count_Y_Q (map_trans_Year, Quaters)
       
    elif method2 == "Map User":
        col1,col2,col3,col4=st.columns(4)
        st.markdown ("Analysis using Year data")
       
        with col1:
         years = st.selectbox("Select the year", options=[2018, 2019, 2020, 2021, 2022, 2023, 2024],key="year3")
        st.write(f"Selected Year: {years}")
        map_user_Year = map_user_plot_geo(Map_User, years)

        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using district data")
            states=st.selectbox("Select The State",Map_Trans["State"].unique(),key="state3")  
        map_user_dist= Map_User_regis(map_user_Year, states)
        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using Quater")
            Quaters=st.selectbox("select Quater",Map_Trans["Quater"].unique(),key="quater2")        
        map_user_quater = map_user_Quater(map_user_Year, Quaters)

with tab3:
    method3=st.radio("select the method",["Top Transaction","Top User"])
    if method3 == "Top Transaction":
        col1,col2,col3,col4=st.columns(4)
        with col1:
         years = st.selectbox("Select the year", options=[2018, 2019, 2020, 2021, 2022],key="y1")
        st.write(f"Selected Year: {years}")
        top_trans_y =pd.DataFrame(Transaction_analysis(top_trans,years))
        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using Quater")
            Quaters=st.selectbox("select Quater",Agg_Trans["Quater"].unique(),key="q1")
        map_trans_q=Transaction_amount_count_Y_Q(top_trans_y, Quaters)
        col1,col2=st.columns(2)

    elif method3 == "Top User":
        col1,col2,col3,col4=st.columns(4)
        with col1:
         years = st.selectbox("Select the year", options=[2018, 2019, 2020, 2021, 2022],key="y1")
        st.write(f"Selected Year: {years}")
        top_user_y =pd.DataFrame(top_user_year(top_user,years))
        col1,col2=st.columns(2)
        with col1:
            st.markdown ("Analysis using State data")
            states=st.selectbox("Select The State",Map_Trans["State"].unique(),key="state3")  
        map_user_dist= top_user_state(top_user_y, states)        
                     


st.markdown('<div id="insights" class="section;">', unsafe_allow_html=True)
st.write("## Insights")

ques= st.selectbox("**Select the Question**", ("1,What are the top 5 states with the highest transaction amounts?",
                    "2,How has the number of registered users changed over the years?",
                    "3,Which states have the highest transaction counts in the most recent year?",
                    "4,Districts With Highest Transaction Amount?",
                    "5,Top 10 Districts With Lowest Transaction Amount?",
                    "6,What are the Top 10 States With AppOpens",
                    "7,Which states have the lowest number of app opens?",
                    "8,What are the transaction amounts and counts for the top 5 transaction types?",
                    "9,What are  the Top Brands Of Mobiles Used?",
                    "10,What are the top 5 districts with the highest registered users?"))

Agg_Trans['Transaction_amount'] = Agg_Trans['Transaction_amount'].astype(int)

def top_5_states_highest_transaction_amounts():
    state = Agg_Trans.groupby('State')['Transaction_amount'].sum().nlargest(5).reset_index()
    fig_state = px.bar(state, x='State', y='Transaction_amount', title="Top 5 States with Highest Transaction Amounts",
                       color_discrete_sequence=px.colors.sequential.Viridis)
    st.plotly_chart(fig_state)

def registered_users_over_years():
    users_yearly = Map_User.groupby('Year')['Registered_user'].sum().reset_index()
    fig_users = px.line(users_yearly, x='Year', y='Registered_user', title="Number of Registered Users Over the Years",
                        markers=True, color_discrete_sequence=px.colors.sequential.Inferno)
    st.plotly_chart(fig_users)

def states_highest_transaction_counts_recent_year():
    recent_year = Agg_Trans['Year'].max()
    recent_data = Agg_Trans[Agg_Trans['Year'] == recent_year]
    state = recent_data.groupby('State')['Transaction_count'].sum().nlargest(5).reset_index()
    fig_state = px.bar(state, x='State', y='Transaction_count', title=f"States with Highest Transaction Counts in {recent_year}",
                    color_discrete_sequence=px.colors.sequential.Blues)
    st.plotly_chart(fig_state)

def Districts_With_Highest_Transaction_Amount():
    htd= Map_Trans[["District", "Transaction_amount"]]
    htd1= htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "District", title="TOP 10 DISTRICTS OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def Districts_With_Lowest_Transaction_Amount():
    htd= Map_Trans[["District", "Transaction_amount"]]
    htd1= htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "District", title="TOP 10 DISTRICTS OF LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_htd)
def Top_10_States_With_AppOpens():
    sa= Map_User[["State", "Appopens"]]
    sa1= sa.groupby("State")["Appopens"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "State", y= "Appopens", title="Top 10 States With Appopens",
                color_discrete_sequence= px.colors.sequential.deep_r)
    return st.plotly_chart(fig_sa)
def states_lowest_app_opens():
    state = Map_User.groupby('State')['Appopens'].sum().nsmallest(5).reset_index()
    fig_state = px.bar(state, x='State', y='Appopens', title="States with Lowest Number of App Opens",
                        color_discrete_sequence=px.colors.sequential.Greys)
    st.plotly_chart(fig_state)
def transaction_amounts_counts_by_type():
    trans_type = Agg_Trans.groupby('Transaction_type')[['Transaction_amount', 'Transaction_count']].sum().reset_index()
    fig_amount = px.bar(trans_type, x='Transaction_type', y='Transaction_amount', title="Transaction Amounts by Type",
                        color_discrete_sequence=px.colors.sequential.Purp)
    fig_count = px.bar(trans_type, x='Transaction_type', y='Transaction_count', title="Transaction Counts by Type",
                        color_discrete_sequence=px.colors.sequential.Sunset)
    st.plotly_chart(fig_amount)
    st.plotly_chart(fig_count)
def transaction_amounts_counts_by_month():
    brand= Agg_User[["brand","Transaction_count"]]
    brand1= brand.groupby("brand")["Transaction_count"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.pie(brand2, values= "Transaction_count", names= "brand", color_discrete_sequence=px.colors.sequential.dense_r,
                       title= "Top Mobile Brands of Transaction_count")
    return st.plotly_chart(fig_brands)

def top_5_districts_highest_registered_users():
    district = Map_User.groupby('District')['Registered_user'].sum().nlargest(5).reset_index()
    fig_district = px.bar(district, x='District', y='Registered_user', title="Top 5 Districts with Highest Registered Users",
                            color_discrete_sequence=px.colors.sequential.Inferno)
    st.plotly_chart(fig_district)

if ques == ("1,What are the top 5 states with the highest transaction amounts?"):
    top_5_states_highest_transaction_amounts()
elif ques == ("2,How has the number of registered users changed over the years?"):
    registered_users_over_years()
elif ques == ("3,Which states have the highest transaction counts in the most recent year?"):
    states_highest_transaction_counts_recent_year()
elif ques == ("4,Districts With Highest Transaction Amount?"):
    Districts_With_Highest_Transaction_Amount()
elif ques == ("5,Top 10 Districts With Lowest Transaction Amount"):
    Districts_With_Lowest_Transaction_Amount()
elif ques == ("6,What are the Top 10 States With AppOpens"):
    Top_10_States_With_AppOpens()
elif ques == ("7,Which states have the lowest number of app opens?"):
    states_lowest_app_opens()
elif ques == ("8,What are the transaction amounts and counts for the top 5 transaction types?"):
    transaction_amounts_counts_by_type()
elif ques == ("9,What are  the Top Brands Of Mobiles Used?"):
    transaction_amounts_counts_by_month()
elif ques == ("10,What are the top 5 districts with the highest registered users?"):
    top_5_districts_highest_registered_users()


st.markdown('<div id="Database" class="section">', unsafe_allow_html=True)
st.write("## Database")


# Function to fetch data from a given table
def fetch_data(table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine)
    return df

# List of your table names
table_names = {
    "Aggregated transaction": "agg_trans",
    "Aggregated user": "agg_user",
    "Map Transaction": "map_trans",
    "map user": "map_user",
    "Top Transaction": "top_trans",
    "Top User": "top_user"
}

# Initialize session state for navigation
if 'table_selected' not in st.session_state:
    st.session_state['table_selected'] = None

# Main function to run the Streamlit app
def main():
    st.title('Database Viewer')

    # Create buttons for each table
    for label, table_name in table_names.items():
        if st.button(f"Show {label}"):
            st.session_state['table_selected'] = table_name

    # Display data from the selected table
    if st.session_state['table_selected']:
        data = fetch_data(st.session_state['table_selected'])
        st.write(f"Data from {st.session_state['table_selected']}:")
        st.dataframe(data)
if __name__ == '__main__':
    main()
#st.write("Tables in the database")
#dbselect =st.radio("Select the table name to fetch the data from DB",["Aggregated transaction","Aggregated user","Map Transaction",
                    #"map user","Top Transaction","Top User"])


#if dbselect == "Aggregated transaction"
# JavaScript for default active link
st.markdown(
    """
    <script>
    document.getElementById('home-link').classList.add('active');
    document.getElementById('home').style.display = 'block';
    </script>
    """,
    unsafe_allow_html=True
)