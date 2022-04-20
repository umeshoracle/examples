#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 10:27:27 2022

@author: Umesh Patel
"""

import sys
import streamlit as st
import json
import pandas as pd
import snowflake.connector
from datetime import datetime
import datetime as dt
import pytz
#  map chart
import pydeck as pdk

# for data frame tables display
from st_aggrid import AgGrid as stwrite
from st_aggrid.grid_options_builder import GridOptionsBuilder
# for role chart
import graphviz as graphviz
#annoated text
from annotated_text import annotated_text as atext

radiolist = {
    "Query": "query",
    "Time Travel & Cloning": "timetravel",
    "Charts": "chart",
    "Role Heirarchey": "rolechart",
}


def write_env(sess):
    df=exec_sql(sess,"select  current_region() region, current_account() account, current_user() user, current_role() role, current_warehouse() warehouse, current_database() database, current_schema() schema ")
    df.fillna("N/A",inplace=True)
    csp=df.at[0,"REGION"]
    cspcolor="#ff9f36"
    if "AWS"  in csp :
        cspcolor="#FF9900"
    elif "AZURE" in csp:
        cspcolor = "#007FFF"
    elif "GCP" in csp:
        cspcolor = "#4285F4"
    atext((csp,"REGION",cspcolor)," ",
          (df.at[0,"ACCOUNT"],"ACCOUNT","#2cb5e8")," ",
          (df.at[0,"USER"],"USER","#afa" ),          " ",
          (df.at[0,"ROLE"],"ROLE", "#fea"),       " ",
          (df.at[0,"WAREHOUSE"],"WAREHOUSE","#8ef"),     " ",
          (df.at[0,"DATABASE"],"DATABASE"),           " ",
          (df.at[0,"SCHEMA"],"SCHEMA"),
          )

def exec_sql(sess, query):
    try:
        df=pd.read_sql(query,sess)
    except:
            st.error("Oops! ", query, "error executin", sys.exc_info()[0], "occurred.")
    else:
        return df
    return




def create_session():
    with open('creds.json') as f:
        cp = json.load(f)
    conn = snowflake.connector.connect(
                    user=cp["user"],
                    password=cp["password"],
                    account=cp["account"],
                    warehouse=cp["warehouse"],
                    database=cp["database"],
                    role=cp["role"],
                    schema=cp["schema"]
                    )

    return conn


curr_sess = create_session()

def query():
    global curr_sess
    st.write("# Run Your Own Query")
    curr_wh=curr_sess.warehouse
    curr_wh=curr_wh.replace("\"","")
    sstr='show warehouses like \'' +curr_wh+'%\''
    whparam=exec_sql(curr_sess,sstr)
    whsize=whparam[["size"]].loc[0].item()
    whsize = st.select_slider(
            'Change Warehouse Size',
            options=['X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3x-Large', '4X-Large'],
                value=whsize)
    exec_sql(curr_sess,"alter warehouse "+curr_wh+" set warehouse_size=\'"+whsize+'\'')
    with st.form("q_form"):
        query_txt = st.text_area("Query")
        submitted = st.form_submit_button("Run")
        if submitted:
            # creates a new df and unions with existing data in Snowflake, writes to table
            st.write("Output for ", query_txt)
            qdf=exec_sql(curr_sess, query_txt)
            #st.write(qdf)
            #stwrite(qdf)
            gb = GridOptionsBuilder.from_dataframe(qdf)
            gb.configure_pagination()
            gb.configure_side_bar()
            gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            gridOptions = gb.build()
            stwrite(qdf, gridOptions=gridOptions,  height=600, enable_enterprise_modules=True)

def chart():
    global curr_sess
    st.write("# Streamlit Pydeck Chart ")
    st.subheader('Top 20 Popular Rides')
    df=exec_sql(curr_sess,'  select  \
          any_value(start_station) from_station_name, any_value(end_station) end_station_name, start_lat, start_lon, end_lat, end_lon, \
          count(*) num_trips, \
          avg(datediff("minute", starttime, stoptime))::integer avg_duration_mins \
          from citibike.public.trips_stations_weather_vw \
          where    \
          (start_lat is not null and end_lat is not null)  \
          and start_lat <> end_lat  \
          group by  start_lat, start_lon, end_lat, end_lon  \
          order by num_trips  desc \
         limit 20;')

    lay1 = pdk.Layer(
            "ArcLayer",
            data = df,
            get_source_position=["START_LON", "START_LAT"],
            get_target_position=["END_LON", "END_LAT"],
            #get_source_color=[200, 30, 0, 160],
            #get_target_color=[200, 30, 0, 160],
            get_source_color=[64, 255, 0],
            get_target_color=[0, 128, 200],
            auto_highlight=True,
            width_scale=0.0004,
            get_width="NUM_TRIPS",
            width_min_pixels=3,
            width_max_pixels=30,
        )
    lay2 = pdk.Layer(
            "TextLayer",
            data=df,
            get_position=["START_LON", "START_LAT"],
            get_text="FROM_STATION_NAME",
            get_color=[0, 0, 0, 200],
            get_size=15,
            get_alignment_baseline="'bottom'",
        )
    lay3 = pdk.Layer(
            "HexagonLayer",
            data=df,
            get_position=["START_LON", "START_LAT"],
            radius=200,
            elevation_scale=4,
            elevation_range=[0, 1000],
            extruded=True,
        )
    st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state={"latitude": 40.776676,
                                "longitude": -73.971321, "zoom": 11, "pitch": 50},
            layers=[lay1,lay2, lay3],
            tooltip={"text": "{FROM_STATION_NAME} to {END_STATION_NAME}"}
        ))
    stwrite(df)

def rolechart()    :
            global curr_sess
            st.subheader("Role Heirarchey")
            t=exec_sql(curr_sess,"use role accountadmin")
            write_env(curr_sess)
            sqlstr='\
                SELECT enabled_roles.role_name child, applicable_roles.grantee parent \
                FROM snowflake.information_schema.enabled_roles \
                JOIN snowflake.information_schema.applicable_roles ON \
                enabled_roles.role_name = applicable_roles.role_name \
            '
            rdf=exec_sql(curr_sess,sqlstr)
            rolechart = graphviz.Digraph()
            rolechart.attr("node", shape="doublecircle")
            rolechart.attr("node", color="#11567f")
            rolechart.attr( rankdir="BT")
            rolechart.attr( "node", fontsize="6pt")
            for num, row in rdf.iterrows():
                #st.write( row["CHILD"], row['PARENT'], type(row["CHILD"]))
                if row["CHILD"] == 'ACCOUNTADMIN':
                    rolechart.edge(row["CHILD"], row['PARENT'],color="red", arrowsize="3",size="double")
                else:
                    rolechart.edge(row["CHILD"], row['PARENT'])
            rolechart.node("ACCOUNTADMIN",style="filled",fillcolor="red",fontcolor="white")
            rolechart.node("SECURITYADMIN",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            rolechart.node("USERADMIN",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            rolechart.node("SYSADMIN",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            rolechart.node("PUBLIC",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")

            st.graphviz_chart(rolechart)

def timetravel():
    global curr_sess
    st.write("# Back in Time with Time Travel")

    curr_table = "CITIBIKE.PUBLIC.TRIPS"
    with st.form("tt_form"):
        str1 = st.text_input("Input Table for Time Travel",curr_table)
        needclone=st.checkbox("Clone (It will overwrite if table exists) ?")
        newtab=st.text_input("Enter new table name",str1+"_CLONE")
        submitted = st.form_submit_button("Go")
        if submitted:
            st.write("TimeTravel for ", str1)
            curr_table=str1

        if needclone:
                newtab=newtab.upper()
        qdf=exec_sql(curr_sess, 'show tables like \''+curr_table[curr_table.rindex('.')+1:].upper()+'\' in schema '+curr_table[:curr_table.rindex('.')].upper())
        st.write(qdf)
        tcreatedstr=qdf.get("created_on").to_string()[4:20]
        table_created=dt.datetime(int(tcreatedstr[0:4]),
                                     int(tcreatedstr[5:7]),
                                     int(tcreatedstr[8:10]),
                                     int(tcreatedstr[11:13])-7, ## convert to local time
                                     int(tcreatedstr[14:16])+1) ## round to min
        #table_created = datetime.datetime.strptime(tcreatedstr, '%Y-%m-%d %H:%M')
        st.write('Table Created :' ,table_created)
        ret_time=int(qdf.get("retention_time").to_string()[4:])
        st.write('Retention days',ret_time)
        tdt=datetime.today().astimezone(pytz.timezone("America/Los_Angeles"))
        end_date = datetime.strptime(tdt.strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')
        if table_created <= end_date - dt.timedelta(days=ret_time):
            table_created = end_date - dt.timedelta(days=ret_time)
        #st.write(table_created,end_date)
        asof_time = st.slider(
            "When do you want to go back in time for table "+curr_table,
            value=end_date,
            max_value=end_date,
            min_value=table_created,
            step=dt.timedelta(minutes=1),
            format="MM/DD/YY - HH:mm")

        st.write("Query as of :", asof_time)
        if  asof_time > end_date :
                st.write("in future")
                asof_time = end_date
        sqlstr='select * from '+curr_table+' at(timestamp => to_timestamp_ltz(\''+asof_time.strftime('%Y-%m-%d %H:%M:%S')+'\',\'yyyy-mm-dd hh:mi:ss\')) limit 1000'
        st.write("Executing...", sqlstr)
        # convert to timezone
        qtime=asof_time+dt.timedelta(hours=7)
        sdf=exec_sql(curr_sess,'select count(*) from '+curr_table+' at(timestamp => to_timestamp(\''+qtime.strftime('%Y-%m-%d %H:%M:%S')+'\',\'yyyy-mm-dd hh:mi:ss\')) ')
        st.write(sdf)
        sdf=exec_sql(curr_sess,sqlstr)
        stwrite(sdf)
        if needclone:
                clonesql='create or replace table ' +newtab+' clone '+ curr_table +  ' at (timestamp => to_timestamp(\''+asof_time.strftime('%Y-%m-%d %H:%M:%S')+'\',\'yyyy-mm-dd hh:mi:ss\')) '
                #st.write(clonesql)
                cdf=exec_sql(curr_sess,clonesql)
                st.write(cdf)
        #slider2 = st.slider('Select date', min_value=start_date, value=end_date ,max_value=end_date, format=format)
        #select_hour=st.slider('Select time', min_value=table_created, value=datetime.datetime.now(),  max_value=datetime.datetime.now() ,step=datetime.timedelta(hours=1), format=format)
        #st.write(select_hour)


def main():
    global curr_sess
    st.set_page_config(page_title='Awesome Snowflake', layout="wide")
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", list(radiolist.keys()))
    selectoption = radiolist[selection]
    with st.sidebar:
        write_env(curr_sess)
    possibles = globals().copy()
    possibles.update(locals())
    method = possibles.get(selectoption)
    if not method:
        raise NotImplementedError("Method %s not implemented" % method)
    method()
    st.sidebar.title("Documentation")
    st.sidebar.info(
        "Powered by Snowflake/Streamlit"
        "Here is documentations for [Snowflake](https://docs.snowflake.com/en/index.html) and [Streamlit](https://docs.streamlit.io/)"
        " Use this guide for setup [Snowflake Quickstarts](https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html?index=..%2F..index#0)"
    )

if __name__ == "__main__":
    main()
    st.snow()
