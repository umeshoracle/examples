#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 3, 2022

@author: umesh.patel@snowflake.com
"""
import streamlit as st
import json
import pandas as pd 
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *

from PIL import Image
from st_aggrid import AgGrid as stwrite,  GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from annotated_text import annotated_text as atext


cp = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"]
}



js = JsCode("""
function(e) {
    let api = e.api;
    let rowIndex = e.rowIndex;
    let col = e.column.colId;
    
    let rowNode = api.getDisplayedRowAtIndex(rowIndex);
    api.flashCells({
      rowNodes: [rowNode],
      columns: [col],
      flashDelay: 10000000000
    });
};
""")
#first col display value, second col procedure name

curr_env = ""
curr_role = ""
curr_user = ""
curr_account = ""
curr_env = ""

makeitsnow = True

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
          (df.at[0,"USER"],"USER","#afa","#01070d" ),          " ",
          (df.at[0,"ROLE"],"ROLE", "#fea","#01070d"),       " ",             
          (df.at[0,"WAREHOUSE"],"WAREHOUSE","#8ef","#01070d"),     " ",               
          (df.at[0,"DATABASE"],"DATABASE"),           " ",         
          (df.at[0,"SCHEMA"],"SCHEMA"),                           
          )

    

  
def create_session(fn):
    global curr_sess, curr_user, curr_role, curr_account, curr_env
    session = Session.builder.configs(cp).create()
    curr_sess = session
    curr_user=cp["user"].upper()
    curr_account=cp["account"].upper()
    curr_role=cp["role"].upper()
    session.query_tag="streamlit demo : "+curr_role
    return session

# not using at this point

def exec_sql(sess, query):
    try:
        rowset=sess.sql(query)
        #pd=df.toPandas();        
    except Exception as e:
            st.error("Oops! ", query, "error executing", str(e), "occurred.")
            return pd.DataFrame()
    else:
        try:
            tdf = pd.DataFrame(rowset.collect())
        except Exception as e1:
                st.error(str(e1))
                return pd.DataFrame()
        else:
            return tdf
    return 


def hybrid_search():
    global curr_sess, curr_role, curr_region, curr_env, curr_user, curr_account
    st.header('CRM Sales Force Account and Opportunties')
    ndf=exec_sql(curr_sess,"use demodb.unistore") 
    write_env(curr_sess)  
    acct = st.text_input('Enter search words:')
    submitted = st.button("Search Accounts")  
    #if submitted:
    adf=exec_sql(curr_sess,' select name,type, billingcountry country,segment__c segment, numberofemployees emps,  annualrevenue rev, accountnumber,id  from sfo_accounts  \
                                    where  id in (select accountid from sfo_opps) and  name ilike \'%'+acct+'%\' ' );
    
    if (len(adf.index)==0 ):
            st.write("Account not found")
    else:
        gd = GridOptionsBuilder.from_dataframe(adf)
        gd.configure_pagination(enabled=True)
        gd.configure_default_column(editable=True, groupable=True)
        gd.configure_selection(selection_mode="single", use_checkbox=True)
        gridoptions = gd.build()
        grid_table = stwrite(
                adf,
                gridOptions=gridoptions,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                allow_unsafe_jscode=True, reload_data=False ,
        theme="material",
        )
        sel_row = grid_table["selected_rows"]
        if not sel_row:
            aid=''
            aname=''
        else:
            aid=sel_row[0]["ID"]
            aname=sel_row[0]["NAME"]
            st.subheader("List of Opportunites for "+ aname)
            sdf=exec_sql(curr_sess,' select name,type  , closedate, stagename, to_char(amount) amount, forecastcategory,  leadsource,  opportunity_source__c, lastmodifieddate , id  from sfo_opps \
                                    where accountid= \''+aid+'\' ' );
            gb = GridOptionsBuilder.from_dataframe(sdf)
            gb.configure_default_column(groupable=True,  editable=True)
            gb.configure_selection(selection_mode="multiple", use_checkbox=True)
            gb.configure_grid_options(onCellValueChanged=js) 
            gd.configure_auto_height(autoHeight=True)
            gridOptions = gb.build()
            newedf=stwrite(sdf, gridOptions=gridOptions,  update_mode=GridUpdateMode.SELECTION_CHANGED,   allow_unsafe_jscode=True,  reload_data=False , theme="material")
            tdf=newedf["selected_rows"]     
            if (st.button("Save")):   
                st.write(tdf)        
                for row in tdf:
                    updstmt='update sfo_opps set  \
                    closedate=\''+row['CLOSEDATE']+'\' , \
                    amount=to_number(\''+row['AMOUNT']+'\') , \
                    stagename=\''+str(row['STAGENAME'])+'\', \
                    lastmodifieddate= current_timestamp \
                    where id=\''+str(row['ID'])+'\';'
                    udf=exec_sql(curr_sess,updstmt)                      
                    #st.write(updstmt)
                st.experimental_rerun()

        #st.dataframe(sdf)    
def hybridtable():
    global curr_sess, curr_role, curr_region, curr_env, curr_user, curr_account

    st.header("Snowflake Hybrid Table")
    hsess = curr_sess 
    #= create_session(connections["Hybrid Table Account"])
    ndf=exec_sql(hsess,"use demodb.unistore") 
    write_env(hsess)    
    #edf=exec_sql(hsess,"select ename, dname, job, empno, hiredate, loc  from emp, dept  where emp.deptno = dept.deptno  order by ename")
    ddf=exec_sql(hsess,"select dname,deptno  from  dept")   
    edf=exec_sql(hsess,"select ename,empno  from  emp")       
    #st.write(edf)    
    with st.form("empform"):
        st.subheader("Enter New Hire details")
        col1,col2 = st.columns([1,1])
        with col1:            
            pename = st.text_input("Name")            
            pjob = st.text_input("Job Title")
            psalary = st.slider("Salary",10000,300000,100000)        
        with col2:
            pmgr = st.selectbox('Manager',edf)
            pdept  = st.selectbox('Department',ddf)            
            phdate = st.date_input("Hire Date")        
        submitted = st.form_submit_button("Submit")
        if submitted:
            pdno=ddf[ddf["DNAME"]==pdept]["DEPTNO"].item()
            pmgrno=edf[edf["ENAME"]==pmgr]["EMPNO"].item()
            #st.write(pdno["DEPTNO"].item())
            # creates a new df and unions with existing data in Snowflake, writes to table
            #st.write("name:", pename, "| pjob:", pjob, "| phdate:", phdate, "| salary:", psalary, "Dept:",pdno, "MGR:",pmgrno)
            insstmt="insert into emp (empno, ename, job, mgr, hiredate, sal,  deptno)\
                     values (emp_seq.nextval, \'"+pename.upper()+"\', \'"+pjob.upper()+"\' , "+str(pmgrno)+", to_date(\'"+phdate.strftime("%d-%B-%Y")+"\') \
                             , "+str(psalary)+", "+str(pdno)+")"
            #st.write(insstmt)
            rdf=exec_sql(hsess,insstmt)
            #st.write(rdf)
            #newEmployeeDf=session.createDataFrame([Row(name=name_val, age=age_val, job=job_val, insider=insider_val)])
            #employeeDf = employeeDf.union(newEmployeeDf)
            #employeeDf.write.mode("overwrite").saveAsTable("employee")
    st.subheader("Update Employees")
    st.caption('1. Update in place 2.  Select row to update,  3. Save')
    edf=exec_sql(hsess,"select * from  emp order by empno desc")   
    gb = GridOptionsBuilder.from_dataframe(edf)
    gb.configure_pagination()
    gb.configure_default_column(groupable=True,  editable=True)
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(onCellValueChanged=js) 
    gridOptions = gb.build()
    newedf=stwrite(edf, gridOptions=gridOptions, update_mode=GridUpdateMode.SELECTION_CHANGED,  fit_columns_on_grid_load=True, allow_unsafe_jscode=True, reload_data=False , theme="blue")
    tdf=newedf["selected_rows"]     
    
    #st.write(pd.DataFrame(tdf))
    if (st.button("Save")):   
        #st.write(tdf)        
        for row in tdf:
            updstmt='update emp set  \
            ename=\''+row['ENAME']+'\' , \
            job=\''+row['JOB']+'\' , \
            mgr='+str(row['MGR'])+', \
            sal='+str(row['SAL'])+' , \
            comm='+str(row['COMM'])+', \
            deptno='+str(row['DEPTNO'])+' \
            where empno='+str(row['EMPNO'])+';'
            udf=exec_sql(hsess,updstmt)                      
        st.experimental_rerun()

PAGES = {     
    "Unistore (Insert/Update)": hybridtable,       
    "Unistore (Lookup)": hybrid_search,                      

}

def main():
    global curr_sess, curr_role, curr_region, curr_env, curr_user, curr_account
    img = Image.open('./snowlogo.png')
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", list(PAGES.keys()))

    page = PAGES[selection]
    
    #selected_conn = st.sidebar.selectbox("Select Snowflake Connection", options=connections.keys())
    curr_env="Main Account"
    curr_sess = create_session("snowflake")
   

    PAGES[selection]()
  
    st.sidebar.title("About Demo")
    st.sidebar.info(
        "This native app is to demonstrate various Snowflake and Streamlit features.  :joy:. "
        " Here is documentations for [Snowflake](https://docs.snowflake.com/en/index.html). and [Streamlit](https://docs.streamlit.io/)"
    )
    
            
 
if __name__ == "__main__":
    st.set_page_config(page_title='Awesome Snowflake',layout="wide",menu_items={     
         'About': "This is Demo for Snowflake SEs created by Umesh Patel"
     })
    main()
    if makeitsnow :
        #st.sidebar.snow()
        makeitsnow=False;
        
