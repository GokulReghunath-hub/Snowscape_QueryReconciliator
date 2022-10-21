#importing packages

from logging import exception
import os
import pytz 
import pyodbc
import datetime
from AthenaQueryExecutor import AthenaQueryExecutor 
import Config
import pandas as pd
import numpy as np
import snowflake.connector as sf
from random import randint
from snowflake.connector.pandas_tools import write_pandas
import time


#Creating SQL Server Connection
sqlserver_conn_dict = {}
for i in range(1,int(Config.sqlserver_db_count)+1):
    globals()[f"sqlserver_conn{i}"] = pyodbc.connect('Driver={SQL Server};'
                        'Server=IN2451286W1;'
                        'Database=SF_POC;'
                        'Trusted_Connection=yes;')
    sqlserver_conn_dict[f"SqlServer_Conn{i}"] = globals()[f"sqlserver_conn{i}"]                    

#creating first Snowflake connection
sf_conn_dict = {}
for i in range(1,int(Config.snowflake_db_count)+1):
    globals()[f"sf_conn{i}"] = sf.connect(
        user=Config.user,
        #password=Config.password,
        account=Config.account,
        authenticator='externalbrowser',
        role = Config.role,
        database = f"Config.database{i}",
        schema = f"Config.schema{i}",
        warehouse = Config.warehouse,
    )
    globals()[f"cur{i}"] = globals()[f"sf_conn{i}"] .cursor()
    sf_conn_dict[f"Snowflake_Conn{i}"] = globals()[f"sf_conn{i}"]

#Fetching current session id
sessionId = randint(1000001,9999999)
print("Your Session ID is : ",sessionId)

#Function to check whether the column value is numeric or not
def is_num(n):
    if pd.isna(n):
        return False
    elif isinstance(n, (int, np.integer)) or isinstance(n, (float, np.float)):
        return True
    else:
        return False

#Declaring Input files for reading packages and testids
df_masterfile = pd.read_excel('InputFiles\Master_File_Accelerator.xlsx')
df_detailedconnectionfile = pd.read_excel('InputFiles\Detailed Connection File.xlsx')

#Taking User Inputs and defining TestIDs based on the user input
print("How you want to run?\n  1 = Package vise\n  2 = Test Case vise")
runchoice = int(input("Enter your choice : "))
if runchoice == 1:
    pckgeid = input("Enter the Package IDs seperated by comma : ")
    li_ippckgeidstr =pckgeid.split(",")
    li_ippckgeid = [float(i) for i in li_ippckgeidstr]
    df_packageselectedmaster = df_masterfile[df_masterfile['PACKAGE_ID'].isin(li_ippckgeid)]
    pckgtestid_li =[]
    for rows in df_packageselectedmaster.itertuples():
        testid_li =float(rows.TEST_ID)
        pckgtestid_li.append(testid_li)
    df_selectedmaster = df_masterfile[df_masterfile['TEST_ID'].isin(pckgtestid_li)]
    df_selecteddetailedconnectionfile = df_detailedconnectionfile[df_detailedconnectionfile['TEST_ID'].isin(pckgtestid_li)]
    update_testid_li = pckgtestid_li
else:
    iptestidt = input("Enter the Test IDs seperated by comma : ")
    li_iptestidstr =iptestidt.split(",")
    li_iptestid = [float(i) for i in li_iptestidstr]
    df_selectedmaster = df_masterfile[df_masterfile['TEST_ID'].isin(li_iptestid)]
    df_selecteddetailedconnectionfile = df_detailedconnectionfile[df_detailedconnectionfile['TEST_ID'].isin(li_iptestid)]
    update_testid_li = li_iptestid
print("Do you want to reconcile queries?\n  1 = YES\n  2 = NO")
reconcilechoice = int(input("Enter your choice : "))
strt_time = time.time()
if reconcilechoice == 1:
    print("Output Format which you prefer?\n 1 = Excel \n 2 = Database\n 3 = Excel and Database")
    output_option = int(input("Enter your choice : "))
    acceptablevar = input("Enter acceptable percentage of difference in values : ")

    if(output_option != 1):
        #creating result Snowflake connection
        sf_result_conn = sf.connect(
            user=Config.sf_result_user,
            authenticator='externalbrowser',
            #password=Config.sf_result_password,
            account=Config.sf_result_account,
            role = Config.sf_result_role,
            database = Config.sf_result_db,
            schema = Config.sf_result_schema,
            warehouse = Config.sf_result_warehouse,
            )
        sf_result_cur = sf_result_conn.cursor()

    #Merging Master and Details input files based on user given TestIDs
    df_querysource = df_selectedmaster.merge(df_selecteddetailedconnectionfile, left_on='TEST_ID', right_on='TEST_ID', how='inner')
    df_querysource.sort_values("TEST_ID", axis = 0, ascending = True,
                    inplace = True, na_position ='last') 

    #Count Queries
    df_testquerycount = df_querysource[["TEST_ID","QUERY_FILENAME"]].groupby("TEST_ID").count().to_dict()
    dict_testquerycount = df_testquerycount["QUERY_FILENAME"]

    #Declaring Result dataframes           
    df_comparedoutput = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','ROW_ID', 'STATUS', 'DATA_GRANULARITY','DIMENSION_VALUES','MEASURE','MEASURE_VALUE','PERCENTAGE_DIFF','LATEST_RUN_FLAG','START_TIME','END_TIME'])
    df_summaryoutput = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','SQUAD','PACKAGE_NAME','TEST_NAME','STATUS', 'TOTAL_RECORD_COUNT','PASSED_RECORD_COUNT','FAILED_RECORD_COUNT','PASS_PERCENTAGE','LATEST_RUN_FLAG','START_TIME','END_TIME'])
    df_connectiondetails = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','TEST_NAME','CONNECTION', 'DATABASE','QUERY_PATH','SQL_DESCRIPTION','GRAIN_COL_COUNT','LATEST_RUN_FLAG','EXECUTION_ORDER'])
    df_failuresummary = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','TOTAL_FAILED_RECORD_COUNT','FAILED_RECORD_COUNT', 'FAILURE_CATEGORY','LATEST_RUN_FLAG'])

    #Starts processing Queries from input files
    processingtestId = -1
    querycount = 0
    print("Processing SQL Queries...")
    for index, row in df_querysource.iterrows():
        
        #declaring result variables
        testid = row["TEST_ID"]
        dbenv = row["DATABASE"]
        packageid = row["PACKAGE_ID"]
        testname = row["TEST_NAME"]
        packagename = row["PACKAGE_NAME"]
        queryfilename = "Input SQL Queries\\" + row["QUERY_FILENAME"]
        sqldesciption = row["SQL_DESCRIPTION"]
        dbconnection = row["CONNECTION"]
        grain = int(row["GRAIN_COL_COUNT"])
        squad = row['SQUAD']

        #Reading current time
        now = datetime.datetime.now()
        start_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        #Reading Query file
        fd = open(queryfilename, 'r')
        sqlFile = fd.read()
        fd.close()

        
        if processingtestId == testid:
            querycount += 1
        else:
            querycount = 1

        #Writing connection details to dataframe
        df_connectiondetails = df_connectiondetails.append({'SESSION_ID':sessionId,'PACKAGE_ID' : packageid,'TEST_ID' : testid,'TEST_NAME' : testname,'CONNECTION' : dbconnection, 'DATABASE' : dbenv,'QUERY_PATH': queryfilename,'SQL_DESCRIPTION' : sqldesciption,'GRAIN_COL_COUNT':grain,'LATEST_RUN_FLAG':'Y','EXECUTION_ORDER':querycount},ignore_index = True)
        

        #Executing querys in database
        try:
            if dbenv == 'Snowflake':
                globals()[f"df_queryresult_{querycount}"] = pd.read_sql_query(sqlFile,sf_conn_dict[row["CONNECTION"]])
                print("Snowflake query exeecution finished for testid:",testid)
            elif dbenv == 'SQL Server':
                globals()[f"df_queryresult_{querycount}"] = pd.read_sql_query(sqlFile,sqlserver_conn_dict[row["CONNECTION"]])
                print("SQL Server query exeecution finished for testid:",testid)
            elif dbenv == 'Athena':
                queryresult = AthenaQueryExecutor(query=sqlFile)
                globals()[f"df_queryresult_{querycount}"] = queryresult.run_query()
                print("Athena query exeecution finished for testid:",testid)
            globals()[f"df_queryresult_{querycount}"].fillna("NULLVALUE",inplace = True)
        except exception as msg:
            print("Input Query Execution failed.: ", msg)

        if processingtestId == testid and querycount == dict_testquerycount[testid]:
            rowid = 1
            if grain != 0:
                queryresult_cols = globals()["df_queryresult_1"].columns
                graincollist = ','.join(globals()["df_queryresult_1"].columns[:grain])
                dimlist = list(globals()["df_queryresult_1"].columns[:grain])
                measurelist = list(globals()["df_queryresult_1"].columns[grain:])
                measurelistcols = [col + f"_tq1" for col in measurelist]
                globals()["df_queryresult_1"].columns = dimlist + measurelistcols

                for i in range(1,dict_testquerycount[testid]):
                    globals()[f"df_queryresult_{i+1}"].columns = queryresult_cols
                    measurelistcols = [col + f"_tq{i+1}" for col in measurelist]
                    globals()[f"df_queryresult_{i+1}"].columns = dimlist + measurelistcols
                    if i == 1:
                        df_merged = globals()[f"df_queryresult_{i}"]
                    df_merged = pd.merge(df_merged,globals()[f"df_queryresult_{i+1}"],how='outer',left_on=list(df_merged.columns[:grain]),right_on=list(globals()[f"df_queryresult_{i+1}"].columns[:grain]))
                    
                for mergedindex, mergedrow in df_merged.iterrows():
                    strdimlist = [str(i) for i in mergedrow[:grain]]
                    dimlist = str(','.join(strdimlist))

                    for measurecol in queryresult_cols[grain:]:
                        mergeddimlist = []
                        status = 0
                        pervariance = 0
                        for i in range(1,dict_testquerycount[testid]):
                            querycol = measurecol+"_tq"+str(i)
                            nextquerycol = measurecol+"_tq"+str(i+1)          
                            if is_num(mergedrow[querycol]) and is_num(mergedrow[nextquerycol]):
                                pervariance = abs(round(((mergedrow[querycol] - mergedrow[nextquerycol])/mergedrow[querycol]) * 100,3))
                                if mergedrow[querycol] != mergedrow[nextquerycol] and pervariance > float(acceptablevar):
                                    status += 1
                            elif mergedrow[querycol] != mergedrow[nextquerycol]:
                                status += 1
                                pervariance = 100
                            if i == 1:
                                measurevaluelist = str(mergedrow[querycol])+" | "+str(mergedrow[nextquerycol])
                                perofdifflist = str(pervariance)
                            else:
                                measurevaluelist = measurevaluelist+" | "+str(mergedrow[nextquerycol])
                                perofdifflist = perofdifflist + ' | ' + str(pervariance)
                        
                            if pd.isna(mergedrow[querycol]):
                                mergeddimlist.append("NULLRECORD_TestQuery"+str(i))                  
                            else:
                                mergeddimlist.append(dimlist)
                            if i == dict_testquerycount[testid]-1:
                                if pd.isna(mergedrow[nextquerycol]):
                                    mergeddimlist.append("NULLRECORD_TestQuery"+str(i+1))
                                else:
                                    mergeddimlist.append(dimlist)
                        
                        if status == 0:
                            check_result = "Pass"
                        else:
                            check_result = "Fail"
                        resultdimlist = str(' | '.join(mergeddimlist))
                        #Fetching end time
                        now = datetime.datetime.now()
                        end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                        #Writing results to dataframe
                        
                        df_comparedoutput = df_comparedoutput.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'ROW_ID' : rowid,'STATUS' : check_result, 'DATA_GRANULARITY': str(graincollist), 'DIMENSION_VALUES' : resultdimlist ,'MEASURE' : measurecol,'MEASURE_VALUE' : measurevaluelist,'PERCENTAGE_DIFF' : perofdifflist,'LATEST_RUN_FLAG':'Y','START_TIME' : start_time,'END_TIME' : end_time}, 
                        ignore_index = True)
                    rowid += 1
            else:
                #Processing records with zero grain
                status = 0
                pervariance = 0

                for i in range(1,dict_testquerycount[testid]):
                    if is_num(globals()[f"df_queryresult_{i}"].iloc[0,0]) and is_num(globals()[f"df_queryresult_{i+1}"].iloc[0,0]):
                        pervariance = abs(round(((globals()[f"df_queryresult_{i}"].iloc[0,0] - globals()[f"df_queryresult_{i+1}"].iloc[0,0])/globals()[f"df_queryresult_{i}"].iloc[0,0]) * 100,3))
                        if ~globals()[f"df_queryresult_{i}"].equals(globals()[f"df_queryresult_{i+1}"]) and pervariance > float(acceptablevar):
                            status += 1
                    elif mergedrow[querycol] != mergedrow[nextquerycol]:
                        status += 1
                        pervariance = 100

                            
                    if i == 1:
                        measurevaluelist = str(globals()[f"df_queryresult_{i}"].iloc[0,0])+" | "+str(globals()[f"df_queryresult_{i+1}"].iloc[0,0])
                        perofdifflist = str(pervariance)
                    else:
                        measurevaluelist = measurevaluelist+" | "+str(globals()[f"df_queryresult_{i+1}"].iloc[0,0])
                        perofdifflist = perofdifflist + " | " + str(pervariance)
                if status == 0:
                    check_result = "Pass"
                else:
                    check_result = "Fail"
                now = datetime.datetime.now()
                end_time = now.strftime("%Y-%m-%d %H:%M:%S")
                df_comparedoutput = df_comparedoutput.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'ROW_ID' : rowid, 'STATUS' : check_result, 'DATA_GRANULARITY': 'NOT APPLICABLE', 'DIMENSION_VALUES' : df_queryresult_1.columns[0], 'MEASURE': 'NOT APPLICABLE' ,'MEASURE_VALUE' : measurevaluelist,'PERCENTAGE_DIFF' : perofdifflist,'LATEST_RUN_FLAG':'Y','START_TIME' : start_time,'END_TIME' : end_time}, 
                    ignore_index = True)

            #Calculating Failure metrics
            rowcount = df_comparedoutput[df_comparedoutput["TEST_ID"] == testid].ROW_ID.nunique()
            fail_rowcount = df_comparedoutput[(df_comparedoutput["STATUS"] == 'Fail') & (df_comparedoutput["TEST_ID"] == testid)].ROW_ID.nunique()
            pass_rowcount = int(rowcount) - int(fail_rowcount)
            if rowcount == 0:
                rowcount = 1
                print("!!! Error: SQL Query and Granularity Values doesn't match for Test_id :",testid)
            pass_percentage = round((pass_rowcount/rowcount) * 100,0)
            now = datetime.datetime.now()
            end_time = now.strftime("%Y-%m-%d %H:%M:%S")
            if df_comparedoutput[(df_comparedoutput["STATUS"] == 'Fail') & (df_comparedoutput["TEST_ID"] == testid)].ROW_ID.nunique() == 0:
                status = "Pass"
            else:
                status = "Fail"
            
            #Writing Comparison summary
            df_summaryoutput = df_summaryoutput.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid,'SQUAD':squad,'PACKAGE_NAME':packagename,'TEST_NAME':testname, 'STATUS' : status, 'TOTAL_RECORD_COUNT': rowcount, 'PASSED_RECORD_COUNT' : pass_rowcount, 'FAILED_RECORD_COUNT': fail_rowcount ,'PASS_PERCENTAGE' : pass_percentage,'LATEST_RUN_FLAG':'Y','START_TIME' : start_time,'END_TIME' : end_time},ignore_index = True)
            
            #Calculating Failure row counts
            nullrecordcount = df_comparedoutput[(df_comparedoutput["STATUS"] == 'Fail') & (df_comparedoutput["TEST_ID"] == testid) & (df_comparedoutput["DIMENSION_VALUES"].str.contains("NULLRECORD_TestQuery"))].ROW_ID.nunique()
            failrecordcount_meas = fail_rowcount - nullrecordcount
            if nullrecordcount > 0:
                df_failuresummary = df_failuresummary.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'TOTAL_FAILED_RECORD_COUNT' : fail_rowcount, 'FAILED_RECORD_COUNT' : nullrecordcount, 'FAILURE_CATEGORY': "Dimension value combination is missing", 'LATEST_RUN_FLAG' : 'Y'},ignore_index = True)
            #Writing to Failure summary dataframe
            df_failuresummary = df_failuresummary.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'TOTAL_FAILED_RECORD_COUNT' : fail_rowcount, 'FAILED_RECORD_COUNT' : failrecordcount_meas, 'FAILURE_CATEGORY': "Measure mismatch between source and target", 'LATEST_RUN_FLAG' : 'Y'},ignore_index = True)

        else:
            processingtestId = testid
        

    df_failuresummary = df_failuresummary[df_failuresummary["FAILED_RECORD_COUNT"] != 0]

    if(output_option != 2):
        #Writing results to Excel file
        now = datetime.datetime.now()
        print("Writing result to Excel file...")
        file_time = now.strftime("%Y%m%d%H%M")
        Filename = "OutputFiles\\"+"Query_Reconciliation_SessionID_"+str(sessionId)+"_DT_"+file_time+".xlsx"
        excelwriter = pd.ExcelWriter(Filename, engine = 'xlsxwriter')

        df_summaryoutput.to_excel (excelwriter, sheet_name='Result_Summary', index = False, header=True)
        df_failuresummary.to_excel (excelwriter, sheet_name='Faliure_Summary', index = False, header=True)
        df_comparedoutput.to_excel (excelwriter, sheet_name='Result_Detail', index = False, header=True)
        df_connectiondetails.to_excel (excelwriter, sheet_name='Test_Query_Detail', index = False, header=True)

        excelwriter.save()
        print("Results writen to ",Filename)
        excelwriter.close()

    if(output_option != 1):
        #Writing results to Snowflake Table
        print("Loading result to Snowflake table...")
        #Loading Input file tables

        try:
            sf_result_cur.execute("TRUNCATE TABLE SNOWSCAPE_MASTER_FILE_ACCELERATOR")
        except exception as msg:
                print("Error in Truncating SNOWSCAPE_MASTER_FILE_ACCELERATOR Table: ", msg)
        m_success, m_nchunks, m_nrows, m_output = write_pandas(sf_result_conn, df_masterfile, 'SNOWSCAPE_MASTER_FILE_ACCELERATOR')
        if m_success == False:
            print("Failed to Load SNOWSCAPE_MASTER_FILE_ACCELERATOR Table. ")

        try:
            sf_result_cur.execute("TRUNCATE TABLE SNOWSCAPE_DETAILED_CONNECTION_FILE")
        except exception as msg:
                print("Error in Truncating SNOWSCAPE_DETAILED_CONNECTION_FILE table : ", msg)
        dc_success, dc_nchunks, dc_nrows, dc_output = write_pandas(sf_result_conn, df_detailedconnectionfile, 'SNOWSCAPE_DETAILED_CONNECTION_FILE')
        if dc_success == False:
            print("Failed to Load SNOWSCAPE_DETAILED_CONNECTION_FILE Table. ")
        
        #Updating LATEST_RUN_FLAG for processed TestIDs
        update_testid_str = ','.join([str(elem) for elem in update_testid_li])
        update_script_summary = "UPDATE SNOWSCAPE_SUMMARY_RESULT SET LATEST_RUN_FLAG = 'N' WHERE TEST_ID in ("+update_testid_str+")"
        update_script_detail = "UPDATE SNOWSCAPE_DETAILED_RESULT SET LATEST_RUN_FLAG = 'N' WHERE TEST_ID in ("+update_testid_str+")"
        
        try:
            sf_result_cur.execute(update_script_summary)
        except exception as msg:
                print("Error in Updating LATEST_RUN_FLAG for table SNOWSCAPE_SUMMARY_RESULT: ", msg)
        try:
            sf_result_cur.execute(update_script_detail)
        except exception as msg:
                print("Error in Updating LATEST_RUN_FLAG for table SNOWSCAPE_DETAILED_RESULT: ", msg)

        #Writing results to tables
        s_success, s_nchunks, s_nrows, s_output = write_pandas(sf_result_conn, df_summaryoutput, 'SNOWSCAPE_SUMMARY_RESULT')
        d_success, d_nchunks, d_nrows, d_output = write_pandas(sf_result_conn, df_comparedoutput, 'SNOWSCAPE_DETAILED_RESULT')       
        qc_success, qc_nchunks, qc_nrows, qc_output = write_pandas(sf_result_conn, df_connectiondetails, 'SNOWSCAPE_TEST_QUERY_DETAIL')  

        if d_success and s_success and qc_success:
            print("Successfully loaded {} row(s) to Snowflake table SNOWSCAPE_DETAILED_RESULT".format(d_nrows))
            print("Successfully loaded {} row(s) to Snowflake table SNOWSCAPE_SUMMARY_RESULT".format(s_nrows))
        else:
            print("Failed to load data to Snowflake tables !!!")

    #Printing results to console
    print("     *****************************************************        ")
    print(df_summaryoutput)
    print("     *****************************************************        ")

else:
    #Merging Master and Details input files based on user given TestIDs
    df_querysource = df_selectedmaster.merge(df_selecteddetailedconnectionfile, left_on='TEST_ID', right_on='TEST_ID', how='inner')
    df_querysource.sort_values("TEST_ID", axis = 0, ascending = True,
                    inplace = True, na_position ='last') 

    print("Processing SQL Queries...")
    processingtestId = -1
    querycount = 1
    now = datetime.datetime.now()
    file_time = now.strftime("%Y%m%d%H%M")
    Filename = "OutputFiles\\"+"Query_Reconciliation_SessionID_"+str(sessionId)+"_DT_"+file_time+".xlsx"
    excelwriter = pd.ExcelWriter(Filename, engine = 'xlsxwriter')
    for index, row in df_querysource.iterrows():
        #Reading Query file
        testid = row["TEST_ID"]
        dbenv = row["DATABASE"]
        fd = open('Input SQL Queries\\' + row["QUERY_FILENAME"], 'r')
        sqlFile = fd.read()
        fd.close()
        try:
            if dbenv == 'Snowflake':
                df_queryresult = pd.read_sql_query(sqlFile,sf_conn_dict[row["CONNECTION"]])
                print("Snowflake query exeecution finished for testid:",testid)
            elif dbenv == 'SQL Server':
                df_queryresult = pd.read_sql_query(sqlFile,sqlserver_conn_dict[row["CONNECTION"]])
                print("SQL Server query exeecution finished for testid:",testid)
            elif dbenv == 'Athena':
                queryresult = AthenaQueryExecutor(query=sqlFile)
                df_queryresult = queryresult.run_query()
                print("Athena query exeecution finished for testid:",testid)
            df_queryresult.fillna("NULLVALUE",inplace = True)
        except exception as msg:
            print("Input Query Execution failed.: ", msg)
            #Writing results to Excel file
        if processingtestId == row["TEST_ID"]:
            querycount += 1
        else:
            querycount = 1
        sheetname = "TestID_"+str(row["TEST_ID"]) + "_Query_"+str(querycount)
        df_queryresult.to_excel (excelwriter, sheet_name=sheetname, index = False, header=True)
        processingtestId = row["TEST_ID"]
    excelwriter.save()
    print("Results writen to ",Filename)
    excelwriter.close()
    
#Closing connections    
for i in range(1,int(Config.snowflake_db_count)+1):    
    globals()[f"sf_conn{i}"].close()

end_time = time.time()
print("Execution Time: ",time.strftime("%H:%M:%S", time.gmtime(end_time - strt_time)))