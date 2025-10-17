# Warehouse-process-improvement-workflow
This automated python workflow extracts SAP data to MySQL, transforms data using Pandas library and loads it into Snowflake. Power BI is in a live connection with Snowflake and power bi reports are emailed daily using Outlook.

SITUATION:
The Company is undergoing a major systematic change in tea blending processes and so, there were operational errors in scanning correct material code to correct hopper. These errors could be found out from SAP Warehouse task data but it takes approximately 4 hours to create report, identify errors and retrain the personnel. Also, the report created was not easily readable and it will be difficult for stakeholders to understand the errors/wrong material usage instances.

TASK:
There is a need to create workflow that pulls SAP data and emails Power BI report. This report should allow stakeholders to quickly understand the number of errors and the operator responsible for the wrong material usage. Also, microscopic details should be present in the report for production coordinators to easily investigate the error further.

PRE-REQUISITES:
The SAP HANA databse instance, subscription, schema, table details are obtained from IT

ACTION:
1. There are two sources of data from SAP HANA. One is Warehouse task data which details the warehouse task information for the regular tea hoppers and other is silo bin table which describes the hopper code and its corresponding material code. The idea is to manipulate these two information to find out wrong usage of materials to hoppers and in turn find out the operator responsible for the issue

2. The two data tables are extracted from SAP HANA using Python's hdb client.

SAP HANA DATABASE EXPLORER:

<img width="882" height="599" alt="image" src="https://github.com/user-attachments/assets/14d48302-407e-40dc-88ef-028f5ff4162c" />

SAP HANA TABLES:

<img width="1860" height="834" alt="image" src="https://github.com/user-attachments/assets/49917e6a-da6f-4179-b3dc-5945f2f93148" />


CODE TO EXTRACT DATA:

<img width="753" height="371" alt="image" src="https://github.com/user-attachments/assets/93e6a1a7-7b1b-4a5b-a3b1-ab16ffce6fe2" />


3. The pandas library in Python is used to manipulate the two sources of information to obtain the final table

CODE TO TRANSFORM:

<img width="562" height="519" alt="image" src="https://github.com/user-attachments/assets/db2d9150-8806-4777-868e-2eb3876b1ddc" />

4. The final table is pushed to Snowflake using snowflake.connector library in python

CODE TO LOAD:

<img width="472" height="514" alt="image" src="https://github.com/user-attachments/assets/5965d70e-0082-4f11-b550-849b476dbbe3" />

5. Power BI is connected with Snowflake and report is created to ensure information is presented clearly to stakeholders. This report is published to Power BI service

Power BI report:

<img width="1565" height="778" alt="image" src="https://github.com/user-attachments/assets/b1566f64-4884-49fb-908c-f6d6cfff29bd" />

6. Email containing Power BI report link is sent to stakeholders through Outlook using Azure Active Directory.

CODE TO EMAIL:

<img width="812" height="532" alt="image" src="https://github.com/user-attachments/assets/343f7468-2455-488c-bff0-fdac4d2dcb76" />

RESULT:

The email is sent to stakeholders with the information about the wrong usages. This caused a significant reduction in reporting time and improved visibility into mistakes as soon as they occur, allowing the personnel to be retrained promptly.

<img width="708" height="358" alt="image" src="https://github.com/user-attachments/assets/aa1c3b19-3de8-40fd-a72b-b824fe9925c2" />




