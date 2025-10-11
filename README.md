# Warehouse-process-improvement-workflow
This automated python workflow extracts SAP data to MySQL, transforms data using Pandas library and loads it into Snowflake. Power BI is in a live connection with Snowflake and power bi reports are emailed daily using Outlook.

SITUATION:
The Company is undergoing a major systematic change in tea blending processes and so, there were operational errors in scanning correct material code to correct hopper. These errors could be found out from SAP Warehouse task data but it takes approximately 4 hours to create report, identify errors and retrain the personnel. Also, the report created was not easily readable and it will be difficult for stakeholders to understand the errors/wrong material usage instances.

TASK:
There is a need to create workflow that pulls SAP data and emails Power BI report. This report should allow stakeholders to quickly understand the number of errors and the operator responsible for the wrong material usage. Also, microscopic details should be present in the report for production coordinators to easily investigate the error further.
