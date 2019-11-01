# Airflow_Code
Airflow Reference and Sample Code
1. DAG Template that allows Airflow to check the previous multiple dag runs (self references) and if any in failed state then fails the current task
(All previous DAG runs must be set to success for the current DAG run to proceed). This task should be the first in any DAG that is created to enable this functionality 
