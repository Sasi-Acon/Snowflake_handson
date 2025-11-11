@echo OFF
 
REM --- 1. SET YOUR SECRET CREDENTIALS ---
set "SNOWFLAKE_USER=YOUR_USERNAME"
set "SNOWFLAKE_PASSWORD=YOUR_PASSWORD"
set "SNOWFLAKE_ACCOUNT=YOUR_ACCOUNT_IDENTIFIER"
 
REM --- 2. ACTIVATE/RUN YOUR SCRIPT ---
REM We no longer need the 'call activate.bat' line.
REM Instead, we will use the FULL, DIRECT path to the python.exe
REM inside your 'snowflake_etl' environment.
 
REM 'cd %~dp0' changes to the script's own directory
cd %~dp0
 
C:\Users\SasidaranVelliangiri\miniconda3\envs\snowflake_etl\python.exe load_to_snowflake.py
 
REM --- 4. (OPTIONAL) PAUSE TO SEE ERRORS ---
pause