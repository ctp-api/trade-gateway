@echo off
title Run Service
:: Activate Python
call .venv\Scripts\activate
echo Start Service......
set current_path=%~dp0
echo Current path: %current_path%
python -m pyctp.main
echo Finished.
exit