@echo off
echo Testing SAP Auto Runner
echo.

REM Create test output directory
if not exist "C:\sap\outputs" mkdir "C:\sap\outputs"

REM Create a test file with timestamp
set timestamp=%date:~6,4%%date:~3,2%%date:~0,2%%time:~0,2%%time:~3,2%%time:~6,2%
set timestamp=%timestamp: =0%
echo Creating test file: %timestamp%_y_149-ALL.txt

REM Create test TSV content
echo In-Transfer (Push Delivery) Materials Report > "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo Acme Manufacturing Corp >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo. >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo User                                   TESTUSER >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo Run Date   :                           2025-01-15 >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo Run Time   :                           14:30:22 >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo. >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo         Plant	Delivery	Material >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo         PLT01	9876543210	55512345 >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"
echo         PLT02	9876543211	55512346 >> "C:\sap\outputs\%timestamp%_y_149-ALL.txt"

echo Test file created successfully!
echo.
echo You can now run: target\release\sap_auto_runner.exe --config config.toml --verbose
echo.
pause
