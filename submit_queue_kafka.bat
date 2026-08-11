@echo OFF

call submit_aff.bat %*
echo _______________________________________________________________________
echo Beginning of script execution...
date /T
time /T
echo _______________________________________________________________________

rem TEST mode
if "%TOM_JOB_EXEC%" == "TEST" (
	echo Job executed in TEST mode
	%ABM_BIN%\tsend -sT -r0 -m"Job completed (TEST mode)"
	%ABM_BIN%\vtgestlog
	goto FIN
)

py %ABM_BIN%\kafkaJob.py "%1" "%2" "%3" "%4" "%5" "%6"

set RETCODE=%ERRORLEVEL%
if %RETCODE% equ 0 goto TERMINE
goto ERREUR

:ERREUR
%ABM_BIN%\tsend -sE -r%RETCODE% -m"Job in error (%RETCODE%)"
%ABM_BIN%\vtgestlog
echo _______________________________________________________________________
echo End of script execution
date /T
time /T
echo Exit [%RETCODE%] so no acknowledgment
echo _______________________________________________________________________
if not "%TOM_LOG_ACTION%"=="   " call Gestlog_wnt.bat
exit %RETCODE%

:TERMINE
%ABM_BIN%\tsend -sT -r%RETCODE% -m"Job completed (%RETCODE%)"
%ABM_BIN%\vtgestlog
echo _______________________________________________________________________
echo End of script execution
date /T
time /T
echo Exit [%RETCODE%] so acknowledgment
if not "%TOM_LOG_ACTION%"=="   " call Gestlog_wnt.bat
exit %RETCODE%

:FIN
