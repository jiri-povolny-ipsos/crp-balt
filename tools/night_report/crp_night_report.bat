@echo off
rem ---------------------------------------------------------------------------
rem Prehled o nocnim behu CRP BALT do Teams
rem
rem Vytvoril: Jiri Povolny
rem Vytvoreno dne: 22.9.2026
rem
rem Urceno pro Task Scheduler. Nocni beh auto_rel_pub_inc() jede v 01:45,
rem doporuceny cas spusteni teto ulohy je 04:00.
rem
rem Registrace ulohy (spustit v prikazovem radku jako administrator):
rem   schtasks /create /tn "CRP BALT nocni prehled" /tr "d:\Python\crp_balt\tools\night_report\crp_night_report.bat" /sc daily /st 04:00
rem
rem Skript si sam zapisuje do logs\crp_night_report.log. Presmerovani nize
rem zachyti i pripady, kdy vubec nenastartuje Python (chybejici modul apod.).
rem
rem Navratove kody: 0 = OK, 1 = varovani, 2 = chyba behu nebo kontroly
rem ---------------------------------------------------------------------------

cd /d "%~dp0"
if not exist logs mkdir logs
python crp_night_report.py >> logs\crp_night_report_task.log 2>&1
exit /b %ERRORLEVEL%
