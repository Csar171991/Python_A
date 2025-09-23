@echo off
chcp 65001 >nul
cls
echo ====================================

echo    Selecciona una opción:

echo ====================================

echo 1. Ejecutar instrumento con campo sección 
echo 2. Ejecutar instrumento sin campo sección

echo ====================================

set /p choice="Elige 1 o 2: "

if "%choice%"=="1" (
    echo Ejecutando ...
    cd/d "C:\Users\procesosumc05\Documents\DEPURACION\proyecto_depuracion\">nul 2>&1
python app_bd2.py
) else if "%choice%"=="2" (
    echo Ejecutando ...
    cd/d "C:\Users\procesosumc05\Documents\DEPURACION\proyecto_depuracion\">nul 2>&1
python app_bd.py
) else (
    echo Opción no válida. Saliendo...
    exit /b
)

pause

