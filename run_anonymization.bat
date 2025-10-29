@echo off
chcp 65001 >nul
setlocal

REM === Определяем путь к окружению ===
set VENV_DIR=.venv
set ACTIVATE=.\%VENV_DIR%\Scripts\activate

REM === Проверяем, существует ли папка окружения ===
if not exist "%VENV_DIR%" (
    echo [INFO] Виртуальное окружение не найдено. Создаю...
    python -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo [ERROR] Не удалось создать виртуальное окружение.
        pause
        exit /b 1
    )

    echo [INFO] Устанавливаю зависимости из requirements.txt...
    call "%ACTIVATE%"
    python -m pip install --upgrade pip
    pip install -r requirements.txt
) else (
    echo [INFO] Окружение уже существует.
    echo [INFO] Проверка зависимостей...
    call %ACTIVATE%
    python -m pip install --upgrade pip
    pip install -r requirements.txt
)

REM === Активируем окружение ===
call "%ACTIVATE%"

REM === Запускаем скрипт ===
echo [INFO] Запуск src\anonimization.py...
python src\anonimization.py

REM === Завершаем работу ===
deactivate
pause
