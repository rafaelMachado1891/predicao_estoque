# test_env.py
from dotenv import load_dotenv
import os
from pathlib import Path

print("Caminho:", Path(".").resolve())

load_dotenv(".env")

print("DB_POSTGRES:", os.getenv("DB_POSTGRES"))
print("USER_POSTGRES:", os.getenv("USER_POSTGRES"))
print("PASSWORD_POSTGRES:", os.getenv("PASSWORD_POSTGRES"))
print("HOST_POSTGRES:", os.getenv("HOST_POSTGRES"))
print("PORT_POSTGRES:", os.getenv("PORT_POSTGRES"))
