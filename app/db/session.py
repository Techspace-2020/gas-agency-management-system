from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
import urllib.parse

DB_PASSWORD = urllib.parse.quote_plus(DB_PASSWORD)
DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()