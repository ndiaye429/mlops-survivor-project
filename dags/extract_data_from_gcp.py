from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlalchemy
import os
from google.cloud import storage
import logging

# Configuration GCS
BUCKET_NAME = "my_bucket-2024"
FILE_NAME = "Titanic-Dataset.csv"
PROJECT_ID = "mlops-project-491208"
LOCAL_FILE = "/tmp/titanic_data.csv"

# Configuration PostgreSQL - À MODIFIER SELON CE QUE VOUS AVEZ TROUVÉ
PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "postgres"  # ← MODIFIER ICI
PG_PASSWORD = "postgres"  # ← MODIFIER ICI
PG_DATABASE = "postgres"  # ← MODIFIER ICI

def download_from_gcs():
    """Télécharge depuis GCS"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    blob.download_to_filename(LOCAL_FILE)
    print(f"✅ Fichier téléchargé: {LOCAL_FILE}")
    print(f"📊 Taille: {os.path.getsize(LOCAL_FILE)} bytes")

def test_postgres_connection():
    """Test la connexion PostgreSQL"""
    import psycopg2
    print(f"🔍 Test connexion: {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE,
            connect_timeout=5
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"✅ Connexion réussie! PostgreSQL: {version[:50]}...")
        
        # Vérifier si la table existe déjà
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'titanic'
            )
        """)
        exists = cursor.fetchone()[0]
        print(f"📋 Table titanic existe: {exists}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Échec connexion: {e}")
        raise

def transform_and_load():
    """Transforme et charge dans PostgreSQL"""
    print("="*50)
    print("CHARGEMENT DANS POSTGRESQL")
    print("="*50)
    
    # Lecture
    df = pd.read_csv(LOCAL_FILE)
    print(f"✅ {len(df)} lignes chargées")
    
    # Nettoyage
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
    
    # Connexion
    db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    print(f"🔌 Connexion à: postgresql://{PG_USER}:***@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    
    engine = sqlalchemy.create_engine(db_url)
    
    # Tester la connexion
    with engine.connect() as test_conn:
        test_conn.execute(sqlalchemy.text("SELECT 1"))
    print("✅ Connexion établie")
    
    # Chargement
    df.to_sql(
        name='titanic',
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=500
    )
    print(f"✅ Données chargées dans la table 'titanic'")
    
    # Vérification
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM titanic")).scalar()
        print(f"✅ Vérification: {result} lignes")
        
        # Aperçu
        sample = conn.execute(sqlalchemy.text("""
            SELECT passengerid, survived, pclass, name, sex 
            FROM titanic LIMIT 5
        """))
        print("\n📊 Aperçu:")
        for row in sample:
            print(f"   ID:{row[0]}, Survécu:{row[1]}, Classe:{row[2]}, Nom:{row[3][:20]}...")
    
    engine.dispose()
    print("\n✅ CHARGEMENT RÉUSSI!")

with DAG(
    dag_id="extract_titanic_postgres",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Pipeline GCS → PostgreSQL"
) as dag:
    
    test_db = PythonOperator(
        task_id="test_connection",
        python_callable=test_postgres_connection,
    )
    
    download = PythonOperator(
        task_id="download_from_gcs",
        python_callable=download_from_gcs,
    )
    
    load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )
    
    test_db >> download >> load