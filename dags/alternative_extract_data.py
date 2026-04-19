from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage
import os

# CONFIGURATION
BUCKET_NAME = "my_bucket-2024"
FILE_NAME = "Titanic-Dataset.csv"
PROJECT_ID = "mlops-project-491208"  # ← REMPLACEZ ICI AVEC VOTRE PROJECT_ID
LOCAL_FILE = "/tmp/titanic_test.csv"

def test_gcs_access():
    """Test simple d'accès à GCS"""
    print("="*50)
    print("TEST ACCÈS GCS")
    print("="*50)
    
    print(f"1. Bucket: {BUCKET_NAME}")
    print(f"2. Fichier: {FILE_NAME}")
    print(f"3. Project ID: {PROJECT_ID}")
    
    # Créer le client
    client = storage.Client(project=PROJECT_ID)
    print("✅ Client GCS créé")
    
    # Accéder au bucket
    bucket = client.bucket(BUCKET_NAME)
    print(f"✅ Bucket accédé: {bucket.name}")
    
    # Lister tous les fichiers
    print("\n📋 Liste des fichiers dans le bucket:")
    blobs = list(bucket.list_blobs(max_results=10))
    if blobs:
        for blob in blobs:
            print(f"   - {blob.name} ({blob.size} bytes)")
    else:
        print("   ⚠️ Aucun fichier trouvé")
    
    # Vérifier notre fichier spécifique
    blob = bucket.blob(FILE_NAME)
    if blob.exists():
        print(f"\n✅ Fichier '{FILE_NAME}' trouvé!")
        print(f"   Taille: {blob.size} bytes")
        
        # Télécharger
        blob.download_to_filename(LOCAL_FILE)
        print(f"✅ Téléchargé vers {LOCAL_FILE}")
        
        # Vérifier
        file_size = os.path.getsize(LOCAL_FILE)
        print(f"✅ Fichier local: {file_size} bytes")
    else:
        print(f"\n❌ Fichier '{FILE_NAME}' non trouvé")
        raise Exception("Fichier non trouvé dans GCS")
    
    print("\n" + "="*50)
    print("✅ TEST RÉUSSI!")
    print("="*50)

# DAG
with DAG(
    dag_id="test_gcs_only",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    test = PythonOperator(
        task_id="test_gcs_access",
        python_callable=test_gcs_access,
    )