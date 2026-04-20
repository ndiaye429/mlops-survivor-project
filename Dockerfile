FROM astrocrpublic.azurecr.io/runtime:3.2-2

RUN pip install apache-airflow-providers-google
RUN pip install apache-airflow-providers-postgres

# Créer le dossier .config/gcloud
RUN mkdir -p /home/astro/.config/gcloud

# Copier les credentials depuis votre projet
COPY credentials/application_default_credentials.json /home/astro/.config/gcloud/

# Définir la variable d'environnement
ENV GOOGLE_APPLICATION_CREDENTIALS=/home/astro/.config/gcloud/application_default_credentials.json

# AJOUTER LE PROJECT ID GCP
ENV GOOGLE_CLOUD_PROJECT=mlops-project-49120*

# Fixer les permissions
USER root
RUN chown -R astro:astro /home/astro/.config
USER astro
