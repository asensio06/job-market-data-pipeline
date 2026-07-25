# Utiliser une image Python 3.12 légère
FROM python:3.12-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Empêcher Python d'écrire des fichiers .pyc et forcer l'affichage direct des logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copier les fichiers de dépendances
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source de l'application
COPY . .

# S'assurer que le dossier data existe
RUN mkdir -p data

# Commande par défaut pour exécuter le pipeline Medallion complet
CMD ["python", "run_pipeline.py"]
