#!/bin/bash
set -e
 
echo "==> Running migration: v0-cursos-table.py"
python bd/migration/v0-cursos-table.py
 
#echo "==> Running loaders: df-to-db.py"
#python bd/scripts/loaders/df-to-db.py
 
#echo "==> Running updates"
#python bd/scripts/update/update-participantes.py
#python bd/scripts/update/update-encuesta-inicial.py
#python bd/scripts/update/update-eventos.py
 
#echo "==> Running duplicate handling"
#python -m bd.scripts.manejo_duplicados
 
#echo "==> Running seeds"
# Add any seed scripts from bd/seeds/ here
# python bd/seeds/your-seed.py
 
#echo "✅ Database population complete."
