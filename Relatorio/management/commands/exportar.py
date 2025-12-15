import os
import django
from django.core.management import call_command

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Ginsights.settings')
django.setup()

with open('dados.json', 'w', encoding='utf-8') as f:
    try:
        call_command(
            'dumpdata', 
            '--natural-foreign', 
            '--natural-primary', 
            exclude=['contenttypes', 'auth.permission', 'admin.logentry', 'sessions.session'], 
            indent=4, 
            stdout=f
        )
        print("✅ Sucesso! O arquivo dados.json foi gerado corretamente.")
    except Exception as e:
        print(f"❌ Erro ao exportar: {e}")