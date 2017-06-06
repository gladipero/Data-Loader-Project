import django,os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Data_Loader.settings")
django.setup()

from django.apps import apps
from django.contrib import admin
from django.contrib.admin.sites import AlreadyRegistered
import json
with open('apps.json','r') as json_data:
    d=json.load(json_data)
    print(d)
#from django.core import management
#management.call_command('dumpdata',d)
from django.apps import apps
myapp = apps.get_app_config('polls')

print myapp.models

