import django,os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Data_Loader.settings")
django.setup()
import json,re
from django.apps import apps





with open('C:/Users/ashishod/Desktop/Data Loader/DataLoader/Data_Loader/Data_Loader/app_name_class_name.json') as data_file:
    data = json.load(data_file)
i=0
with open('class_name.json','w') as outfile:
    for i,elem in enumerate(data):
        if not re.match('django',data[i]):
            myapp1 = apps.get_app_config(data[i])
            app=myapp1.verbose_name
            print myapp1
myapp = apps.get_models(app.lower())
print myapp
# json.dump(myapp,outfile)
        
