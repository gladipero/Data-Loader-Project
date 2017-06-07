import settings
data = settings.INSTALLED_APPS
import json

with open('app_name_class_name.json','w') as outfile:
    json.dump(data,outfile)
