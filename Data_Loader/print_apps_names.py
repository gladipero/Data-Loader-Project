import settings
#print(settings.INSTALLED_APPS)
data = settings.INSTALLED_APPS
import json

with open('apps.json','w') as outfile:
    json.dump(data,outfile)
