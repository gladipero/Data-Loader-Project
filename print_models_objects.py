import os
from django.core.wsgi import get_wsgi_application

os.environ['DJANGO_SETTINGS_MODULE']='Data_Loader.settings'
application=get_wsgi_application()

from polls.models import Question

questions=Question.objects.all()
print questions

