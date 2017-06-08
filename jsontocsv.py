#def jsontocsv():
import json,csv

arr='dataq'
with open(arr+'.json','r') as data_file:
    x=json.load(data_file)
with open("csv"+arr+".csv","wb") as csv_file:
    f = csv.writer(csv_file)
    for x in x:
                   f.writerow([x["model"],
                               x["pk"],
                               x["fields"]["question_text"],
                               x["fields"]["pub_date"]
                               ]
                              )
