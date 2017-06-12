import mysql.connector
import MySQLdb
import json,csv

json_name={}
csv_name='abc.csv'#Random initialisation
def jsontocsv(a):
    print("What is the name of the JSON file to be converted to csv without the extension")

    json_name=raw_input()


    with open(json_name+".json",'r') as data_file:

        x=json.load(data_file)

    with open("csv"+json_name+".csv","wb") as csv_file:

        f = csv.writer(csv_file)
        print "Enter the number of columns in table",a
        t=input()
        counter = t
        i=1
        j=1
        table={}
        while t:
            print "Enter field name of",i,"column in table",a
            table[i]=raw_input()
            i=i+1
            t=t-1
        columns='[x["pk"],'
        while counter:
            counter = counter - 1
            columns = columns +'x["fields"]["'+table[j]+'"],'
        #print columns+']'
        columns=str(columns +']')    
        for x in x:
            print columns
            f.writerow([x["pk"],x["fields"]["question_text"],])
    csv_name = "csv"+json_name
    print "The csv file is created as",csv_name
    return csv_name


#sql cursor initialisation
cnx = mysql.connector.connect(user = 'gladipero',password ='gladipero',host='localhost',database = 'mydatabase')
cursor = cnx.cursor(buffered=True)



print "Welcome To Data Loader App /n"
print "Is the database schema loaded in the MySql database?"
print "Press 1 for yes"
print "press 2 for No"
n=input()
if n==2:
    print "Enter the number of tables"
    t=input()
    while t:
        print "Which format is the data in table ",t,"? \nPress 1 for JSON \nPress 2 for CSV"
        n=input()
        if n==1:
               csv_name=jsontocsv(t)
        else:
            print "Enter the name of csv file for table without extension",t
            csv_name=raw_input()



        print csv_name
        with open(csv_name+".csv","rb") as csvfile:
            
            csv_data = csv.reader(csvfile)
            for row in csv_data:
                cursor.execute('insert into polls_question(id,question_text) VALUES\
                (%s,%s)',row)
        t=t-1

cnx.commit()
cursor.close()
cnx.close()         
