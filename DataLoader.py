import mysql.connector
import MySQLdb
import sqlite3
import json,csv
import os,getpass
import string
import random

class ConnectMYSQL():

    '''Class containing tools for connecting to the MySQL database '''

	#Method to take user input for database credentials and establishing connections
    def connect_mysql(self):
        while True:
            os.system('cls')
            username = raw_input("Please enter username:")
            password = getpass.getpass()
            hostn = raw_input("Enter the host name: ")
            db_name = raw_input("Enter the database name:")
            port = raw_input("Enter the port: ")
            try:
                ConnectMYSQL.connection = mysql.connector.connect(user = username , password = password, \
                                                         host = hostn , database = db_name,port = port)
                ConnectMYSQL.db_name = db_name
                os.system('cls')
                break
            except:
                os.system('cls')
                a=raw_input("Wrong credentials were entered. Press Enter to continue")
        ConnectMYSQL.cursor = self.connection.cursor(buffered = True)
        ConnectMYSQL.execute = self.cursor.execute

    #Method to run SQL query to get it ready for data insertion
    def initalise_database_for_randomDataLoad(self,execute):
        execute('SET FOREIGN_KEY_CHECKS = 0')
        execute("SET sql_mode = ''")
        execute ("SET @g = ST_GeomFromText('point(1 1)')")


    #Method to commit and close the database connected
    def close_cursor(self,connection,cursor):
        connection.commit()
        ConnectMYSQL.execute("SET FOREIGN_KEY_CHECKS =1;")
        cursor.close()
        connection.close()
        print "Connection closed. Thank You for using Random Data Loader app"



class UtilityFunctions:
    '''Class Containing utility tools used by various functions for string input formatting etc'''

	#Method to return table name,refer table name , refer column name from input schema string
    def process_info(self,each):
        end_info = each[3:].find("'")
        table_name = each[3:end_info+3]
        each = each[end_info+5:]
        k = 3
        j = 0
        lists = ['column_name','refer_table_name','refer_column_name']
        while k:
            start_list = each.find("'")
            each = each[start_list+1:]
            end_info = each.find("'")
            lists[j]=each[0:end_info]
            each = each[end_info+4:]
            k = k-1
            j = j+1
        return table_name,lists[0],lists[1],lists[2]

    #Method to edit and change the database schema to a readable form for sql queries
    def process_table_name(self,table):
        table = str(table)
        end = table.find("',)")
        return table[3:end]

    #Method to get the column name from input string
    def get_id_name(self,table_schema):
        id_end = table_schema[3:].find("'")
        id_name = table_schema[3:3+id_end]
        return id_name,id_end


    #Method to check if input column has an unsigned integer data type
    def check_if_column_unsigned(self,table_schema):
        if table_schema.find("unsigned") != -1:
            return 1
        else:
            return 0

    #Method to get the random values for the varchar schema
    def get_varchar_random_value(self,table_schema):
        start_info = table_schema.find("(")
        table_schema = table_schema[start_info+1:]
        end_info = table_schema.find(")")
        varchar_info = int(table_schema[:end_info-1])
        random_value = ''
        while varchar_info:
            random_value = str(random_value)+random.choice(string.ascii_lowercase)
            varchar_info = varchar_info - 1
        return random_value


    #Method to get random value for the set schema
    def get_set_value(self,table_schema):
        start_list = table_schema.find("set(") + 4
        end_list = table_schema[start_list:].find("')")
        table_schema = table_schema[10:11+end_list]
        enum_list = []
        while table_schema.find(",") != -1:
            item_end = table_schema.find("'")
            item = table_schema[:item_end]
            enum_list.append(item)
            table_schema = table_schema[item_end+3:]
        item_end = table_schema.find("'")
        item = table_schema[:item_end]
        enum_list.append(item)
        return random.choice(enum_list)


    #Method to get the random_value from the enum schema
    def get_enum_values(self,table_schema):
        start_list = table_schema.find("enum(") + 5
        end_list = table_schema[start_list:].find("')")
        table_schema = table_schema[10:11+end_list]
        enum_list = []
        while table_schema.find(",") != -1:
            item_end = table_schema.find("'")
            item = table_schema[:item_end]
            enum_list.append(item)
            table_schema = table_schema[item_end+3:]
        item_end = table_schema.find("'")
        item = table_schema[:item_end]
        enum_list.append(item)
        return random.choice(enum_list)


    #Method to get the M and D value for the float column datatype
    def get_MD_info(self,table_schema,start):
        table_schema = table_schema[start+6:]
        end1 = table_schema.find(",")
        M = int(table_schema[:end1])
        table_schema = table_schema[end1:]
        end2 = table_schema.find(")")
        D = int(table_schema[1:end2])
        return M,D


    #Method to create a random float number for input M and D values
    def get_float_random_value(self,M,D):
        power1 = M-D
        random_value = random.randrange(0,10**power1)
        float_number =  random.random()
        helper = "{0:."+str(D)+"f}"
        float_number = helper.format(float_number)
        random_value = float(random_value) + float(float_number)
        return random_value


class ForeignKeyHandling:
    '''Class containing objects dealing with the Foreign Key constraints of the MySQL database'''

    class ForeignKeyParameter(object):
    	'''Class containing information of Foreign Key present in database'''
        def __init__(self,table_name,column_name,refer_table_name,refer_column_name):
            self.table_name = table_name
            self.column_name = column_name
            self.refer_table_name = refer_table_name
            self.refer_column_name = refer_column_name
            self.fk_flag = 1
        table_name = ''
        column_name = ''
        refer_table_name = ''
        refer_column_name = ''

    fk_info = {}  											#Initialising list and dict
    fk_fail_tables = []

    #Method to get information about all foreign key in the database
    def get_fk_info(self,execute):
        execute("select INFORMATION_SCHEMA.KEY_COLUMN_USAGE.TABLE_NAME,INFORMATION_SCHEMA.KEY_COLUMN_USAGE.COLUMN_NAME,INFORMATION_SCHEMA.KEY_COLUMN_USAGE\
.REFERENCED_TABLE_NAME,INFORMATION_SCHEMA.KEY_COLUMN_USAGE.REFERENCED_COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE join information_schema\
.TABLE_CONSTRAINTS on information_schema.TABLE_CONSTRAINTS.CONSTRAINT_NAME = INFORMATION_SCHEMA.KEY_COLUMN_USAGE.CONSTRAINT_NAME and INFORMATION_SCHEMA\
.TABLE_CONSTRAINTS.CONSTRAINT_TYPE='FOREIGN KEY' and INFORMATION_SCHEMA.TABLE_CONSTRAINTS.TABLE_SCHEMA ='%s'"%(ConnectMYSQL.db_name))
        test = ConnectMYSQL.cursor.fetchall()
        i = 0

        for each in test:
            table_name,column_name,refer_table_name,refer_column_name = UtilityFunctions().process_info(str(each))
            ForeignKeyHandling.fk_info[i] = ForeignKeyHandling.ForeignKeyParameter(table_name,column_name,refer_table_name,refer_column_name)
            i = i+1
        return ForeignKeyHandling.fk_info


    #Method to initialise fk_flag in each fk_info object
    def set_fk_flags(self,fk_info):
        for i in fk_info:
            if fk_info[i].fk_flag == 0:
                fk_info[i].fk_flag = 1
            elif fk_info[i].fk_flag == 1:
                if self.check_table_fk(fk_info[i].refer_table_name,fk_info):
                    fk_info[i].fk_flag = 2
                elif self.check_if_multiple_fk(fk_info,i):
                    fk_info[i].fk_flag = 2
                else:
                    continue


    #Method to check if current table is in fk_info
    def check_table_fk(self,each_table,fk_info):
        for i in fk_info:
            if fk_info[i].table_name == each_table:
                return 1
        return 0



    #Method to check if the table has more than 1 foreign key CONSTRAINT
    def check_if_multiple_fk(self,fk_info,i):
        for j in fk_info:
            if i != j and fk_info[i].table_name == fk_info[j].table_name:
                return 1
        return 0


    #Method to check if all the tables present in the fk_info table were executed
    def check_if_done(self,fk_info):                  #Used
        for i in fk_info:
            if fk_info[i].fk_flag == 1:
                return 1

        for i in fk_info:
            if fk_info[i].fk_flag == 2:
                return 2
        return 0

    #Method to set the same flag to all tables with the same name in fk_info
    def set_flag_for_all__same_table(self,fk_info,i,flag):
        for j in fk_info:
            if fk_info[i].table_name == fk_info[j].table_name:
                if flag == 0:
                    fk_info[j].fk_flag = 1
                elif flag == 1:
                    fk_info[j].fk_flag = 0


    #Method to edit the table flag status to 1 if no table is referred anymore
    def edit_fk_flag_status(self,fk_info,i):
        for j in fk_info:
            if fk_info[i].refer_table_name == fk_info[j].table_name:
                return
        fk_info[i].fk_flag = 1

    #Method to check if the current column has a Foreign Key
    def check_if_column_has_fk(self,table_name,column_name,fk_info):
        for index in fk_info:
            if fk_info[index].table_name == table_name and fk_info[index].column_name == column_name:
                return 1,index
        return 0,0

    #Method to fetch the value from the row which a foreign key refers to
    def get_random_value_from_FK_reference(self,fk_info,index):
        ConnectMYSQL.execute("Select %s from %s"%(fk_info[index].refer_column_name,fk_info[index].refer_table_name))
        refer_values = ConnectMYSQL.cursor.fetchall()
        try:
            return random.choice(refer_values)
        except:
            ForeignKeyHandling.fk_fail_tables.append(fk_info[index].table_name)
            return 'NULL'




class GeometryHandling(object):
    '''Class containing objects for dealing with geometry Data type present in database'''


    #Method to check if the current table has the geometry data type present in any of the columns
    def check_geometry_data_type(self,each_table,column_name):
        ConnectMYSQL().execute('Select  column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "%s" and table_name = "%s" and DATA_TYPE = "geometry"'%(ConnectMYSQL.db_name,each_table))
        datatypes = ConnectMYSQL.cursor.fetchall()
        for index in datatypes:
            if str(index).find("%s"%column_name) != -1 :
                return 1
        return 0



class MySQL_functions(object):
    '''Class of Objects use for adding data into a MySQL database'''


	#Method to get the name of all tables present in the datadbase
    def get_table_names(self,db_name,execute):
        ConnectMYSQL().execute('SELECT table_name FROM information_schema.tables WHERE table_schema="'+ConnectMYSQL().db_name+'" and TABLE_TYPE = "BASE TABLE"''')
        tables = ConnectMYSQL.cursor.fetchall()
        table_names = []
        for each_table in tables:
            each_table = UtilityFunctions().process_table_name(each_table)
            table_names.append(each_table)
        return table_names


    #Method to get the order in which data has to be added to the tables based on the Foreign Keys present
    def get_table_processing_order(self,fk_info,table_names):
        table_process_order = []

        for table_name in table_names:
            if ForeignKeyHandling().check_table_fk(table_name,fk_info):
                continue
            else:
                table_process_order.append(table_name)
        while ForeignKeyHandling().check_if_done(fk_info):
            for index in fk_info:
                if fk_info[index].fk_flag == 1:
                    fk_info[index].fk_flag = 0
                    ForeignKeyHandling().set_flag_for_all__same_table(fk_info,index,1)
                    table_process_order.append(fk_info[index].table_name)
                elif fk_info[index].fk_flag == 0:
                    continue
                else:
                    ForeignKeyHandling().edit_fk_flag_status(fk_info,index)
                    if ForeignKeyHandling().check_if_done(fk_info) == 2:
                        ForeignKeyHandling().set_flag_for_all__same_table(fk_info,index,0)
        return table_process_order


    #Method to get a dynamically created Column names list and respective Random values to be added in suitable form to be converted into a sql query
    def get_query_variables(self,table_name):
        column_query = ''
        random_value_query = ''
        ConnectMYSQL().execute("describe %s.%s"%(ConnectMYSQL.db_name,table_name))
        table_schema =ConnectMYSQL.cursor.fetchall()
        for each in table_schema:
            column_name,random_values = MySQL_functions().get_column_name_and_random_value(table_name,each)
            column_query = column_query + '%s,'%column_name
            if GeometryHandling().check_geometry_data_type(table_name,column_name):
                random_value_query = random_value_query +'@g,'
            else:
                random_value_query = random_value_query + '"%s",'%random_values

        return column_query[:-1],random_value_query[:-1]

    #Method to get column name and its random value to be added from the input schema
    def get_column_name_and_random_value(self,table_name,table_schema):
        table_schema = str(table_schema)
        id_name,id_end = UtilityFunctions().get_id_name(table_schema)
        table_schema = table_schema[id_end+4:]
        fk_flg,index = ForeignKeyHandling().check_if_column_has_fk(table_name,id_name,ForeignKeyHandling.fk_info)
        random_value = None
        if fk_flg:
            random_value = ForeignKeyHandling().get_random_value_from_FK_reference(ForeignKeyHandling.fk_info,index)
        elif random_value == None:
            random_value = MySQL_functions().get_id_random_value(table_schema)
        return id_name,random_value

    #Method to get the type of id and genereated random value
    def get_id_random_value(self,table_schema):
        id_value = 0
        random_value = 0
        if not(int(table_schema.find("tinyint")) == -1) :
            if UtilityFunctions().check_if_column_unsigned(table_schema):
                random_value = random.randint(1,256)
            else:
                random_value = random.randint(-128,127)


        elif not(table_schema.find("smallint") == -1) :
            if UtilityFunctions().check_if_column_unsigned(table_schema):
                random_value = random.randint(1,65536)
            else:
                random_value = random.randint(-32768,32767)


        elif not(table_schema.find("mediumint") == -1) :
            if UtilityFunctions().check_if_column_unsigned(table_schema):
                random_value = random.randint(0,16777215)
            else:
                random_value = random.randint(-8388608,8388607)


        elif int(table_schema.find("bigint")) != -1 :
            if UtilityFunctions().check_if_column_unsigned(table_schema):
                random_value = random.randint(0,9223372036854775807)
            else:
                random_value = random.randint(-9223372036854775808,9223372036854775807)


        elif table_schema.find("int") != -1 :
            if UtilityFunctions().check_if_column_unsigned(table_schema):
                random_value = random.randint(0,4294967290)
            else:
                random_value = random.randint(-2147483648,2147483647)


        elif table_schema.find("float") != -1 :
            random_value = 0
            if table_schema.find("float(") == -1:
                M = 10
                D = 2
            else:
                start = table_schema.find("float(")
                M,D = UtilityFunctions().get_MD_info(table_schema,start)
            random_value = UtilityFunctions().get_float_random_value(M,D)


        elif table_schema.find("double") != -1 :
            random_value = 0
            if table_schema.find("double(") == -1:
                M = 16
                D = 4
            else:
                start = table_schema.find("double(")+1
                M,D = UtilityFunctions().get_MD_info(table_schema,start)
            random_value = UtilityFunctions().get_float_random_value(M,D)

        elif table_schema.find("decimal") != -1 :
            random_value = 0
            if table_schema.find("decimal(") == -1:
                M = 10
                D = 2
            else:
                start = table_schema.find("decimal(")+2
                M,D = UtilityFunctions().get_MD_info(table_schema,start)
            random_value = UtilityFunctions().get_float_random_value(M,D)
            #string_flag = 1
                                                                        ###Not using a random date but the upper limit

        elif table_schema.find("timestamp") != -1 :
            random_value = "19731230153000"


        elif table_schema.find("year") != -1 :
            if table_schema.find("year(2)") != -1:
                random_value = "2155"
            else:
                random_value = "69"

        elif table_schema.find("date") != -1 :
            randoms = str(random.randint(1970,2069))
            random_value= randoms+"1230"

        elif table_schema.find("time") != -1 :
            random_value = "11:59:59"


        elif table_schema.find("varchar") != -1 :
            #string_flag = 1
            random_value =UtilityFunctions().get_varchar_random_value(table_schema)

        elif table_schema.find("char") != -1 :

            #string_flag = 1
            random_value = str(random.choice(string.ascii_lowercase))

        elif table_schema.find("tinyblob") != -1:
            random_value = random.getrandbits(254)

        elif table_schema.find("tinytext") != -1:
            random_value = random.getrandbits(254)


        elif table_schema.find("mediumblob") != -1:
            random_value = random.getrandbits(1677)

        elif table_schema.find("mediumtext") != -1:
            random_value = random.getrandbits(1677)

        elif table_schema.find("longblob" ) != -1:
            random_value = random.getrandbits(4294)

        elif table_schema.find("longtext") != -1:
            random_value = random.getrandbits(4294)


        elif table_schema.find("blob") != -1:
            random_value = random.getrandbits(655)

        elif table_schema.find("text") != -1:
            random_value = random.getrandbits(655)

        elif table_schema.find("enum") != -1 :
            #string_flag = 1
            random_value = UtilityFunctions().get_enum_values(table_schema)

        elif table_schema.find("geometry") != -1:
            #string_flag = 0
            random_value = 'ST_GeomFromText(@g)'


        elif table_schema.find("set") != -1:
            #string_flag = 1
            random_value = UtilityFunctions().get_set_value(table_schema)
        else:
            print "Data type not supported"
            random_value = 'None'
        return random_value


    #Method to create the final query to be executed to add the data into the database
    def create_query(self,column_query,random_value_query,table_name):
        query = "INSERT INTO %s(%s) VALUES (%s)"%(table_name,column_query,random_value_query)
        return query


class ConnectSqlite3(object):
    '''Class containing tools to connect python to sqlite3 database'''

    #Method to connect to the sqllite DB
    def connect_sqlite3(self):
        while True:
            ConnectSqlite3.db = raw_input("Enter the name/path of sqlite3 Database: ")
            if os.path.isfile(ConnectSqlite3.db):                                                 #Check if file exists
                break
            else:
                print "Enter a valid db or path"

        ConnectSqlite3.connection = sqlite3.connect(ConnectSqlite3.db)
        ConnectSqlite3.cursor = ConnectSqlite3.connection.cursor(buffered=True)                                  #initialising cursor
        ConnectSqlite3.execute = ConnectSqlite3.cursor.execute


class MainClass(object):
    '''Main class to execute on running the script'''

	#Main function to be called first
    def main(self):
        while True:
            os.system('cls')
            print "Welcome To Data Loader\n"
            print "Data Loader is a tool that comes in handy when your team is " \
              "feeling lazy to populate the your database of your project."
            print "Which database system are you using? "
            print "Press 1 for MySQL"
            #print "Press 2 for Sqlite3(Constraint support not added yet)"
            n=input()                               #switch case variable
            if n==1:
                os.system('cls')
                ConnectMYSQL().connect_mysql()
                ConnectMYSQL().initalise_database_for_randomDataLoad(ConnectMYSQL.execute)
                #Loop to get only a correct input
                while True:
                    number_of_rows = raw_input("Enter the number of rows you want to add: ")
                    try:
                        number_of_rows = int(number_of_rows)
                        if number_of_rows > 0:
                            break
                        else:
                            print "Invalid input for number of row. Please enter an integer"
                    except:
                        print "Invalid input for number of row. Please enter an integer"
                if number_of_rows >20:
                    print "Large number of rows to be printed please be patient."
                while int(number_of_rows):																	#Loop to add input number of rows
                    ForeignKeyHandling.fk_info = ForeignKeyHandling().get_fk_info(ConnectMYSQL.execute)
                    ForeignKeyHandling().set_fk_flags(ForeignKeyHandling.fk_info)
                    number_of_rows = number_of_rows - 1
                    table_names = MySQL_functions().get_table_names(ConnectMYSQL.db_name,ConnectMYSQL.execute)
                    table_process_order = MySQL_functions().get_table_processing_order(ForeignKeyHandling.fk_info,table_names)
                    for table_name in table_process_order:
                        while True:																			#Loop to resolve Primary Key Constraint
                            try:
                                column_query,random_value_query = MySQL_functions().get_query_variables(table_name)
                                query = MySQL_functions().create_query(column_query,random_value_query,table_name)
                                ConnectMYSQL.execute(query)
                                break
                            except:
                                print "\4",
            '''elif n==2:
                connect_sqlite3().connect_sqlite3()
                self.get_tables_sqlite()'''
            if n==1 :
                raw_input("Data was added into database. Press enter to close the connection")
                ConnectMYSQL().close_cursor(ConnectMYSQL.connection,ConnectMYSQL.cursor)
                break
            else:
                print "invalid input. Try Again"

#Calling main funtion on execution of python script
if __name__ == "__main__":
    main=MainClass()
    main.main()
