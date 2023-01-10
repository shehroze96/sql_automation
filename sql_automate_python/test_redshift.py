import redshift_connector
import numpy
import csv
import shutil, os

# conn = redshift_connector.connect(
#     host='prod-tracking.cjk1id3qusgl.eu-west-1.redshift.amazonaws.com',
#     port=5439,
#     database='prod',
#     user='skhan',
#     password='q%td!#t<KaspW}L82('
#  )

conn = redshift_connector.connect(
    host='barracuda-klipfolio-postgres.postgres.database.azure.com',
    port=5432,
    database='reporting_prod',
    user='skhan@barracuda-klipfolio-postgres',
    password='r6EM!QvrfFR15BXYU-21'
)



print(os.getcwd())
outputFile = open('output.csv', 'w', newline='')
outputWriter = csv.writer(outputFile)
mycursor = conn.cursor()
#select * from information_schema.columns where table_name = 'tablename';
mycursor.execute("select * from session where inserted_timestamp > '2022-03-01 00:00:00' limit 3;")
##result: tuple = mycursor.fetchall()
##print(result)

headers = [i[0] for i in mycursor.description]
outputWriter.writerow(headers)

print(headers)
for row in mycursor:
   outputWriter.writerow(row)
   #print(row)

outputFile.close()
print("done")

conn.close()
