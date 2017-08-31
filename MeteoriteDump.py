import sqlite3
import json
import codecs

conn = sqlite3.connect('meteorite.sqlite')
cur = conn.cursor()

cur.execute('''SELECT address.reclat,
                      address.reclon,
                      mlandings.name || ' ' || mlandings.recclass
                    FROM mlandings
                JOIN address ON mlandings.address_id = address.id
                WHERE address.geodata IS NOT NULL
                    AND address.geodata NOT LIKE '%ZERO_RESULTS%'
                    AND mlandings.duplicate == 0''')
fhand = codecs.open('where.js', 'w', "utf-8")
fhand.write("meteoriteData = [\n")
count = 0
for row in cur :
    lat = row[0]
    lon = row[1]
    name = row[2]

    try :
        print(lat, lon, name)

        count = count + 1
        if count > 1 : fhand.write(",\n")
        if count > 400 : break
        output = "["+str(lat)+","+str(lon)+", '"+name+"']"
        fhand.write(output)
    except:
        continue

fhand.write("\n];\n")
cur.close()
fhand.close()
print(count, "records written to where.js")
print("Open where.html to view the data in a browser")
