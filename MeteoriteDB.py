import sqlite3
import json
import re

DBname = "meteorite.sqlite"

def createDB():
    conn = sqlite3.connect(DBname)
    cur = conn.cursor()
    #field duplicate: 1 - if there were landings with same latlon and name
    #поле duplicate: 1 - если уже было падение с аналогичным именем и координатами
    cur.execute('''CREATE TABLE IF NOT EXISTS mlandings
                    (id INTEGER primary key UNIQUE,
                     name TEXT,
                     nametype TEXT,
                     recclass TEXT,
                     mass REAL,
                     fall TEXT,
                     year DATE,
                     address_id INTEGER,
                     duplicate INTEGER DEFAULT 0)''')

    cur.executescript('''CREATE TABLE IF NOT EXISTS address
                    (id INTEGER primary key UNIQUE,
                     reclat REAL,
                     reclon REAL,
                     geodata TEXT,
                     country_id INTEGER);
                     CREATE UNIQUE INDEX latlon ON address(reclat,reclon)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS country
                    (id INTEGER primary key UNIQUE,
                     country_longname TEXT UNIQUE,
                     country_shortname TEXT UNIQUE,
                     area REAL)''')
    cur.close()

#fill up database from file "rows.json" that was downloaded from NASA.gov
def fillUpDB():
    fh = open("rows.json")
    mjson = json.load(fh)

    conn = sqlite3.connect(DBname)
    cur = conn.cursor()

    for mitem in (mjson['data']):
        name = mitem[8]
        mid = int(mitem[9]) if mitem[9] is not None else None
        nametype = mitem[10]
        recclass = mitem[11]
        mass = float(mitem[12]) if mitem[12] is not None else None
        fall = mitem[13]
        year = mitem[14]
        reclat = float(mitem[15]) if mitem[15] is not None else None
        reclon = float(mitem[16]) if mitem[16] is not None else None

        print(mid, name, nametype, recclass, mass, fall, year, reclat, reclon)

        #latlon stores in table address and then forein key inserted into mlandings
        if reclat is None or reclon is None or (reclat == 0 and reclon == 0):
            address_id = None
        else:
            cur.execute('''INSERT OR IGNORE INTO address (reclat, reclon)
                VALUES (?, ?)''', (reclat, reclon))
            cur.execute('SELECT id from address where reclat = ? and reclon = ?', (reclat, reclon))
            address_id = cur.fetchone()[0]


        cur.execute('''INSERT OR IGNORE INTO mlandings (id, name, nametype, recclass, mass, fall, year, address_id)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )''', (mid, name, nametype, recclass, mass, fall, year, address_id))

        if mid % 50 == 0 : conn.commit()
    cur.close()

def cleanUpDB():
    conn = sqlite3.connect(DBname)
    maincur = conn.cursor()
    cur = conn.cursor()

    #find all landings with same latlon
    maincur.execute('''select mlandings.id, mlandings.name, mlandings.address_id from
                	(select count(1) mcount, address_id
                    	from mlandings
                    	where address_id is not null
                    	group by address_id) msel
                    join mlandings on msel.address_id = mlandings.address_id
                    where msel.mcount > 1
                    ''')

    for item in maincur:
        short_meteorite_name = re.findall('([a-zA-Z ]+)',item[1])[0].strip()
        print(item[1])
        mlanding_id = int(item[0])
        address_id = item[2]

        #for each landing find first one with same latlon
        cur.execute('''select mlandings.id, mlandings.name from mlandings
                    where address_id = ? and name LIKE ?
                    ORDER BY mlandings.name LIMIT 1''',
                    (address_id, '%'+short_meteorite_name+'%'))
        firstlanding_id = int(cur.fetchone()[0])

        #if current landing is not the first one, set duplicate parametr = 1
        if firstlanding_id != mlanding_id:
            cur.execute('''UPDATE mlandings SET duplicate = 1 where id = ?''', (mlanding_id,))
            conn.commit()

    cur.close()
    maincur.close()

createDB()
fillUpDB()
cleanUpDB()
