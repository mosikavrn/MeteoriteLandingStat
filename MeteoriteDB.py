import sqlite3
import json

fh = open("rows.json")
mjson = json.load(fh)

conn = sqlite3.connect("meteorite.sqlite")
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS mlandings
                (id INTEGER primary key UNIQUE,
                 name TEXT,
                 nametype TEXT,
                 recclass TEXT,
                 mass REAL,
                 fall TEXT,
                 year DATE,
                 address_id INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS address
                (id INTEGER primary key UNIQUE,
                 reclat REAL,
                 reclon REAL,
                 geodata TEXT,
                 country_id INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS country
                (id INTEGER primary key UNIQUE,
                 country_longname TEXT UNIQUE,
                 country_shortname TEXT UNIQUE,
                 aria REAL)''')

for mitem in (mjson['data']):
    name = mitem[8]
    if mitem[9] is not None : mid = int(mitem[9])
    nametype = mitem[10]
    recclass = mitem[11]
    if mitem[12] is not None : mass = float(mitem[12])
    fall = mitem[13]
    year = mitem[14]
    if mitem[15] is not None : reclat = float(mitem[15])
    if mitem[16] is not None : reclon = float(mitem[16])

    print(mid, name, nametype, recclass, mass, fall, year, reclat, reclon)

    cur.execute('''INSERT OR IGNORE INTO address (reclat, reclon)
        VALUES (?, ?)''', (reclat, reclon))
    cur.execute('SELECT id from address where reclat = ? and reclon = ?', (reclat, reclon))
    address_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO mlandings (id, name, nametype, recclass, mass, fall, year, address_id)
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )''', (mid, name, nametype, recclass, mass, fall, year, address_id))

    if mid % 50 == 0 : conn.commit()
cur.close()
