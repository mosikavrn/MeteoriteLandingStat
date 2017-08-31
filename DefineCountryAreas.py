import json
import sqlite3

def fillUpCountryArias():
    fh = open("area.json")
    mjson = json.load(fh)

    conn = sqlite3.connect('meteorite.sqlite')
    cur = conn.cursor()

    for country in (mjson["areas"]):
        print(country["country"],country["area"])
        country_name = country["country"]
        country_area = int(country["area"])

        cur.execute('''UPDATE country SET area = ? WHERE country_shortname = ?''',
            (country_area, country_name) )
        conn.commit()

    cur.close()

fillUpCountryArias()
