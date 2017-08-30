import sqlite3
import urllib.request, urllib.parse, urllib.error
import ssl
import json
import time
import config

#get country from directory
def defineCountry(lst):
    for item in lst:
        if 'country' in item['types']:
            return (item['long_name'],item['short_name'])
    return ('','')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

#Enter your Google geocode key
api_key =  config.GoogleKey

serviceurl = "https://maps.googleapis.com/maps/api/geocode/json?"

conn = sqlite3.connect('meteorite.sqlite')
cur = conn.cursor()

count = 0
while True:
    #seaching for the first unretrived address
    cur.execute('''SELECT address.id,
                          address.reclat,
                          address.reclon,
                          address.geodata,
                          address.country_id
        FROM address
        JOIN mlandings ON mlandings.address_id = address.id
        WHERE address.reclat IS NOT NULL and  address.reclon IS NOT NULL
        AND address.reclat != 0 AND address.reclon != 0
        AND (address.geodata NOT LIKE '%ZERO_RESULTS%' or address.geodata IS NULL)
        AND address.country_id IS NULL
        ORDER BY mlandings.year desc LIMIT 1''')
    res = cur.fetchone()
    address_id = res[0]
    reclat = res[1]
    reclon = res[2]
    geodata = ''
    if res[3] is not None:
        geodata = res[3]
    if res[4] is not None: country_id = res[4]
    print(res)

    if not geodata:
        print(geodata)
        #creating an url for the google api
        url = serviceurl +'latlng='+ str(reclat)+','+str(reclon) + '&key=' + api_key
        print(url)

        #geting address from google
        uh = urllib.request.urlopen(url, context=ctx)
        data = uh.read().decode()

        try:
            js = json.loads(data)
        except:
            print(data)  # We print in case unicode causes an error
            continue

        if 'status' not in js or (js['status'] != 'OK' and js['status'] != 'ZERO_RESULTS') :
            print('==== Failure To Retrieve ====')
            print(data)
            break

        #updating address in the DB
        cur.execute('''UPDATE address SET geodata = ? WHERE id = ?''',
            (memoryview(data.encode()), address_id ) )
        conn.commit()
    else:
        try:
            js = json.loads(geodata)
        except:
            print(geodata)  # We print in case unicode causes an error
            continue

    if js['status'] == 'ZERO_RESULTS':
        conn.commit()
        print('ZERO_RESULTS')
        continue

    country_names = defineCountry(js["results"][0]["address_components"])
    print(country_names)

    cur.execute('''INSERT OR IGNORE INTO country (country_longname, country_shortname)
        VALUES (?, ?)''', country_names)
    cur.execute('SELECT id from country where country_longname = ?', (country_names[0],))
    country_id = cur.fetchone()[0]

    cur.execute('''UPDATE address SET country_id = ? WHERE id = ?''',
        (country_id, address_id ) )

    conn.commit()
    count = count + 1
    if count % 10 == 0 :
        print('Pausing for a bit...')
        time.sleep(5)

    if count == 1200:
        print('Finite incantatem')
        break

conn.commit()
cur.close()
