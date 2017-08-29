MeteoriteDB.py - create meteorite.sqlite if there was not such a DB, read the file rows.json downloaded from 
	https://data.nasa.gov/Space-Science/Meteorite-Landings/gh4g-9sfh, parse it and fill up the DB

	Under NameType, 'valid' is for most meteorites and 'relict' are for objects that were once meteorites but are now highly altered by weathering on Earth.

	More recent meteorites will be checked on http://meteoriticalsociety.org

MeteoriteLocation.py - using Google API define places where meteorites landed. Parse Google JSON and get countries.

MeteoriteDump.py - take data from DB and create JSON file with geodata to put marks on the map
 
