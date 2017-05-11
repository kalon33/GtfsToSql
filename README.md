GtfsToSql
=========
Parses a GTFS feed into an SQL database

Installation
------------
If you're using Eclipse, you need to:

1. File, Import
2. Select 'Existing projects into workspace'
3. Select the directory in 'Select root directory'

Usage
-----
`java -jar GtfsToSql.jar -s jdbc:sqlite:/path/to/db.sqlite -g /path/to/extracted/gtfs/`

Notes
-----
* Only supports Sqlite and PostgreSQL currently
* GTFS file must be extracted already
* All columns are mapped saved as 'text' (that is, not parsed, modified or typecast)

Table names are the same as filename in the GTFS file (without the `.txt`)
