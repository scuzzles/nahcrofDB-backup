## nahcrofDB-backup
This repo is a set of tools that I use internally to manage my backup databases

## Files and their purpose
* client.py - This is literally just a default client for nahcrofDB
* backup_client_api.py - this is a second client since I'm a weirdo and didn't make the default client with classes (coming... eventually. I promise)
* create_backup_script.py - this pulls every value in the database in chunks of 100, then double checks that it isn't missing any
* shallow_backup.py - currently not very friendly but this assumes you already have most of the database will only check for missing values to (hopefully) save time. I would avoid using this for now
