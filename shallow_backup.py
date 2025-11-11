import client # this is production
import backup_client_api as backup_client # this is for backup
import time
database_to_backup = "db_name_here"
external_db_password = "db_pass_here"
external_db_url = "https://db_url.com"

internal_db_port = 8080
internal_db_password = "internal_db_pass"

client.init(database_to_backup, external_db_url, external_db_password)
backup_client.init(database_to_backup, f"http://127.0.0.1:{internal_db_port}", internal_db_password)

print("getting data")
local_data = set(backup_client.searchNames(""))
other_data = set(client.searchNames(""))

print("iterating list")
toget = []

for value in other_data:
    if value not in local_data:
        toget.append(value)

print(f"missing: {len(toget)}")

# if you want to be able to get a large number of keys, use the commented line of code. Otherwise leave as is, I'll make this a CLI later... probably.

if len(toget) > 0:
    new_data = client.getKeysList(toget) # GET KEYS PRECISE
    # new_data = client.getKeysIncrements(toget, log=True) # GET KEYS LARGE

    backup_client.makeKeys(new_data)
