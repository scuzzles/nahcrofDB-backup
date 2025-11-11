import client
import backup_client_api as backup_client
import time

# CONFIG VARIABLES

# these are values for the database you will be pulling data from
database_to_backup = "db_name_here"
external_db_password = "db_api_password_here"
external_db_url = "http://db_url.here"

# these are values for the database you will be updating
internal_db_port = 8080
internal_db_password = "db_api_password_here"

# END OF CONFIG VARIABLES

# you can ignore everything from here down :)
client.init(database_to_backup, external_db_url, external_db_password)
backup_client.init(database_to_backup, f"http://127.0.0.1:{internal_db_port}", internal_db_password)

keys = client.searchNames("", where=None)
known_data = {}
total_keys = len(keys)

start = time.time()
final_request = client.getKeysIncrements(keys, log=True, increment=100)
end = time.time()
total = end-start
print(f"total time to retrieve values: {total}s")
print("writing to file")
print("")


print("starting backup")
start = time.time()
print(backup_client.makeKeys(final_request))
end = time.time()
total = end-start
print(f"backup created! (took {total}s)")

def verify_backup():
    print()
    print("verifying backup...")
    missing_keys = []
    primary_keys = client.searchNames("", where=None)
    backup_keys = backup_client.searchNames("", where=None)
    for key in primary_keys:
        if key not in backup_keys:
            missing_keys.append(key)
    print(f"keys missing from backup: {len(missing_keys)}")
    return missing_keys

missing_keys = verify_backup()
if len(missing_keys) > 0:
    missing_values = client.getKeysList(missing_keys)
    backup_client.makeKeys(missing_values)

    verify_backup()
print("Backup complete!")
