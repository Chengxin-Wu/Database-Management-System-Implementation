import os
db_path = os.path.join(os.getcwd(), "db")
print(db_path)
for path, db_list, _ in os.walk(db_path):
    for db_name in db_list:
        print(db_name)