import pymongo
from utils.common import env_var
from bson.json_util import dumps

uri = env_var("DB_HOST")
db_user = env_var("DB_USER")
db_pass = env_var("DB_PASSWORD")
db_name = env_var("DB_NAME")
client = pymongo.MongoClient(uri, username=db_user, password=db_pass, serverSelectionTimeoutMS=2000)
db = client["nanopore"]
a = db['samples'].find_one({})
b = db['batches'].find_one({})
c = db['files'].find_one({})
d = db['directories'].find_one({})
e = db['curations'].find_one({})
#print(a['dna']['pores'].keys())
for file, stuff in {'samples':a,
                    'batches':b,
                    'files':c,
                    'directories':d,
                    'curations':e}.items():
    with open(f'tmp/{file}.json', 'w') as d:
        d.write(dumps(stuff))
