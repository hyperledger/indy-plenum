import pyorient

client = pyorient.OrientDB("localhost", 2424)
dbName = "test"
user = "root"
password = "password"
session_id = client.connect(user, password)
client.db_drop(dbName, pyorient.STORAGE_TYPE_MEMORY)
client.db_create(dbName, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)
# client.db_create(dbName, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
client.db_exists(dbName, pyorient.STORAGE_TYPE_MEMORY)
client.db_list()
# Need to open the db for doing read/writes on it
client.db_open(dbName, user, password)

cmd1 = client.command("create class Animal extends V")

cmd2 = client.command("create vertex Animal set name = 'rat', specie = 'rodent'")

cmd3 = client.query("select * from Animal")

### Create the vertex and insert the food values

cmd4 = client.command('create class Food extends V')
cmd5 = client.command("create vertex Food set name = 'pea', color = 'green'")

### Create the edge for the Eat action
cmd6 = client.command('create class Eat extends E')

### Lets the rat likes to eat pea
eat_edges = client.command(
    "create edge Eat from ("
    "select from Animal where name = 'rat'"
    ") to ("
    "select from Food where name = 'pea'"
    ")"
)

# Create edges using record id
eat_edges_1 = client.command("CREATE EDGE Eat FROM {} to {}".format(cmd3[0]._rid, cmd5[0]._rid))

### Who eats the peas?
pea_eaters = client.command("select expand( in( Eat )) from Food where name = 'pea'")
for animal in pea_eaters:
    print(animal.name, animal.specie)
'rat rodent'

### What each animal eats?
animal_foods = client.command("select expand( out( Eat )) from Animal")
for food in animal_foods:
    animal = client.query(
                "select name from ( select expand( in('Eat') ) from Food where name = 'pea' )"
            )[0]
    print(food.name, food.color, animal.name)
'pea green rat'

client.command("CREATE CLASS Car EXTENDS V")
client.command("CREATE CLASS Owns EXTENDS E")
client.command("CREATE CLASS Person EXTENDS V")

client.command("CREATE VERTEX Person SET name = 'Luca'")
client.command("CREATE VERTEX Person SET name = 'Luca1'")
client.command("CREATE VERTEX Person SET name = 'Luca2'")

client.command("CREATE VERTEX Car SET name = 'Ferrari Modena'")
client.command("CREATE VERTEX Car SET name = 'Ferrari Modena1'")
client.command("CREATE VERTEX Car SET name = 'Ferrari Modena2'")

cmd7 = client.command("select * from Person where name = 'Luca1' limit 1")

cmd8 = client.command("select * from Person where name = 'Luca1'")

cmd9 = client.command("CREATE EDGE Owns FROM ( SELECT FROM Person where name = 'Luca1') TO ( SELECT FROM Car where name='Ferrari Modena2')")

client.command("create class auto extends V")
client.command("create property auto.name string")
client.command("create index auto.name unique")

client.command("create class rides extends E")
client.command("create property rides.out link Person")
client.command("create property rides.in link auto")

client.command("create class bike extends auto")
client.command("create class cycle extends auto")

client.command("create vertex bike SET name = 'bik1'")
client.command("create vertex bike SET name = 'bike2'")
client.command("create vertex cycle SET name = 'cycle1'")
client.command("create vertex cycle SET name = 'cycle2'")

b= client.command("select * from auto where name='bike2'")[0]
print(b._class)

client.command("create class cycler extends E")
client.command("create property cycler.out link Person")
client.command("create property cycler.in link cycle")
client.command("create edge cycler from ( SELECT FROM Person where name = 'Luca') TO ( SELECT FROM cycle where name='cycle1')")

# client.command("create edge cycler from ( SELECT FROM Person where name = 'Luca1') TO ( SELECT FROM Car where name='Ferrari Modena2')")

client.command("select expand (in('cycler')) from cycle where name = 'cycle1'")
client.command("select expand (in('cycler')) from cycle where name = 'cycle2'")

client.command("create edge rides from ( SELECT FROM Person where name = 'Luca1') TO ( SELECT FROM cycle where name='cycle1')")
client.command("create edge rides from ( SELECT FROM Person where name = 'Luca2') TO ( SELECT FROM bike where name='bike2')")

client.command("create class profile")
client.command("create property profile.id string")
client.command("create index profile.id unique")
client.command("insert into profile set name = 'Luca', age = 21, id = '1'")
client.command("update profile set name = 'Luca1' upsert where id = '1'")
client.command("update profile set name = 'Luca2' upsert where id = '2'")

client.command("create class Version")
client.command("create property Version.v string")
client.command("create property Version.release string")
client.command("create class Package")
client.command("create property Package.versions embeddedlist Version")

r = client.command('insert into Package set versions = [{"v":"1.0.1", "release":"monday"}]')
client.command('update %s add versions = [{"v":"1.0.2", "release":"tuesday"}]' % r[0]._rid)

cmd = (
    # "begin;"
    "create class Address;"
    "create property Address.street String;"
    "create property Address.city String;"
    "create class Client;"
    "create property Client.name String;"
    "create property Client.phones embeddedSet String;"
    "create property Client.addresses embeddedList Address;"
    "insert into client set name = 'James Bond', phones = ['1234', '34567'], addresses = [{'city':'Shanghai', 'zip':'3999'}, {'city':'New York', 'street':'57th Ave'}];"
    "update client add addresses = [{'city':'London', 'zip':'67373'}];"
    # "commit;"
)

client.batch(cmd)
client.command("update Client add addresses = [{'city':'Delhi', 'zip':'2'}]")
client.command("select from Client")[0].oRecordData
client.command("update Client add addresses = {'city':'Mumbai', 'zip':'3'}")
client.command("select from Client")[0].oRecordData

cmd = (
    "create class report;"
    "create property report.id integer;"
    "create property report.marks embeddedmap string;"
    "insert into report set id = 1, marks={'p': 100, 'm': 'A', 'c': 2.4};"
)
client.batch(cmd)

client.command("select from report where marks[p] = 100")[0].oRecordData
client.command("select from report where marks.p = 100")[0].oRecordData

client.command("update report set marks.m = 'B' where id = 1")
client.command("update report set marks.b = 21 where id = 1")
client.command("update report set date = 1460128589.825324 return after $this.marks where id = 1")