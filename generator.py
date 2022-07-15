from faker import Faker
from random import randrange
from datetime import datetime
import re
import csv

fake = Faker("it_IT")

file = [['FullName','FirstName','LastName','CallingNbr','CalledNbr','StartDate','EndDate','Duration','City','State','Address','CellSite']]


''' LISTA DI PERSONE '''
people = []
for _ in range(500):
    now = []    
    now.append(fake.first_name_nonbinary())
    now.append(fake.last_name_nonbinary())
    now.insert(0, now[0]+now[1])
    while True:
        pn = fake.phone_number()
        if re.findall("^[3][0-9]{9}$", pn) != []:
            now.append(pn)
            break
    people.append(now)

''' LISTA DI CHIAMATE '''
calls = []
for i in range(1000):
    calls += [people[randrange(0,50)].copy()]
    while True:
        end = people[randrange(0,50)][3]
        if end != calls[i][3]:
            calls[i] += [end]
            break
with open('people.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(file[0][0:4])
    write.writerows(people)

del people

''' LISTA CELLE '''
cells = []
for i in range(200):
    cells.append([])
    cells[i].append(fake.administrative_unit())
    cells[i].append(fake.current_country_code())
    cells[i].append(fake.street_name())
    cells[i].append(i)

with open('cells.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(file[0][8:12])
    write.writerows(cells)

''' LISTA DATE '''
for row in calls:
    row.append(int(fake.unix_time(datetime(2020,2,1), datetime(2020,1,1))))
    delta = randrange(1,1200)
    row.append(row[5]+delta)
    row.append(delta)

for i in range(len(calls)):
    file.append(calls[i].copy() + cells[randrange(0,20)].copy())

del cells
del calls

print(file[1:])
    
with open('prova.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(file[0])
    write.writerows(file[1:])
