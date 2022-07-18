from faker import Faker
from random import randrange
from datetime import datetime
import re
import csv

''' VARIABILI '''
num_people  = 50
num_calls   = 100
num_cells   = 20
start_date  = [2020, 1, 1]
end_date    = [2020, 2, 1]
range_call  = 1200

fake = Faker("it_IT")

file = [
            'FullName',
            'FirstName',
            'LastName',
            'CallingNbr',
            'CalledNbr',
            'StartDate',
            'EndDate',
            'Duration',
            'City',
            'State',
            'Address',
            'CellSite'
        ]


''' LISTA DI PERSONE '''
people = [file[:3] + ['Number']]
for i in range(num_people):
    now = []    
    now.append(fake.unique.first_name_nonbinary())
    now.append(fake.unique.last_name_nonbinary())
    now.insert(0, now[0] + now[1])
    while True:
        pn = fake.unique.phone_number()
        if re.findall("^[3][0-9]{9}$", pn) != [] and pn not in (person[3] for person in people):
            now.append(pn)
            break
    people.append(now)


''' LISTA DI CHIAMATE '''
calls = [file[3:8].copy()]
for i in range(1,num_calls+1):
    calls += [[people[randrange(0, num_people)][3]]]
    while True:
        end = people[randrange(0, num_people)][3]
        if end != calls[i][0]:
            calls[i] += [end]
            break

'''
with open('people.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(file[0][0:4])
    write.writerows(people)
'''

''' LISTA CELLE '''
cells = [file[-4:]]
for i in range(1,num_cells+1):
    cells += [[fake.unique.administrative_unit()]]
    cells[i].append(fake.current_country_code())
    cells[i].append(fake.unique.street_name())
    cells[i].append(i)

'''
with open('cells.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(file[0][8:])
    write.writerows(cells)
'''

''' LISTA DATE '''
for row in calls[1:]:
    row.append(int(fake.unix_time(datetime(end_date[0], end_date[1], end_date[2]), datetime(start_date[0], start_date[1], start_date[2]))))
    delta = randrange(1, range_call)
    row.append(row[2] + delta)
    row.append(delta)

for i in range(len(calls)):
    file.append(calls[i].copy() + cells[randrange(0,num_cells)].copy())
'''
with open('gen.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(file[0])
    write.writerows(file[1:])
'''
print(people)
print(cells)
print(calls)
