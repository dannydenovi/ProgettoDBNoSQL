from faker import Faker
from random import randrange
from datetime import datetime
from threading import Thread
import csv

''' VARIABILI '''
num_people  = 1000000
num_calls   = 1000000
num_cells   = 200000
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
def gen_people(num_people):
    global people 
    people = [file[:3] + ['Number']]
    for i in range(num_people):
        now = []    
        now.append(fake.first_name_nonbinary())
        now.append(fake.last_name_nonbinary())
        now.insert(0, now[0] + now[1])
        while True:
            pn = fake.unique.phone_number()
            if pn[0] == '3':
                now.append(pn)
                break
        people.append(now)


''' LISTA DI CHIAMATE '''
def gen_calls(num_calls, num_people, start_date, end_date, range_call): 
    global calls
    calls = [file[3:8].copy()]
    for i in range(1,num_calls+1):
        calls += [[people[randrange(1, num_people)][3]]]
        while True:
            end = people[randrange(1, num_people)][3]
            if end != calls[i][0]:
                calls[i] += [end]
                break
                
    """ LISTA DATE  """
    for row in calls[1:]:
        row.append(int(fake.unix_time(datetime(end_date[0], end_date[1], end_date[2]), datetime(start_date[0], start_date[1], start_date[2]))))
        delta = randrange(1, range_call)
        row.append(row[2] + delta)
        row.append(delta)
    write("calls", calls)


''' LISTA CELLE '''
def gen_cells(num_cells):
    global cells
    cells = [file[-4:]]
    for i in range(1,num_cells+1):
        cells += [[fake.administrative_unit()]]
        cells[i].append(fake.current_country_code())
        cells[i].append(fake.street_name())
        cells[i].append(i)
    write("cells", cells)

def write(name, list):
    with open(name+'.csv', 'w') as f:
        write = csv.writer(f)
        write.writerows(list)

gen_people(num_people)

threads = []
threads.append(Thread(target=write, args=("people",people)))
threads.append(Thread(target=gen_cells, args=(num_cells,)))
threads.append(Thread(target=gen_calls, args=(num_calls, num_people, start_date, end_date, range_call)))

for i in range(3):
    threads[i].start()

