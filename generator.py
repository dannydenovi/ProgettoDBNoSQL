#!/bin/env python3

from faker import Faker
from random import randrange
from datetime import datetime
from argparse import ArgumentParser
from threading import Thread
from os import path, mkdir
import csv


parser = ArgumentParser(description="Select multiplyer for number of datas.")
parser.add_argument('-p', '--percentage',
                    dest="P",
                    required=True,
                    help='Specify the percentage.')
args = parser.parse_args()

''' VARIABILI '''
num_people = 20000 * int(args.P)//100
num_calls = 1000000 * int(args.P)//100
num_cells = 16000 * int(args.P)//100
start_date = [2020, 1, 1]
end_date = [2020, 2, 1]
range_call = 1200

fake = Faker("it_IT")
people = []
cells = []
calls = []

file = [
    'FullName',
    'FirstName',
    'LastName',
    'CallingNbr',
    'CalledNbr',
    'StartDate',
    'EndDate',
    'Duration',
    'CellSite',
    'City',
    'State',
    'Address'
]

''' LISTA DI PERSONE '''


def gen_people(num_people):
    global people
    people = [['Number'] + file[:3]]
    for i in range(num_people):
        now = [
            fake.first_name_nonbinary(),
            fake.last_name_nonbinary()
        ]
        now.insert(0, now[0] + now[1])
        while True:
            pn = fake.unique.phone_number()
            if pn[0] == '3' and len(pn) == 10:
                now.insert(0, pn)
                break
        people.append(now)
    write('people', people)


''' LISTA DI CHIAMATE '''


def gen_calls(num_calls, num_people, start_date, end_date, range_call):
    global calls
    calls = [file[3:9].copy()]
    for i in range(1, num_calls + 1):
        calls += [[people[randrange(1, num_people)][0]]]
        while True:
            end = people[randrange(1, num_people)][0]
            if end != calls[i][0]:
                calls[i] += [end]
                break

    """ LISTA DATE  """
    for i in range(1, len(calls[1:]) + 1):
        calls[i].append(int(fake.unix_time(datetime(end_date[0], end_date[1], end_date[2]),
                                           datetime(start_date[0], start_date[1], start_date[2]))))
        delta = randrange(1, range_call)
        calls[i].append(calls[i][2] + delta)
        calls[i].append(delta)
        calls[i].append(cells[randrange(1, num_cells)][0])
    write("calls", calls)


''' LISTA CELLE '''


def gen_cells(num_cells):
    global cells
    cells = [file[-4:]]
    for i in range(1, num_cells + 1):
        cells += [[fake.administrative_unit()]]
        cells[i].append(fake.current_country_code())
        cells[i].append(fake.street_name())
        cells[i].insert(0, i)
    write("cells", cells)


def write(name, list):
    with open('csv/' + name + '.csv', 'w') as f:
        write = csv.writer(f)
        write.writerows(list)


if not path.exists("csv"):
    mkdir("csv")

thread = Thread(target=gen_cells, args=(num_cells,))

thread.start()
gen_people(num_people)

thread.join()

gen_calls(num_calls, num_people, start_date, end_date, range_call)
