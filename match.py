import csv
from utils import is_valid_email
from enum import Enum

class SFC(Enum):
    CONTACTID = 0
    FNAME = 1
    LNAME = 2
    ACCTNAME = 3
    ACCTID = 4
    ACCTOWNER = 5
    CONTACTOWNER = 6
    EMAIL = 7
    PRIMARYEMAIL = 8
    DBSNUM = 9
    INFNUMB = 10


sfc_emails = set() # {list_of_emails}
with open('sfexport_90429.csv', 'r', encoding='ISO-8859-1') as inf:
    reader = csv.reader(inf)
    header = next(reader)

    for row in reader:
        sfc_emails.add(row[SFC.EMAIL.value].lower())

with open('140FlatrateOffer.csv', 'r') as inf:
    reader = csv.reader(inf)

    target_file = open('campaign_import.csv', 'w', newline='')
    writer = csv.writer(target_file)
    writer.writerow(next(reader))

    total_rows = 0
    matches = 0

    for row in reader:
        total_rows = total_rows + 1
        email = row[0].lower()

        if email in sfc_emails:
            matches = matches + 1
            writer.writerow(row)
    
    target_file.close()
    print('Total Rows:', total_rows)
    print('Matches:', matches)