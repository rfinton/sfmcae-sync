import csv
from enum import Enum

class MCAE(Enum):
    PROSPECTID = 0
    FIRSTNAME = 1
    LASTNAME = 2
    EMAIL = 3
    COMPANY = 4
    LASTACTIVITYAT = 5
    CAMPAIGN = 6
    NOTES = 7
    SCORE = 8
    GRADE = 9
    WEBSITE = 10
    JOBTITLE = 11
    DEPARTMENT = 12
    COUNTRY = 13
    ADDRESSONE = 14
    ADDRESSTWO = 15
    CITY = 16
    STATE = 17
    TERRITORY = 18
    ZIP = 19
    PHONE = 20
    FAX = 21
    SOURCE = 22
    ANNUALREVENUE = 23
    EMPLOYEES = 24
    INDUSTRY = 25
    DONOTEMAIL = 26
    DONOTCALL = 27
    YEARSINBUSINESS = 28
    COMMENTS = 29
    SALUTATION = 30
    OPTEDOUT = 31
    REFERRER = 32
    CREATEDDATE = 33
    UPDATEDDATE = 34
    USER = 35
    FIRSTASSIGNED = 36
    CRMCONTACTFID = 37
    CRMLEADFID = 38
    LISTS = 39
    AREAOFINTEREST = 40
    CUSTOMERNUMBER = 41
    EQUIPMENTSTATUS = 42
    GDS4SSOPSELABEL1 = 43
    GDS4SSOPSELABEL2 = 44
    GDS4SSOPSELABEL3 = 45
    GDS4SSOPSELABEL4 = 46
    GDS4SSOPSELABEL5 = 47
    GDS4SSOPSELINK1 = 48
    GDS4SSOPSELINK2 = 49
    GDS4SSOPSELINK3 = 50
    GDS4SSOPSELINK4 = 51
    GDS4SSOPSELINK5 = 52
    MESSAGE = 53
    MODEL = 54
    MODELOFINTEREST = 55
    PRODUCTINTEREST = 56
    SALESMAN = 57
    UTM_CAMPAIGN = 58
    UTM_CONTENT = 59
    UTM_ID = 60
    UTM_MEDIUM = 61
    UTM_SOURCE = 62
    UTM_TERM = 63
    WHEN_DO_YOU_PLAN_TO_PURCHASE = 64
    WHEN_DO_YOU_PLAN_TO_RENT = 65
    GACAMPAIGN = 66
    GAMEDIUM = 67
    GASOURCE = 68
    GACONTENT = 69
    GATERM = 70

class SFC(Enum):
    CONTACTID = 0
    FIRSTNAME = 1
    LASTNAME = 2
    ACCOUNTNAME = 3
    ACCOUNTID = 4
    ACCOUNTOWNER = 5
    CONTACTOWNER = 6
    EMAIL = 7
    PRIMARYEMAIL = 8
    DBSCUSTOMERNUMBER = 9
    INFLUENCERNUMBERINDBS = 10

class CRMContact:
    def __init__(self, data):
        self.firstname = data[SFC.FIRSTNAME.value]
        self.lastname = data[SFC.LASTNAME.value]
        self.acctname = data[SFC.ACCOUNTNAME.value]
        self.acctid = data[SFC.ACCOUNTID.value]
        self.acctowner = data[SFC.ACCOUNTOWNER.value]
        self.email = data[SFC.EMAIL.value]
        self.primemail = data[SFC.PRIMARYEMAIL.value]
        self.dbscustnumber = data[SFC.DBSCUSTOMERNUMBER.value]
        self.infnumber = data[SFC.INFLUENCERNUMBERINDBS.value]

class PardotProspect:
    def __init__(self, data):
        self.prospectid = data[MCAE.PROSPECTID.value]
        self.crmid = data[MCAE.CRMCONTACTFID.value]
        self.email = data[MCAE.EMAIL.value].lower()
        self.dbscustnumber = data[MCAE.CUSTOMERNUMBER.value]

def init_crm_contacts():
    crm_contacts = {}

    with open('sfexport_52126.csv', 'r', encoding='ISO-8859-1') as inf:
        reader = csv.reader(inf)
        headers = next(reader)

        for row in reader:
            contactid = row[SFC.CONTACTID.value]
            crm_contacts[contactid] = CRMContact(row)

    return crm_contacts

def init_mcae_prospects():
    pardot_prospects = {}

    with open('Prospects_06252025.csv', 'r', encoding='utf-8-sig') as inf:
        reader = csv.reader(inf)
        headers = next(reader)

        for row in reader:
            pardot_prospects[row[MCAE.PROSPECTID.value]] = PardotProspect(row)

    return pardot_prospects

def run_dedupes():
    exclusions = set()

    with open('dedupe_exclusions.csv', 'r') as inf:
        reader = csv.reader(inf)
        
        for row in reader:
            exclusions.add(row[0].lower())

    with open('prospectCountsByEmailExport.csv', 'r', encoding='utf-8-sig') as inf:
        reader = csv.reader(inf)
        next(reader) # skip first row

        pardot_prospects = init_mcae_prospects()

        # column indices
        _email = 0
        _prospect_count = 1
        _recycle_bin_count = 2

        results = {
            "both_synced": [],
            "none_synced": [],
            "one_synced": []
        }

        for row in reader:
            if row[_email].lower() in exclusions:
                continue
            
            # Subtract the 'Total Prospect Count' from the 'Recycle Bin Count'
            dupe_count = int(row[_prospect_count]) - int(row[_recycle_bin_count])

            # If dupe_count = 2, three possible variations exist: (both are synced with sf) (neither are synced with sf) (only one is synced with sf)
            if  dupe_count == 2:
                matches = []
                
                for p in pardot_prospects:
                    if pardot_prospects[p].email.lower() == row[_email].lower():
                        matches.append(pardot_prospects[p])

                if len(matches) != 2:
                    print(row[_email], 'found: ' + str(len(matches)))
                    exit()
                
                m1_matched = len(matches[0].crmid) == 18 and '@' not in matches[0].crmid
                m2_matched = len(matches[1].crmid) == 18 and '@' not in matches[1].crmid

                both_synced = m1_matched and m2_matched
                none_synced = not m1_matched and not m2_matched

                if both_synced:
                    results['both_synced'].append([row[_email]])
                elif none_synced:
                    results['none_synced'].append([row[_email]])
                else:
                    results['one_synced'].append([row[_email]])

            # TODO: If dupe_count = 1, check if crm_id value is email. if so sync contact with correct crm_id

        oneSynced = open('one_synced.csv', 'w')
        writer1 = csv.writer(oneSynced)
        writer1.writerow(['Email'])
        writer1.writerows(results['one_synced'])
        oneSynced.close()

        bothSynced = open('both_synced.csv', 'w')
        writer2 = csv.writer(bothSynced)
        writer2.writerow(['Email'])
        writer2.writerows(results['both_synced'])
        bothSynced.close()

        noneSynced = open('none_synced.csv', 'w')
        writer3 = csv.writer(noneSynced)
        writer3.writerow(['Email'])
        writer3.writerows(results['none_synced'])
        noneSynced.close()

        print(len(results['both_synced']), len(results['none_synced']), len(results['one_synced']))

def contacts_exist(inf):
    crm_contacts = init_crm_contacts()
    crm_emails = set()
    rows = 0
    matches = 0

    for c in crm_contacts:
        if crm_contacts[c].email != '':
            crm_emails.add(crm_contacts[c].email.lower())

        if crm_contacts[c].primemail != '':
            crm_emails.add(crm_contacts[c].primemail.lower())

    with open(inf, 'r', encoding='utf-8-sig') as _inf_:
        reader = csv.reader(_inf_)
        headers = [e.lower() for e in next(reader)]
        contacts = set()

        try:
            email = headers.index('email')
        except ValueError:
            print('Error: No Email Column Found')
            exit()

        for row in reader:
            contact = row[email]

            if contact in contacts:
                continue

            rows = rows + 1
            contacts.add(contact)

            if row[email].lower() in crm_emails:
                matches = matches + 1
            else:
                print(row[email])

    print('Rows:', rows)
    print('Matches:', matches)

def gds_deep_links():
    target_file = open('mpse_processed.csv', 'w')
    columns = [
        'Email',
        'gds4_sso_pse_label_1',
        'gds4_sso_pse_link_1',
        'gds4_sso_pse_label_2',
        'gds4_sso_pse_link_2',
        'gds4_sso_pse_label_3',
        'gds4_sso_pse_link_3',
        'gds4_sso_pse_label_4',
        'gds4_sso_pse_link_4',
        'gds4_sso_pse_label_5',
        'gds4_sso_pse_link_5'
    ]

    writer = csv.writer(target_file)
    writer.writerow(columns)

    with open('gds4_deep_link_mpse.csv', 'r', encoding='utf-8-sig') as inf:
        reader = csv.reader(inf)
        next(reader) # skip first row
        
        col_email = 9
        col_desc = [53,28]
        col_link = 57

        target_list = {}

        for row in reader:
            email = row[col_email].lower()
            desc = row[col_desc[0]] + ' - Model: ' + row[col_desc[1]] + ' | '
            link = row[col_link]

            if email not in target_list:
                target_list[email] = [(desc, link)]
            else:        
                target_list[email].append((desc, link))

        for target in target_list:
            email = target
            desc = target_list[target][0][0]
            link = target_list[target][0][1]

            padding = 5 - len(target_list[target])
            data = [target]

            for event in target_list[target]:
                data.append(event[0])
                data.append(event[1])

            for x in range(padding):
                data.append('')

            writer.writerow(data)
                
    target_file.close()


run_dedupes()
# gds_deep_links()
# contacts_exist('mpse_processed.csv')