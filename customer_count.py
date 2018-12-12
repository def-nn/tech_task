import os

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, func


db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'customers.db')
db_engine = create_engine('sqlite:///%s' % db_path)

metadata = MetaData()

loyalty_program_customers = Table('loyalty_program_customers', metadata,
                                  Column('id', Integer, primary_key=True),
                                  Column('loyalty_card_number', String),
                                  Column('email', String),
                                  Column('phone', String),
                                  Column('pib', String)
                                  )

website_users = Table('website_users', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('email', String),
                      Column('phone', String),
                      Column('pib', String)
                      )

subscribers = Table('subscribers', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('email', String),
                    )

Base = automap_base(metadata=metadata)
Base.prepare()

LPCustomer, WebsiteUser, Subscribers = Base.classes.loyalty_program_customers, Base.classes.website_users, \
                                       Base.classes.subscribers


char_map = {
    'а': 'a',
    'б': 'b',
    'в': 'v',
    'г': 'g',
    'д': 'd',
    'е': 'e',
    'ё': 'e',
    'ж': 'zh',
    'з': 'z',
    'и': 'i',
    'й': 'y',
    'к': 'k',
    'л': 'l',
    'м': 'm',
    'н': 'n',
    'о': 'o',
    'п': 'p',
    'р': 'r',
    'с': 's',
    'т': 't',
    'у': 'u',
    'ф': 'f',
    'х': 'h',
    'ц': 'c',
    'ч': 'ch',
    'ш': 'sch',
    'щ': 'sch',
    'ъ': '',
    'ы': 'y',
    'ь': '',
    'э': 'e',
    'ю': 'yu',
    'я': 'ya',
    'і': 'i',
    'ї': 'yi',
    'є': 'e',

    'q': 'k',
    'w': 'w',
    'e': 'e',
    'r': 'r',
    't': 't',
    'y': 'y',
    'u': 'u',
    'i': 'i',
    'o': 'o',
    'p': 'p',
    'a': 'a',
    's': 's',
    'd': 'd',
    'f': 'f',
    'g': 'g',
    'h': 'h',
    'j': 'j',
    'k': 'k',
    'l': 'l',
    'z': 'z',
    'c': 'c',
    'v': 'v',
    'b': 'b',
    'n': 'n',
    'm': 'm',
    'x': 'ks'
}


def normalize_pib(pib, ordered=False):
    pib_normalized = []

    for word in pib.split():
        if len(word) > 2:
            pib_normalized.append(''.join([char_map[c] for c in word if c.isalpha()]))

    if ordered:
        pib_normalized.sort()

    return ' '.join(pib_normalized)


card_numbers_dpc = {}
phones_dpc = {}
emails_dpc = {}


def inspect_dpc(dpc, current_key):
    for spaces in dpc[current_key]:
        for item in dpc[current_key][spaces]:
            exec("sub_space = {}_dpc".format(item))


def get_customer_ucount():
    Session = sessionmaker(bind=db_engine)
    session = Session()

    lp_customers = session.query(
        LPCustomer.email,
        LPCustomer.loyalty_card_number,
        LPCustomer.phone,
        LPCustomer.pib
    ).order_by(LPCustomer.loyalty_card_number)

    website_users = session.query(
        WebsiteUser.email,
        WebsiteUser.phone,
        WebsiteUser.pib
    )

    subscribers = session.query(
        Subscribers.email
    ).distinct(Subscribers.email)

    processed_emails = set()
    processed_phones = set()
    processed_pib = set()

    ##########################################################################################

    # card_numbers_unique = {}
    # multiple_cards = set()
    #
    # for row in lp_customers:
    #     email, card_number, phone, pib = row
    #
    #     if card_number not in card_numbers_unique:
    #         card_numbers_unique[card_number] = {
    #             'emails': [],
    #             'phones': [],
    #             'pib': [],
    #         }
    #     else:
    #         multiple_cards.add(card_number)
    #
    #     card_numbers_unique[card_number]['emails'].append(email)
    #     card_numbers_unique[card_number]['phones'].append(phone)
    #     card_numbers_unique[card_number]['pib'].append(normalize_pib(pib, ordered=True))
    #
    # multiple_cards = list(multiple_cards)
    # multiple_cards.sort()
    #
    # for card_number in multiple_cards:
    #     print(card_number)
    #
    #     print("   emails:")
    #     for each in card_numbers_unique[card_number]['emails']:
    #         print("      {}".format(each))
    #
    #     print("   phones:")
    #     for each in card_numbers_unique[card_number]['phones']:
    #         print("      {}".format(each))
    #
    #     print("   pib:")
    #     for each in card_numbers_unique[card_number]['pib']:
    #         print("      {}".format(each))
    #
    #     print()

    ##########################################################################################

    # res = session.query(
    #     func.count(LPCustomer.id).label('cnt'),
    #     LPCustomer.loyalty_card_number,
    # ).group_by(LPCustomer.loyalty_card_number).order_by('-cnt', LPCustomer.loyalty_card_number)
    #
    # for row in res:
    #     if row[0] == 1: break
    #     print(row)

    lp_cursor = session.query(LPCustomer.email, LPCustomer.phone)
    website_cursor = session.query(WebsiteUser.email, WebsiteUser.phone)
    subscribers_cursor = session.query(Subscribers.email).distinct(Subscribers.email)

    proccesed_emails = set()
    distinct_users_data = set()

    for row in lp_cursor.union(website_cursor):
        distinct_users_data.add("{}_{}".format(row[0], row[1]))
        proccesed_emails.add(row[0])

    subscribers_unique = {row[0] for row in subscribers_cursor if row[0] not in proccesed_emails}

    return len(distinct_users_data) + len(subscribers_unique)

    data = session.query(
        LPCustomer.email,
        LPCustomer.phone,
        LPCustomer.pib,
        LPCustomer.loyalty_card_number
    )

    website_users_data_distinct = session.query(
        WebsiteUser.email,
        WebsiteUser.phone,
        WebsiteUser.pib,
        WebsiteUser.id
    )

    data = data.union(website_users_data_distinct)
    data_distinct = {
        "{:<25}{:<15} {:<45} {:<17}".format(
            row[0],
            row[1],
            normalize_pib(row[2], True),
            row[3],
        ) for row in data
    }

    data_distinct = list(data_distinct)
    data_distinct.sort()

    processed_emails = set()

    i = 1
    for each in data_distinct:
        email_base = each[:25].strip()

        processed_emails.add(email_base)

        print("{:<5}{}".format(i, each))
        i += 1

    # for row in subscribers:
    #     email_base = row[0][:12]
    #
    #     if email_base not in processed_emails:
    #         i += 1
    #         print(i, email_base)

    return i

    processed_phones = {}
    processed_card_numbers = {}

    merged_phones = {}
    merged_emails = {}

    n_distinct = 0

    for row in lp_customers:
        email, card_number, phone, pib = row

        pib = normalize_pib(pib, ordered=True)

        if phone not in processed_phones:
            processed_phones[phone] = {
                'emails': set(),
                'cn': set(),
                'pib': set(),
                'data': []
            }
            n_distinct += 1

        processed_phones[phone]['emails'].add(email)
        processed_phones[phone]['cn'].add(card_number)
        processed_phones[phone]['pib'].add(' '.join(pib))
        processed_phones[phone]['data'].append((email, card_number, pib))

        processed_emails.add(email)

    for row in website_users:
        email, phone, pib = row

        pib = normalize_pib(pib, ordered=True)

        if phone not in processed_phones:
            processed_phones[phone] = {
                'pib': set(),
                'emails': set(),
                'data': []
            }

        processed_phones[phone]['emails'].add(email)
        processed_phones[phone]['pib'].add(' '.join(pib))
        processed_phones[phone]['data'].append((email, card_number, pib))

        processed_emails.add(email)

    i = 0
    n = 0
    for row in subscribers:
        email = row[0]

        print(n, email)
        n += 1

        if email not in processed_emails:
            i += 1


    return len(processed_phones), i


    # for key in processed_card_numbers:
    #     print(key)
    #
    #     for data in processed_card_numbers[key]['data']:
    #         print("   {}".format(data))
    #
    #     # for each in processed_card_numbers[key]:
    #     #     print('   {}:'.format(each))
    #     #     l = list(processed_card_numbers[key][each])
    #     #     l.sort()
    #     #     for item in l:
    #     #         print('      {}'.format(item))
    #     print('-------------------------')
    #
    # return len(processed_card_numbers)

    for row in lp_customers:
        email, card_number, phone, pib = row

        if email not in processed_emails and phone not in processed_phones:
            n_distinct += 1

        processed_emails.add(email)
        processed_phones.add(phone)

    for row in website_users:
        email, phone, pib = row

        if email not in processed_emails and phone not in processed_phones:
            n_distinct += 1

        processed_emails.add(email)
        processed_phones.add(phone)

    for row in subscribers:
        email = row[0]

        if email not in processed_emails:
            n_distinct += 1

        processed_emails.add(email)

    return n_distinct

    for row in lp_customers:
        email, card_number, phone, pib = row

        if email not in processed_emails and phone not in processed_phones:
            n_distinct += 1

        processed_emails.add(email)
        processed_phones.add(phone)

    for i, email in enumerate(processed_emails):
        print(i, email)
    print()
    for i, email in enumerate(processed_phones):
        print(i, email)

    return n_distinct

    customers_unique = {}
    processed_emails = set()
    processed_phones = set()
    n = 0
    n_distinct = 0

    for row in lp_customers:
        pib = row[3]

    for row in lp_customers:
        email, card_number, phone, pib = row

        if card_number not in customers_unique.keys():
            customers_unique[card_number] = {
                'emails': set(),
                'phones': set(),
                'pib': set()
            }
            n_distinct += 1

        # customers_unique[phone]['cn'].add(card_number)
        customers_unique[card_number]['emails'].add(email)
        customers_unique[card_number]['phones'].add(phone)
        customers_unique[card_number]['pib'].add(pib)

        processed_emails.add(email)
        processed_phones.add(phone)

        n += 1

    for row in website_users:
        email, card_number, pib = row

        if email not in processed_emails and phone not in processed_phones:
            n_distinct += 1
            processed_phones.add(phone)
            processed_emails.add(email)

        # if phone not in customers_unique:
        #     customers_unique[phone] = {
        #         'emails': set(),
        #         # 'phones': set(),
        #         'pib': set()
        #     }
        #     n_distinct += 1
        #
        # customers_unique[phone]['emails'].add(email)
        # # customers_unique[email]['phones'].add(phone)
        # customers_unique[phone]['pib'].add(pib)
        # processed_emails.add(email)

        n += 1

    # for row in subscribers:
    #     email = row[0]
    #     email = email[:12]
    #
    #     n += 1
    #
    #     if email not in processed_emails:
    #         n_distinct += 1


    # for key in customers_unique:
        # n_distinct += min(len(customers_unique[key]['emails']), len(customers_unique[key]['phones']))
        # n_distinct += len(customers_unique[key]['emails'])

    for key in customers_unique:
        print(key)
        for each in customers_unique[key]:
            print('   {} => {}'.format(each, customers_unique[key][each]))
        print('-------------------------')
        n_distinct += 1
    #
    # for each in customers_unique:
    #     print(each, customers_unique[each])
    #
    # for row in website_users:
    #     uid = "{}-{}".format(row[0], row[1])
    #
    #     if uid not in customers_unique:
    #         customers_unique[uid] = []
    #
    #         customers_unique[uid].append(row[2].split())
    #     n += 1
    #
    # for each in customers_unique:
    #     print(each, customers_unique[each])
    #
    # for row in session.query(Subscribers.email).distinct(Subscribers.email):
    #     customers_unique[row[0]] = ''
    #     print(row)
    #     n += 1

    print(n_distinct, n)

    return 0
