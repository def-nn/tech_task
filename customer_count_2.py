import os

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, func
from sqlalchemy.sql import literal_column

from tools import normalize_pib, calculate_pib_similarity


SIMILARITY_THRESHOLD = 2 + 2/3


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

user_instances = Table('user_instances', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('email', String),
                       Column('phone', String),
                       Column('card_number', String),
                       Column('pib', String)
                       )


Base = automap_base(metadata=metadata)
Base.prepare()

LPCustomer, WebsiteUser, Subscribers, UserInstance = Base.classes.loyalty_program_customers, \
                                                     Base.classes.website_users,  Base.classes.subscribers, \
                                                     Base.classes.user_instances


def generate_user_instances(session, instances):
    session.query(UserInstance).delete()

    lp_customers_cursor = session.query(
        LPCustomer.email.label('user_email'),
        LPCustomer.phone.label('user_phone'),
        LPCustomer.pib.label('user_pib'),
        LPCustomer.loyalty_card_number.label('user_card_number')
    )

    website_users_cursor = session.query(
        WebsiteUser.email.label('user_email'),
        WebsiteUser.phone.label('user_phone'),
        WebsiteUser.pib.label('user_pib'),
        literal_column('""').label('user_card_number')
    )

    cursor = lp_customers_cursor.union(
        website_users_cursor
    ).order_by(
        'user_email',
        'user_phone',
        'user_pib',
        'user_card_number',
    )

    _id = 0
    for row in cursor:
        user_instance = UserInstance(
            id=_id,
            card_number=row[3],
            email=row[0],
            phone=row[1],
            pib=normalize_pib(row[2], True).strip()
        )
        session.add(user_instance)
        instances[_id] = user_instance

        _id += 1

    session.commit()


def group_rows(similarity_map, _id, current_group):
    for each in similarity_map[_id]:
        if each in current_group:
            continue
        current_group.add(each)
        group_rows(similarity_map, each, current_group)


def get_customer_ucount():
    Session = sessionmaker(bind=db_engine)
    session = Session()

    unique_clients_count = 0
    processed_emails = set()

    instances = {}
    generate_user_instances(session, instances)

    similarity_map = {}

    for idx in instances:
        instance = instances[idx]
        similarity_map[instance.id] = set()

        processed_emails.add(instance.email)

        # Calculate similarity
        cursor = session.query(
            UserInstance.id,
            UserInstance.email,
            UserInstance.phone,
            UserInstance.card_number,
            UserInstance.pib
        )

        for row in cursor:
            _id, email, phone, card_number, pib = row

            if instance.id == _id:
                continue

            sim = 0

            if instance.email == email:
                sim += 1

            if instance.phone == phone:
                sim += 1

            if (instance.card_number or card_number) and \
                    ((not instance.card_number or not card_number) or instance.card_number == card_number):
                sim += 1

            if not pib or not instance.pib or pib == instance.pib:
                sim += 1
            else:
                sim += calculate_pib_similarity(instance.pib.split(), pib.split())

            if sim >= SIMILARITY_THRESHOLD:
                similarity_map[instance.id].add(_id)

    processed_idx = set()

    for _id in similarity_map:
        if _id in processed_idx:
            continue

        current_group = {_id}
        group_rows(similarity_map, _id, current_group)

        processed_idx.update(current_group)

        # PRINT MERGED GROUPS TO CONSOLE
        # COMMENT THIS LINES FOR SPEED-UP
        ############################################################################################################

        ordered_group = list(current_group)
        ordered_group.sort()

        print('---------------------------------------------------------------------------------------------------')
        for _id in ordered_group:
            print("{:<25}{:<15} {:<40} {:<17}".format(
                instances[_id].email,
                instances[_id].phone,
                instances[_id].pib,
                instances[_id].card_number)
            )
        print('---------------------------------------------------------------------------------------------------')

        ############################################################################################################

        unique_clients_count += 1

    unique_clients_count += session.query(
        Subscribers.email
    ).distinct(
        Subscribers.email
    ).filter(
        ~Subscribers.email.in_(processed_emails)
    ).count()

    return unique_clients_count
