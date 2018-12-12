import os

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String


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


def get_customer_ucount():
    Session = sessionmaker(bind=db_engine)
    session = Session()

    lp_cursor = session.query(
        LPCustomer.email,
        LPCustomer.phone
    )

    website_cursor = session.query(
        WebsiteUser.email,
        WebsiteUser.phone
    )

    distinct_users_data = set()
    procesed_emails = set()

    # Retrieve all unique pairs of clients email and phone
    for row in lp_cursor.union(website_cursor):
        distinct_users_data.add("{}_{}".format(row[0], row[1]))
        # Keep tracking for all processed emails
        procesed_emails.add(row[0])

    # Count how many emails haven't been took into account
    unprocessed_emails_cnt = session.query(
        Subscribers.email
    ).distinct(
        Subscribers.email
    ).filter(
        ~Subscribers.email.in_(procesed_emails)
    ).count()

    return len(distinct_users_data) + unprocessed_emails_cnt
