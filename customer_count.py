import os
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


def get_customer_ucount():
    # TODO Write your code here
    pass
