from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def sql_engine(source_db='postgresql',
               username='postgres',
               pwd='sunweiyao',
               ip='localhost',
               port=5432,
               db='quant'):

    if source_db == 'postgresql':
        source_db_str = 'postgresql+psycopg2'

    engine_str = '{}://{}:{}@{}:{}/{}'
    username = str(username)
    pwd = str(pwd)
    ip = str(ip)
    port = str(port)
    db = str(db)
    engine_str = engine_str.format(source_db_str, username, pwd, ip, port, db)
    return create_engine(engine_str)

def ex_task(sql_engine, sql_str):
    Con = sessionmaker(bind=sql_engine)
    con = Con()
    con.execute(sql_str)
    con.commit()
    con.close()
    print('Done.')
    return