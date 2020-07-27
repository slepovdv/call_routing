#!/usr/bin/env python3

import sys
import logging
import pymysql.cursors
from asterisk.agi import AGI

logging.basicConfig(stream=sys.stdout, filemode='a', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s\n')

# Database connection option
sql_server = '10.0.0.2'
sql_port = 3306
sql_db = 'asterisk'
sql_user = 'asterisk'
sql_password = 'asterisk'

connection = pymysql.connect(host=sql_server, port=sql_port, user=sql_user,
                             password=sql_password, db=sql_db,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

logging.info('Connect database')

agi = AGI()

caller = agi.env['agi_callerid']
logging.info(f'AGI callerid is {caller}')

try:
    with connection.cursor() as cursor:
        sql: str = f"SELECT *, DATE_FORMAT(cdr.calldate, '%Y-%m-%d') FROM cdr WHERE (DATE(calldate) = CURDATE() or " \
                   f"DATE(calldate) = DATE_ADD(CURDATE(), INTERVAL -1 DAY)) and (src={caller} or dst={caller})"
        cursor.execute(sql)
        result = cursor.fetchall()
        logging.info(f'sql -> {result}')
        number_of_rows = len(result)
        logging.info(f'Number of rows {caller} is {number_of_rows}')

        if number_of_rows != 0:
            agi.appexec('Goto', 'bike')
            logging.info(f'{caller} go to bike priority in dialplan')
        else:
            logging.info(f'This number {caller} is first call')
finally:
    connection.close()

logging.info(f'Database connection {caller} close')
