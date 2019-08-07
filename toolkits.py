# -*- coding: utf-8 -*-
# @Time    : 2019/8/6 18:57
# @Author  : qiyibaba
# @Site    : 
# @File    : mysql_toolkits.py

import mysql.connector

config = {
    'user': 'root',
    'password': 'zte',
    'host': '127.0.0.1',
    'database': 'lttest',
    'charset': 'utf8',
    "connection_timeout": 5,
    "use_pure": True
}


def mysql_conn(sql):
    items = []

    cnx = mysql.connector.connect(**config)  # 建立连接
    cursor = cnx.cursor(dictionary=True)

    cursor.execute(sql)
    for item in cursor.fetchall():
        items.append(item)

    cursor.close()
    cnx.close()

    return items


def hit1():
    sql = "select t1.variable_value,t2.variable_value as hit_rate from performance_schema.global_status t1 join performance_schema.global_status t2" \
          " where t1.variable_name = 'innodb_buffer_pool_reads' and t2.variable_name = 'innodb_buffer_pool_read_requests'"
    v = mysql_conn(sql)
    print(v)


def hit2():
    sql = "select group_concat(hit_rate order by hit_rate separator ' ') as hit from information_schema.innodb_buffer_pool_stats"
    v = mysql_conn(sql)
    total = 0
    index = 0
    for s in v[0]['hit']:
        total += s
        index += 1
    if total > 0:
        print("[hit rate][AVG]{}[LIST]{}".format(total / index, s))
    else:
        print('free')


if __name__ == '__main__':
    hit2()
