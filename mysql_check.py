# -*- coding: utf-8 -*-
# @Time    : 2019/8/9 16:59
# @Author  : qiyibaba
# @Site    : 对MySQL的健康性检查
# @File    : mysql_check.py
import mysql.connector

cc = {
    'user': 'root',
    'password': 'zte',
    'host': '127.0.0.1',
    'database': 'lttest',
    'charset': 'utf8',
    "connection_timeout": 5,
    "use_pure": True
}


def run_sql(sql):
    cnx = mysql.connector.connect(**cc)  # 建立连接
    cursor = cnx.cursor(dictionary=True)
    cursor.execute(sql)
    records = cursor.fetchall()
    cursor.close()
    cnx.close()
    return records


def f_print_title():
    info = '*' * 100 + "\n"
    info += '* {:^96} *\n'.format('MySQL Check Tools v1.0 (by qiyibaba)')
    info += '*' * 100 + "\n\n+----------------------+------------+------------+------------+\n"
    info += '+{:^22}+{:^12}+{:^12}+{:^12}+\n'.format('server_ip', 'user_name', 'db_name', 'db_version')
    info += "+----------------------+------------+------------+------------+\n"
    info += '+{:^22}+{:^12}+{:^12}+{:^12}+\n'.format(cc['host'], cc['user'], 'sys', 'Mysql 5.7')
    info += "+----------------------+------------+------------+------------+\n"
    print(info)


def f_print_index_info():
    s_sql = "select * from sys.schema_index_statistics"
    r_sql = "select * from sys.schema_redundant_indexes"
    u_sql = "select * from sys.schema_unused_indexes"

    print('=== {:^50} ==='.format('冗余索引'))
    f = '+{:^20}+{:^20}+{:^20}+{:^80}+'
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 80))
    # print(f.format('库名', '表名', '冗余索引名', '删除索引的语句'))
    print(f.format('table_schema', 'table_name', 'redundant_index_name', 'sql_drop_index'))
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 80))
    for row in run_sql(r_sql):
        print(f.format(row['table_schema'], row['table_name'], row['redundant_index_name'], row['sql_drop_index']))
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 80))

    print()
    print('=== {:^50} ==='.format('无用索引'))
    f = '+{:^20}+{:^20}+{:^20}+'
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 80))
    # print(f.format('库名', '表名', '没有使用过的索引名'))
    print(f.format('object_schema', 'object_name', 'index_name'))
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 80))
    for row in run_sql(u_sql):
        print(f.format(row['object_schema'], row['object_name'], row['index_name']))
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 80))


def f_print_auto_increment():
    # • table_schema 库名
    # • table_name 表名
    # • column_name 自增列名
    # • data_type 列的数据类型
    # • column_type 列的类型的其他属性，如int(10) unsigned，而data_type只是int
    # • is_signed 是否signed，0否1是
    # • is_unsigned 是否unsigned，0否1是
    # • max_value 该列允许的最大值
    # • auto_increment 即将插入的AUTO_INCREMENT值
    # • auto_increment_ratio 这个列已经使用的值的比例
    sql = "select * from sys.schema_auto_increment_columns order by auto_increment_ratio desc limit 5"
    print('\n=== {:^50} ==='.format('自增列使用情况'))
    f = '+{:^20}+{:^20}+{:^20}+{:^20}+{:^20}+{:^20}+{:^20}+'
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20))
    print(f.format('table_schema', 'table_name', 'column_name', 'data_type', 'max_value', 'auto_increment', 'auto_increment_ratio'))
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20))
    for row in run_sql(sql):
        print(f.format(row['table_schema'], row['table_name'], row['column_name'], row['data_type'], row['max_value'], row['auto_increment'], row['auto_increment_ratio']))
    print(f.format('-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20, '-' * 20))


def f_sql_analysis():
    sql = "select db,exec_count,query from sys.statement_analysis order by exec_count desc limit 10"
    print('\n=== {:^50} ==='.format('sql执行情况分析'))
    f = '+ {:<18} + {:<18} + {:<78} +'
    print(f.format('-' * 18, '-' * 18, '-' * 78))
    print('+{:^20}+{:^20}+{:^80}+'.format('db', 'exec_count', 'query'))
    print(f.format('-' * 18, '-' * 18, '-' * 78))
    for row in run_sql(sql):
        print(f.format(row['db'], row['exec_count'], row['query']))
    print(f.format('-' * 18, '-' * 18, '-' * 78))

    # 全表扫描
    # • query 执行的语句
    # • db 语句所在的数据库
    # • exec_count 语句执行次数
    # • total_latency 发生全表扫描事件的语句总的等待时间
    # • no_index_used_count 扫描时没有索引的次数
    # • no_good_index_used_count 扫描时没有用好的索引的次数，good index？
    # • no_index_used_pct 扫描时没有用索引的比例
    # • rows_sent 表返回的行数
    # • rows_examined 从存储引擎层read的行数
    # • rows_sent_avg 表返回的平均行数
    # • rows_examined_avg 从存储引擎层read的行数
    # • first_seen 语句第一次执行时间
    # • last_seen 语句最后一次执行时间
    # • digest 语句digest
    print('\n=== {:^50} ==='.format('sql全表扫描'))
    f = '+ {:<78} + {:<18} + {:<18} + {:<18} +'
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))
    print('+{:^80}+{:^20}+{:^20}+'.format('query', 'exec_count', 'no_index_used_count', 'no_index_used_pct'))
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))
    for row in run_sql('select * from sys.statements_with_full_table_scans'):
        print(f.format(row['query'], row['exec_count'], row['no_index_used_count'], row['no_index_used_pct']))
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))

    # 排序操作
    # • query 执行的语句
    # • db 语句所在的数据库，没有的话则为NULL。
    # • exec_count 执行次数
    # • total_latency 总的等待时间
    # • sort_merge_passes sort merge passes总数目
    # • avg_sort_merges 平均sort merge passes数目
    # • sorts_using_scans 使用表扫描的排序次数
    # • sort_using_range 使用范围查询的排序次数
    # • rows_sorted 总的排序行数
    # • avg_rows_sorted 平均排序行数
    # • first_seen 语句第一次被发现事件
    # • last_seen 语句最后一次呗发现时间
    # • digest 语句digest
    print('\n=== {:^50} ==='.format('sql排序操作'))
    f = '+ {:<78} + {:<18} + {:<18} + {:<18} +'
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))
    print('+{:^80}+{:^20}+{:^20}+{:^20}+'.format('query', 'exec_count', 'sorts_using_scans', 'sort_using_range'))
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))
    for row in run_sql('select * from sys.statements_with_sorting order by exec_count desc limit 10'):
        print(f.format(row['query'], row['exec_count'], row['sorts_using_scans'], row['sort_using_range']))
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))

    # 哪些SQL使用了临时表，又有哪些SQL用到了磁盘临时表
    print('\n=== {:^50} ==='.format('哪些SQL使用了临时表，又有哪些SQL用到了磁盘临时表'))
    f = '+ {:<78} + {:<18} + {:<18} + {:<18} +'
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))
    print('+{:^80}+{:^20}+{:^20}+{:^20}+'.format('query', 'db', 'tmp_tables', 'tmp_disk_tables'))
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))
    for row in run_sql(
            'select query,db,tmp_tables,tmp_disk_tables from sys.statement_analysis where tmp_tables>0 or tmp_disk_tables >0 order by (tmp_tables+tmp_disk_tables) desc limit 10'):
        print(f.format(row['query'], row['db'], row['tmp_tables'], row['tmp_disk_tables']))
    print(f.format('-' * 78, '-' * 18, '-' * 18, '-' * 18))


if __name__ == '__main__':
    f_print_title()
    # 索引使用情况如何，有哪些冗余索引和无用索引
    f_print_index_info()
    # 自增长字段的最大值和当前已经使用到的值
    f_print_auto_increment()
    # 数据库中哪些SQL被频繁执行
    f_sql_analysis()
