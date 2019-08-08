# -*- coding: utf-8 -*-
# @Time    : 2019/8/8 9:47
# @Author  : qiyibaba
# @Site    : 
# @File    : mysql_tuning.py

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import mysql.connector
import explain
import datetime

op = {
    'sys_param': 'ON',
    'sql_plan': 'ON',
    'obj_stat': 'ON',
    'ses_status': 'ON',
    'sql_profile': 'ON'
}

cc = {
    'user': 'root',
    'password': 'zte',
    'host': '127.0.0.1',
    'database': 'lttest',
    'charset': 'utf8',
    "connection_timeout": 5,
    "use_pure": True
}

SYS_PARM_FILTER = (
    'BINLOG_CACHE_SIZE',
    'BULK_INSERT_BUFFER_SIZE',
    'HAVE_PARTITION_ENGINE',
    'HAVE_QUERY_CACHE',
    'INTERACTIVE_TIMEOUT',
    'JOIN_BUFFER_SIZE',
    'KEY_BUFFER_SIZE',
    'KEY_CACHE_AGE_THRESHOLD',
    'KEY_CACHE_BLOCK_SIZE',
    'KEY_CACHE_DIVISION_LIMIT',
    'LARGE_PAGES',
    'LOCKED_IN_MEMORY',
    'LONG_QUERY_TIME',
    'MAX_ALLOWED_PACKET',
    'MAX_BINLOG_CACHE_SIZE',
    'MAX_BINLOG_SIZE',
    'MAX_CONNECT_ERRORS',
    'MAX_CONNECTIONS',
    'MAX_JOIN_SIZE',
    'MAX_LENGTH_FOR_SORT_DATA',
    'MAX_SEEKS_FOR_KEY',
    'MAX_SORT_LENGTH',
    'MAX_TMP_TABLES',
    'MAX_USER_CONNECTIONS',
    'OPTIMIZER_PRUNE_LEVEL',
    'OPTIMIZER_SEARCH_DEPTH',
    'QUERY_CACHE_SIZE',
    'QUERY_CACHE_TYPE',
    'QUERY_PREALLOC_SIZE',
    'RANGE_ALLOC_BLOCK_SIZE',
    'READ_BUFFER_SIZE',
    'READ_RND_BUFFER_SIZE',
    'SORT_BUFFER_SIZE',
    # 'SQL_MODE',
    'TABLE_CACHE',
    'THREAD_CACHE_SIZE',
    'TMP_TABLE_SIZE',
    'WAIT_TIMEOUT'
)


def run_sql(sql):
    cnx = mysql.connector.connect(**cc)  # 建立连接
    cursor = cnx.cursor(dictionary=True)
    cursor.execute(sql)
    records = cursor.fetchall()
    cursor.close()
    cnx.close()
    return records


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        # print item.ttype,item.value
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_real_name()
        elif isinstance(item, Identifier):
            yield item.get_real_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value


def f_find_in_list(my_list, value):
    for v in range(0, len(my_list)):
        if value == my_list[v]:
            return 1
    return 0


def f_print_parm():
    sql = "select lower(variable_name) as v_key,variable_value from PERFORMANCE_SCHEMA.GLOBAL_VARIABLES where upper(variable_name) in ('" + "','".join(
        list(SYS_PARM_FILTER)) + "') order by variable_name"
    records = run_sql(sql)
    info = "===== SYSTEM PARAMETER =====\n"
    info += "+--------------------------------+------------------------------------------------------------+\n"
    info += "+{:^32}+{:^60}+\n".format('parameter_name', 'value')
    info += "+--------------------------------+------------------------------------------------------------+\n"

    for row in records:
        info += "+{:32}".format(row['v_key'])
        if 'size' in row['v_key']:
            if int(row['variable_value']) >= 1024 * 1024 * 1024:
                info += "+{:>59} +\n".format(str(round(int(row['variable_value']) / 1024 / 1024 / 1024, 2)) + ' G')
            elif int(row['variable_value']) >= 1024 * 1024:
                info += "+{:>59} +\n".format(str(round(int(row['variable_value']) / 1024 / 1024, 2)) + ' M')
            elif int(row['variable_value']) >= 1024:
                info += "+{:>59} +\n".format(str(round(int(row['variable_value']) / 1024, 2)) + ' K')
            else:
                info += "+{:>59} +\n".format(row['variable_value'] + ' B')
        else:
            info += "+{:>59} +\n".format(row['variable_value'])
    info += "+--------------------------------+------------------------------------------------------------+\n"
    print(info)


def f_print_optimizer_switch():
    sql = "select variable_value from PERFORMANCE_SCHEMA.GLOBAL_VARIABLES where upper(variable_name)='OPTIMIZER_SWITCH'"
    records = run_sql(sql)

    info = "===== OPTIMIZER SWITCH =====\n+------------------------------------------+------------+\n"
    info += "+{:^42}+{:^12}+\n".format('switch_name', 'value')
    info += "+------------------------------------------+------------+\n"

    for row in records[0]['variable_value'].split(','):
        info += "+ {:<41}+{:>11} +\n".format(row.split('=')[0], row.split('=')[1])
    info += "+------------------------------------------+------------+\n"
    print(info)


def f_exec_sql(p_sqltext, p_option):
    results = {}
    # 不添加buffered=True抛出异常mysql.connector.errors.InternalError: Unread result found
    cnx = mysql.connector.connect(**cc, buffered=True)  # 建立连接
    cursor = cnx.cursor(dictionary=True)
    query_id = 0

    if f_find_in_list(p_option, 'PROFILING'):
        cursor.execute("set profiling=1")
        cursor.execute("select ifnull(max(query_id),0) as query_id from INFORMATION_SCHEMA.PROFILING")
        records = cursor.fetchall()
        query_id = records[0]['query_id'] + 2

    s_sql = "select concat(upper(left(variable_name,1)),substring(lower(variable_name),2,(length(variable_name)-1))) var_name,variable_value var_value" \
            " from PERFORMANCE_SCHEMA.SESSION_STATUS order by 1"
    if f_find_in_list(p_option, 'STATUS'):
        cursor.execute(s_sql)
        results['BEFORE_STATUS'] = cursor.fetchall()

    cursor.execute(p_sqltext)

    if f_find_in_list(p_option, 'STATUS'):
        cursor.execute(s_sql)
        results['AFTER_STATUS'] = cursor.fetchall()

    if f_find_in_list(p_option, 'PROFILING'):
        cursor.execute(
            "select STATE,DURATION,CPU_USER,CPU_SYSTEM,BLOCK_OPS_IN,BLOCK_OPS_OUT ,MESSAGES_SENT ,MESSAGES_RECEIVED ,PAGE_FAULTS_MAJOR ,"
            "PAGE_FAULTS_MINOR ,SWAPS from INFORMATION_SCHEMA.PROFILING where query_id=" + str(query_id) + " order by seq")
        records = cursor.fetchall()
        results['PROFILING_DETAIL'] = records

        cursor.execute(
            "SELECT STATE as state,SUM(DURATION) AS total_r,ROUND(100*SUM(DURATION)/(SELECT SUM(DURATION) FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID=" + str(
                query_id) + "),2) AS pct_r,COUNT(*) AS calls,SUM(DURATION)/COUNT(*) AS r_call FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID=" + str(
                query_id) + " GROUP BY STATE ORDER BY Total_R DESC")
        records = cursor.fetchall()
        results['PROFILING_SUMMARY'] = records

    cursor.close()
    cnx.close()
    return results


def f_print_status(p_before_status, p_after_status):
    info = "===== SESSION STATUS (DIFFERENT) =====\n"
    f = '+{:^37}+{:^17}+{:^17}+{:^17}+\n'
    info += "+-------------------------------------+-----------------+-----------------+-----------------+\n"
    info += f.format('status_name', 'before', 'after', 'diff')
    info += "+-------------------------------------+-----------------+-----------------+-----------------+\n"
    for i in range(len(p_before_status)):
        if p_before_status[i]['var_value'] != p_after_status[i]['var_value']:
            diff = ''
            try:
                diff = str(int(p_after_status[i]['var_value']) - int(p_before_status[i]['var_value']))
            except:
                diff = ''
            info += f.format(p_before_status[i]['var_name'], p_before_status[i]['var_value'], p_after_status[i]['var_value'], diff)
    info += "+-------------------------------------+-----------------+-----------------+-----------------+\n"
    print(info)


def f_print_time(p_starttime, p_endtime):
    print("===== EXECUTE TIME =====\n")
    print(timediff(p_starttime, p_endtime))


def f_print_profiling(p_profiling_detail, p_profiling_summary):
    info = "===== SQL PROFILING(DETAIL)=====\n"
    info += "+--------------------------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+\n"
    f = '+{:^32}+{:^10}+{:^10}+{:^10}+{:^10}+{:^10}+{:^10}+{:^10}+{:^10}+{:^10}+{:^10}+\n'
    info += f.format('state', 'duration', 'cpu_user', 'cpu_sys', 'bk_in', 'bk_out', 'msg_s', 'msg_r', 'p_f_ma', 'p_f_mi', 'swaps')
    info += "+--------------------------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+\n"

    for row in p_profiling_detail:
        info += f.format(row['STATE'], row['DURATION'], row['CPU_USER'], row['CPU_SYSTEM'], str(row['BLOCK_OPS_IN']), str(row['BLOCK_OPS_OUT']),
                         str(row['MESSAGES_SENT']), str(row['MESSAGES_RECEIVED']), str(row['PAGE_FAULTS_MAJOR']), str(row['PAGE_FAULTS_MINOR']),
                         str(row['SWAPS']))
    info += "+--------------------------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+\n"
    info += "'bk_in:   block_ops_in'\n'bk_out:  block_ops_out'\n'msg_s:   message sent'\n'p_f_ma:  page_faults_major'\n'p_f_mi:  page_faults_minor'\n"

    f = '+{:^37}+{:^17}+{:^12}+{:^7}+{:^17}+\n'
    info += "===== SQL PROFILING(SUMMARY)=====\n+-------------------------------------+-----------------+------------+-------+-----------------+\n"
    info += f.format('state', 'total_r', 'pct_r', 'calls', 'r/call')
    info += "+-------------------------------------+-----------------+------------+-------+-----------------+\n"

    for row in p_profiling_summary:
        info += f.format(row['state'], row['total_r'], row['pct_r'], row['calls'], row['r_call'])
    info += "+-------------------------------------+-----------------+------------+-------+-----------------+\n"
    print(info)


def f_print_tableinfo(p_tablename):
    info = "+-----------------+------------+------------+------------+------------+------------+------------+------------+\n"
    f = '+{:^17}+{:^12}+{:^12}+{:^12}+{:^12}+{:^12}+{:^12}+{:^12}+\n'
    info += f.format('table_name', 'engine', 'format', 'table_rows', 'avg_row', 'total_mb', 'data_mb', 'index_mb')
    info += "+-----------------+------------+------------+------------+------------+------------+------------+------------+\n"
    sql = "select engine,row_format as format,table_rows,avg_row_length as avg_row,round((data_length+index_length)/1024/1024,2) as total_mb," \
          "round((data_length)/1024/1024,2) as data_mb,round((index_length)/1024/1024,2) as index_mb from information_schema.tables where table_schema='" + \
          cc['database'] + "' and table_name='" + p_tablename + "'"
    rows = run_sql(sql)
    for row in rows:
        info += f.format(row['engine'], row['engine'], row['format'], row['table_rows'], row['avg_row'], row['total_mb'], row['data_mb'], row['index_mb'])
    info += "+-----------------+------------+------------+------------+------------+------------+------------+------------+\n"
    print(info)


def f_print_indexinfo(p_tablename):
    sql = "select index_name,non_unique,seq_in_index,column_name,collation,cardinality,nullable,index_type" \
          " from information_schema.statistics where table_schema='" + \
          cc['database'] + "' and table_name='" + p_tablename + "' order by 1,3"
    rows = run_sql(sql)
    f = '+{:^17}+{:^17}+{:^17}+{:^17}+{:^17}+{:^17}+{:^17}+{:^17}+\n'
    if len(rows) > 0:
        info = "+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+\n"
        info += f.format('index_name', 'non_unique', 'seq_in_index', 'column_name', 'collation', 'cardinality', 'nullable', 'index_type')
        info += "+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+\n"
        for row in rows:
            info += f.format(row['index_name'], row['non_unique'], row['seq_in_index'], row['column_name'], row['collation'], row['cardinality'],
                             row['nullable'], row['index_type'])
        info += "+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+\n"
        print(info)


def f_print_title(p_sqltext):
    info = '*' * 100 + "\n"
    info += '* {:^96} *\n'.format('MySQL SQL Tuning Tools v2.0 (by qiyibaba(gai))')
    info += '*' * 100 + "\n\n===== BASIC INFORMATION =====\n+----------------------+------------+------------+------------+\n"
    info += '+{:^22}+{:^12}+{:^12}+{:^12}+\n'.format('server_ip', 'user_name', 'db_name', 'db_version')
    info += "+----------------------+------------+------------+------------+\n"
    info += '+{:^22}+{:^12}+{:^12}+{:^12}+\n'.format(cc['host'], cc['user'], cc['database'], 'Mysql 5.7')
    info += "+----------------------+------------+------------+------------+\n\n===== ORIGINAL SQL TEXT =====\n"
    # sqlparse.format(p_sqltext,reindent=True, keyword_case='upper')
    info += p_sqltext + "\n"
    print(info)


def timediff(timestart, timestop):
    t = (timestop - timestart)
    time_day = t.days
    s_time = t.seconds
    ms_time = t.microseconds / 1000000
    usedtime = int(s_time + ms_time)
    time_hour = usedtime / 60 / 60
    time_minute = (usedtime - time_hour * 3600) / 60
    time_second = usedtime - time_hour * 3600 - time_minute * 60
    time_micsecond = (t.microseconds - t.microseconds / 1000000) / 1000

    retstr = "%d day %d hour %d minute %d second %d microsecond " % (time_day, time_hour, time_minute, time_second, time_micsecond)
    return retstr


# 主方法
if __name__ == "__main__":
    sql_text = "select * from index_test where cola='b'"
    f_print_title(sql_text)

    if op['sys_param'] is 'ON':
        f_print_parm()
        f_print_optimizer_switch()

    # 提取到explain.py中单独执行
    if op['sql_plan'] is 'ON':
        # print('please execute explain.py')
        print(explain.explain(sql_text))

    if op['obj_stat'] is 'ON':
        print("===== OBJECT STATISTICS =====\n")
        for table_name in list(extract_table_identifiers(extract_from_part(sqlparse.parse(sql_text)[0]))):
            f_print_tableinfo(table_name)
            f_print_indexinfo(table_name)

    option = []
    if op['ses_status'] is 'ON':
        option.append('STATUS')
    if op['sql_profile'] is 'ON':
        option.append('PROFILING')

    if op['ses_status'] is 'ON' or op['sql_profile'] is 'ON':
        start_time = datetime.datetime.now()
        exec_result = f_exec_sql(sql_text, option)
        end_time = datetime.datetime.now()

        if op['ses_status'] is 'ON':
            f_print_status(exec_result['BEFORE_STATUS'], exec_result['AFTER_STATUS'])

        if op['sql_profile'] is 'ON':
            f_print_profiling(exec_result['PROFILING_DETAIL'], exec_result['PROFILING_SUMMARY'])

        f_print_time(start_time, end_time)
