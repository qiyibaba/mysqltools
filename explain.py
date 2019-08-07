# -*- coding: utf-8 -*-
# @Time    : 2019/8/5 15:04
# @Author  : qiyibaba
# @Site    : 
# @File    : explain.py


# import json
import mysql.connector

j = {
    "keywords": [
        "access_type",
        "attached_condition",
        "attached_subqueries",
        "buffer_result",
        "cacheable",
        "cost_info",
        "data_read_per_join",
        "dependent",
        "duplicates_removal",
        "eval_cost",
        "filtered",
        "group_by_subqueries",
        "grouping_operation",
        "having_subqueries",
        "key",
        "key_length",
        "materialized_from_subquery",
        "message",
        "nested_loop",
        "optimized_away_subqueries",
        "order_by_subqueries",
        "ordering_operation",
        "possible_keys",
        "prefix_cost",
        "query_block",
        "query_cost",
        "query_specifications",
        "read_cost",
        "ref",
        "rows_examined_per_scan",
        "rows_produced_per_join",
        "select_id",
        "select_list_subqueries",
        "sort_cost",
        "table",
        "table_name",
        "union_result",
        "update_value_subqueries",
        "used_columns",
        "used_key_parts",
        "using_filesort",
        "using_index",
        "using_index_for_group_by",
        "using_temporary_table"
    ],
    "id": {
        "mean": "id为SELECT的标识符. 它是在SELECT查询中的顺序编号. 如果这一行表示其他行的union结果, 这个值可以为空. 在这种情况下, table列会显示为形如<union M,N>, 表示它是id为M和N的查询行的联合结果"
    },
    "select_type": {
        "mean": "表示查询的类型",
        "selectType": {
            "SIMPLE": "简单SELECT(不使用UNION或子查询等)",
            "PRIMARY": "最外层的select",
            "UNION": "UNION中的第二个或后面的SELECT查询, 不依赖于外部查询的结果集",
            "DEPENDENT": "UNION中的第二个或后面的SELECT查询, 依赖于外部查询的结果集",
            "UNION RESULT": "UNION查询的结果集",
            "SUBQUERY": "子查询中的第一个SELECT查询, 不依赖于外部查询的结果集",
            "DEPENDENT SUBQUERY": "子查询中的第一个SELECT查询, 依赖于外部查询的结果集",
            "DERIVED": "用于from子句里有子查询的情况. MySQL会递归执行这些子查询, 把结果放在临时表里",
            "MATERIALIZED": "Materialized subquery.",
            "UNCACHEABLE SUBQUERY": "结果集不能被缓存的子查询, 必须重新为外层查询的每一行进行评估",
            "UNCACHEABLE UNION": "UNION中的第二个或后面的select查询, 属于不可缓存的子查询（类似于UNCACHEABLE SUBQUERY）"
        }
    },
    "table": {
        "mean": "输出行所引用的表"
    },
    "type": {
        "mean": "连接使用的类型",
        "accessType": {
            "system": {
                "mean": "这是const连接类型的一种特例, 该表仅有一行数据(=系统表).",
                "scalability": "O(1)"
            },
            "const": {
                "mean": "const用于使用常数值比较PRIMARY KEY时, 当查询的表仅有一行时, 使用system. 例:SELECT * FROM tbl WHERE col = 1.",
                "scalability": "O(1)"
            },
            "eq_ref": {
                "mean": "除const类型外最好的可能实现的连接类型. 它用在一个索引的所有部分被连接使用并且索引是UNIQUE或PRIMARY KEY, 对于每个索引键, 表中只有一条记录与之匹配. 例: 'SELECT * FROM RefTbl, tbl WHERE RefTbl.col=tbl.col;'.",
                "scalability": "O(log n)"
            },
            "ref": {
                "mean": "连接不能基于关键字选择单个行, 可能查找到多个符合条件的行. 叫做ref是因为索引要跟某个参考值相比较. 这个参考值或者是一个数, 或者是来自一个表里的多表查询的结果值. 例:'SELECT * FROM tbl WHERE idx_col=expr;'.",
                "scalability": "O(log n)"
            },
            "fulltext": {
                "mean": "查询时使用 FULLTEXT 索引.",
                "scalability": "O(log n)+"
            },
            "ref_or_null": {
                "mean": "如同ref, 但是MySQL必须在初次查找的结果里找出null条目, 然后进行二次查找.",
                "scalability": "O(log n)+"
            },
            "index_merge": {
                "mean": "表示使用了索引合并优化方法. 在这种情况下. key列包含了使用的索引的清单, key_len包含了使用的索引的最长的关键元素. 详情请见 8.2.1.4, “Index Merge Optimization”.",
                "scalability": "O(log n)+"
            },
            "unique_subquery": {
                "mean": "在某些IN查询中使用此种类型，而不是常规的ref:'value IN (SELECT PrimaryKey FROM SingleTable WHERE SomeExpr)'.",
                "scalability": "O(log n)+"
            },
            "index_subquery": {
                "mean": "在某些IN查询中使用此种类型, 与 unique_subquery 类似, 但是查询的是非唯一索引性索引.",
                "scalability": "O(log n)+"
            },
            "range": {
                "mean": "只检索给定范围的行, 使用一个索引来选择行. key列显示使用了哪个索引. key_len包含所使用索引的最长关键元素.",
                "scalability": "O(log n)+"
            },
            "index": {
                "mean": "全表扫描, 只是扫描表的时候按照索引次序进行而不是行. 主要优点就是避免了排序, 但是开销仍然非常大.",
                "scalability": "O(n)"
            },
            "ALL": {
                "mean": "最坏的情况, 从头到尾全表扫描.",
                "scalability": "O(n)"
            }
        }
    },
    "possible_keys": {
        "mean": "指出MySQL能在该表中使用哪些索引有助于查询. 如果为空, 说明没有可用的索引"
    },
    "key": {
        "mean": "MySQL实际从possible_keys选择使用的索引. 如果为NULL, 则没有使用索引. 很少情况下, MySQL会选择优化不足的索引. 这种情况下, 可以在select语句中使用USE INDEX (indexname)来强制使用一个索引或者用IGNORE INDEX (indexname)来强制MySQL忽略索引"
    },
    "key_len": {
        "mean": "显示MySQL使用索引键的长度. 如果key是NULL, 则key_len为NULL. 使用的索引的长度. 在不损失精确性的情况下, 长度越短越好"
    },
    "ref": {
        "mean": "显示索引的哪一列被使用了"
    },
    "rows": {
        "mean": "表示MySQL认为必须检查的用来返回请求数据的行数"
    },
    "filtered": {
        "mean": "表示返回结果的行占需要读到的行(rows列的值)的百分比"
    },
    "Extra": {
        "mean": "该列显示MySQL在查询过程中的一些详细信息, MySQL查询优化器执行查询的过程中对查询计划的重要补充信息",
        "info": {
            "Using temporary": "表示MySQL在对查询结果排序时使用临时表. 常见于排序order by和分组查询group by.",
            "Using filesort": "MySQL会对结果使用一个外部索引排序,而不是从表里按照索引次序读到相关内容. 可能在内存或者磁盘上进行排序. MySQL中无法利用索引完成的排序操作称为'文件排序'.",
            "Using index condition": "在5.6版本后加入的新特性（Index Condition Pushdown）。Using index condition 会先条件过滤索引，过滤完索引后找到所有符合索引条件的数据行，随后用 WHERE 子句中的其他条件去过滤这些数据行。",
            "Range checked for each record": "MySQL没有发现好的可以使用的索引,但发现如果来自前面的表的列值已知,可能部分索引可以使用。",
            "Using where with pushed condition": "这是一个仅仅在NDBCluster存储引擎中才会出现的信息，打开condition pushdown优化功能才可能被使用。",
            "Using MRR": "使用了 MRR Optimization IO 层面进行了优化，减少 IO 方面的开销。",
            "Skip_open_table": "Tables are read using the Multi-Range Read optimization strategy.",
            "Open_frm_only": "Table files do not need to be opened. The information is already available from the data dictionary.",
            "Open_full_table": "Unoptimized information lookup. Table information must be read from the data dictionary and by reading table files.",
            "Scanned": "This indicates how many directory scans the server performs when processing a query for INFORMATION_SCHEMA tables.",
            "Using index for group-by": "Similar to the Using index table access method, Using index for group-by indicates that MySQL found an index that can be used to retrieve all columns of a GROUP BY or DISTINCT query without any extra disk access to the actual table. Additionally, the index is used in the most efficient way so that for each group, only a few index entries are read.",
            "Start temporary": "This indicates temporary table use for the semi-join Duplicate Weedout strategy.Start",
            "End temporary": "This indicates temporary table use for the semi-join Duplicate Weedout strategy.End",
            "FirstMatch": "The semi-join FirstMatch join shortcutting strategy is used for tbl_name.",
            "Materialize": "Materialized subquery",
            "Start materialize": "Materialized subquery Start",
            "End materialize": "Materialized subquery End",
            "unique row not found": "For a query such as SELECT ... FROM tbl_name, no rows satisfy the condition for a UNIQUE index or PRIMARY KEY on the table.",
            "Index dive skipped due to FORCE": "This item applies to NDB tables only. It means that MySQL Cluster is using the Condition Pushdown optimization to improve the efficiency of a direct comparison between a nonindexed column and a constant. In such cases, the condition is “pushed down” to the cluster's data nodes and is evaluated on all data nodes simultaneously. This eliminates the need to send nonmatching rows over the network, and can speed up such queries by a factor of 5 to 10 times over cases where Condition Pushdown could be but is not used.",
            "Impossible WHERE noticed after reading const tables": "查询了所有const(和system)表, 但发现WHERE查询条件不起作用.",
            "Using where": "WHERE条件用于筛选出与下一个表匹配的数据然后返回给客户端. 除非故意做的全表扫描, 否则连接类型是ALL或者是index, 且在Extra列的值中没有Using Where, 则该查询可能是有问题的.",
            "Using join buffer": "从已有连接中找被读入缓存的数据, 并且通过缓存来完成与当前表的连接.",
            "Using index": "只需通过索引就可以从表中获取列的信息, 无需额外去读取真实的行数据. 如果查询使用的列值仅仅是一个简单索引的部分值, 则会使用这种策略来优化查询.",
            "const row not found": "空表做类似 SELECT ... FROM tbl_name 的查询操作.",
            "Distinct": "MySQL is looking for distinct values, so it stops searching for more rows for the current row combination after it has found the first matching row.",
            "Full scan on NULL key": "子查询中的一种优化方式, 常见于无法通过索引访问null值.",
            "Impossible HAVING": "HAVING条件过滤没有效果, 返回已有查询的结果集.",
            "Impossible WHERE": "WHERE条件过滤没有效果, 最终是全表扫描.",
            "LooseScan": "使用半连接LooseScan策略.",
            "No matching min/max row": "没有行满足查询的条件, 如 SELECT MIN(...) FROM ... WHERE condition.",
            "no matching row in const table": "对于连接查询, 列未满足唯一索引的条件或表为空.",
            "No matching rows after partition pruning": "对于DELETE 或 UPDATE, 优化器在分区之后, 未发现任何要删除或更新的内容. 类似查询 Impossible WHERE.",
            "No tables used": "查询没有FROM子句, 或者有一个 FROM DUAL子句.",
            "Not exists": "MySQL能够对LEFT JOIN查询进行优化, 并且在查找到符合LEFT JOIN条件的行后, 则不再查找更多的行.",
            "Plan isn't ready yet": "This value occurs with EXPLAIN FOR CONNECTION when the optimizer has not finished creating the execution plan for the statement executing in the named connection. If execution plan output comprises multiple lines, any or all of them could have this Extra value, depending on the progress of the optimizer in determining the full execution plan.",
            "Using intersect": "开启了index merge，即：对多个索引分别进行条件扫描，然后将它们各自的结果进行合并，使用的算法为：index_merge_intersection",
            "Using union": "开启了index merge，即：对多个索引分别进行条件扫描，然后将它们各自的结果进行合并，使用的算法为：index_merge_union",
            "Using sort_union": "开启了index merge，即：对多个索引分别进行条件扫描，然后将它们各自的结果进行合并，使用的算法为：index_merge_sort_union"
        }
    }
}

config = {
    'user': 'root',
    'password': 'zte',
    'host': '127.0.0.1',
    'database': 'lttest',
    'charset': 'utf8',
    "connection_timeout": 5,
    "use_pure": True
}

explain_info = None


class ExplainInfo:
    def __init__(self, sql, exp_rows=None, warnings=None):
        self.sql = sql
        self.exp_rows = exp_rows
        self.warnings = warnings

    def __str__(self):
        info = "SQL [" + self.sql + "]\n"
        f = "|{:3s}|{:15s}|{:10s}|{:10s}|{:10s}|{:20s}|{:10s}|{:10s}|{:10s}|{:10s}|{:10s}|{:12s}|{:10s}|\n"
        info += f.format("id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "scalability",
                         "Extra")
        # print(','.join(map(lambda x: ''.join('"---"'), list(range(12)))))
        info += f.format("---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---", "---")
        for row in self.exp_rows:
            info += f.format(str(row.id), str(row.select_type), str(row.table), str(row.partitions), str(row.type), str(row.possible_keys), str(row.key),
                             str(row.key_len), str(row.ref), str(row.rows), str(row.filtered), str(row.scalability), str(row.Extra))
            info += str(row)
        return info


class ExplainRow:
    def __init__(self, id, select_type, table, partitions, type,
                 possible_keys, key, key_len, ref, rows, filtered, Extra):
        self.id = id
        self.select_type = select_type
        self.table = table
        self.partitions = partitions
        self.type = type
        self.possible_keys = possible_keys
        self.key = key
        self.key_len = key_len
        self.ref = ref
        self.rows = rows
        self.filtered = filtered
        self.Extra = Extra
        self.scalability = None
        self.info = self.parse()

    def parse(self):
        info = "### Explain信息解读\n"
        # j = json.loads(open("explainInfo.json", "r", encoding='UTF-8').read())
        if self.select_type is not None:
            info += '#### SelectType信息解读\n* **{}**: {}\n'.format(self.select_type, j['select_type']['selectType'][self.select_type])
        if self.type is not None:
            info += '#### Type信息解读\n* **{}**: {}\n'.format(self.type, j['type']['accessType'][self.type]['mean'])
            self.scalability = j['type']['accessType'][self.type]['scalability']
        if self.Extra is not None:
            e = self.Extra
            info += "#### Extra信息解读\n"
            for item in j['Extra']['info']:
                if e.find(item) >= 0:
                    if item is "Impossible WHERE" and e.find("Impossible WHERE noticed after reading const tables") >= 0:
                        continue
                    if item is "Using index" and e.find("Using index condition") >= 0:
                        continue
                    info += '* **{}**: {}\n'.format(item, j['Extra']['info'][item])
        return info

    def __str__(self):
        return self.info


def explain(s):
    exp = ExplainInfo(sql)
    rows = []
    cnx = mysql.connector.connect(**config)  # 建立连接
    cursor = cnx.cursor(dictionary=True)

    cursor.execute("explain " + s)
    for item in cursor.fetchall():
        r = ExplainRow(**item)
        rows.append(r)

    cursor.close()
    cnx.close()

    exp.exp_rows = rows
    return exp


if __name__ == '__main__':
    sql = "select * from index_test where cola='b'"
    explain = explain(sql)
    print(explain)
