from parserSQL.mySqlParser import MySqlParser
from database.myDatabase import MyTable
from util.myUtil import _print, merge_dict, merge_result_inner
from pickle import dump, load
from shutil import rmtree
import os
import time


class MySQLExecuter:
    def __init__(self):
        # init the parserSQL
        self.parser = MySqlParser()

        # database_map ={
        #       name, tables_map = { name, Table }
        # }
        self.database_map = {}

        # path_map = {
        #       db_name, path
        # }
        self.path_map = {}

        self.currentDatabase = None

        # tables_map = {
        #       name, Table
        # }
        self.tables_map = None

        # map the keywords with function
        self.function_map = {
            'insert': self._insert,
            'create': self._create,
            'search': self._select,
            'delete': self._delete,
            'update': self._update,
            'create_index': self._createIndex,
            'create_db': self._createDatabase,
            'use': self._useDatabase,
            'exit': self._exit,
            'show': self._show,
            'drop': self._drop,
            'search join': self._select_join,
        }

        self._load()

    def _load(self):
        db_path = os.path.join(os.getcwd(), "db")
        for path, db_list, _ in os.walk(db_path):
            for db_name in db_list:
                self.database_map[db_name] = {}
                # print(db_name)
                self.path_map[db_name] = os.path.join(path, db_name)

    def run(self):
        while True:
            statement = input("MySql> ")
            self.execute(statement)

    def execute(self, statement):
        start_time = time.time()
        operation_map = self.parser.parse(statement)
        # print('action: ', action)
        if operation_map:
            self.function_map[operation_map['type']](operation_map)

            end_time = time.time()
            run_time = end_time - start_time
            print('run time: ', run_time)

    def _insert(self, operation_map):
        # {'type': 'insert',
        #  'table': table_name,
        #  'data':
        #          {
        #           col0_name : data,
        #           col2_name : data
        #          }
        # }

        # or 'data' : ['https://www.baidu.com/', 4, 'CN']
        #
        if self.currentDatabase is None:
            print("No database select")
        if self.tables_map.get(operation_map['table']) is None:
            print("No such table")
        else:
            try:
                self.tables_map[operation_map['table']].insert(operation_map['data'])
                # self.tables_map[operation_map['table']].updateIndex()
                self._updateTable({'database': self.currentDatabase, 'name': operation_map['table']})
            except Exception as e:
                print(e.args[0])

    # create table
    def _create(self, operation_map):
        # operation = {
        #       type: create,
        #       name: table_name,
        #       col: {
        #           col0_name: [col0_type_list],
        #           ...
        #           ...
        #           }
        # }
        if self.currentDatabase == None:
            print("Did not Choose Database!")
            return
        # try:
        if operation_map['name'] in self.tables_map.keys():
            print("Table %s Already Exists!" % (operation_map['name']))
            return
        print(operation_map['col'])
        self.tables_map[operation_map['name']] = MyTable(operation_map['name'], operation_map['col'])
        self._updateTable({
            'database': self.currentDatabase,
            'name': operation_map['name']
        })

    # except Exception as e:
    #     print(111)
    #     print(e.args[0])

    def _updateTable(self, operation_map):
        # print(action)
        filepath = os.path.join("db", operation_map['database'])
        filepath = os.path.join(filepath, operation_map['name'])
        if os.path.exists(filepath):
            os.remove(filepath)
        f = open(filepath, 'wb')
        dump(self.tables_map[operation_map['name']], f)
        f.close()

    def _select(self, operation_map):
        # 如果有
        # operation = {
        #       type: search,
        #       field: [cols_name_list]
        #       table: table_name,
        #       groupby: col_name,
        #       orderby: col_name,
        #       limit: num
        #       conditions_logic: OR/AND
        #       condition: [{field: col_name, cond: {operation: >or=or ..., value: num}}, {}, {}, ...]
        # }
        if self.currentDatabase is None:
            print("No Database")
            return

        if operation_map['table'] not in self.tables_map.keys():
            print('Error!!! No Table Named %s' % (operation_map['table']))
            return

        # res_map = {
        #     col_name: [val0, val1,...]
        #     ...
        # }
        res_map, type_map, orderby_col_data_list = self.tables_map[operation_map['table']].select(operation_map)

        if orderby_col_data_list:
            # 将result按照orderby_col_data_list里的值排序(从小到大)
            for key, value in res_map.items():
                new_value_list = [i for _, i in sorted(zip(orderby_col_data_list, value))]
                res_map[key] = new_value_list

        if operation_map.get('having'):
            having_col = operation_map['having'][0][:3]
            having_opt = operation_map['having'][1]
            if operation_map['having'][2].isdigit():
                having_num = float(operation_map['having'][2])
            else:
                print('Syntax error')
                return
            having_index_list = []
            for key in res_map:
                if key[0:3] == having_col:
                    if having_opt == '>':
                        for i in range(0, len(res_map[key]), 1):
                            if not res_map[key][i] > having_num:
                                having_index_list.append(i)
                    elif having_opt == '>=':
                        for i in range(0, len(res_map[key]), 1):
                            if not res_map[key][i] >= having_num:
                                having_index_list.append(i)
                    elif having_opt == '=':
                        for i in range(0, len(res_map[key]), 1):
                            if not res_map[key][i] == having_num:
                                having_index_list.append(i)
                    elif having_opt == '<':
                        for i in range(0, len(res_map[key]), 1):
                            if not res_map[key][i] < having_num:
                                having_index_list.append(i)
                    elif having_opt == '<=':
                        for i in range(0, len(res_map[key]), 1):
                            if not res_map[key][i] <= having_num:
                                having_index_list.append(i)
            # print(having_index_list)
            for i in range(len(having_index_list) - 1, -1, -1):
                for key in res_map:
                    del res_map[key][having_index_list[i]]

        # 处理limit
        if operation_map.get('limit'):
            try:
                operation_map['limit'] = int(operation_map['limit'])
                _print(res_map, type_map, operation_map['limit'])
            except Exception:
                print('ERROR! Please Enter Integer As Limit Constraint!!')
                return
        else:
            # print(res_map)
            print(type_map)
            _print(res_map, type_map)

    def _delete(self, operation_map):
        """
        {
            'type': 'delete',
            'table': table_name,
            conditions_logic: OR/AND
            condition: [{field: col_name, cond: {operation: >or=or ..., value: num}}, {}, {}, ...]
        }
        """
        if self.currentDatabase is None:
            print("No Database")
            return
        elif operation_map['table'] not in self.tables_map.keys():
            print("No such Table")
            return
        self.tables_map[operation_map['table']].delete(operation_map)
        self.tables_map[operation_map['table']].updateIndex()
        self._updateTable({
            'database': self.currentDatabase,
            'name': operation_map['table']
        })

    def _update(self, operation_map):
        # {
        #     'type': 'update',
        #     'table': tabel_name,
        #     'data': {col1_name: data, col2_name: data},
        #     'conditions': [
        #                     {'field': col1_name, 'cond': {'operation': '=', 'value': value}},
        #                     {'field': col2_name, 'cond': {'operation': '=', 'value': value}}
        #                    ],
        #     'conditions_logic': 'AND'}

        if operation_map['table'] not in self.tables_map.keys():
            print("No such table")
            return
        elif self.currentDatabase is None:
            print("No database")
            return
        else:
            self.tables_map[operation_map['table']].update(operation_map)
            self.tables_map[operation_map['table']].updateIndex()
            self._updateTable({
                'database': self.currentDatabase,
                'name': operation_map['table']
            })

    def _createIndex(self, operation_map):
        if self.currentDatabase == None:
            print("Did not Choose Database!")
            return
        if self.tables_map[operation_map['table']].createIndex(operation_map):
            self._updateTable({
                'database': self.currentDatabase,
                'name': operation_map['table']
            })


    def _createDatabase(self, operation_map):
        # operation = {
        #       type: create_db,
        #       name: database_name
        # }
        if operation_map['name'] not in self.database_map.keys():
            self.database_map[operation_map['name']] = {}
            db_path = os.path.join('db', operation_map['name'])
            if not os.path.exists(db_path):
                os.makedirs(db_path)
            self._load()
        else:
            print("Database '%s' exists" % (operation_map['name']))

    def _useDatabase(self, operation_map):
        # operation_map = {
        #     "type": 'use',
        #     "database": statement[1]
        # }

        if operation_map['database'] in self.database_map.keys():
            self.currentDatabase = operation_map['database']
            self.tables_map = self.database_map[operation_map['database']]

            for filepath, _, table_list in os.walk(self.path_map[self.currentDatabase]):
                for table_name in table_list:
                    f = open(os.path.join(filepath, table_name), 'rb')
                    self.database_map[self.currentDatabase][table_name] = load(f)
                    # print(load(f))
                    f.close()

        else:
            print("No Database Named %s" % (operation_map['database']))

    def _exit(self, operation_map):
        self._save()
        os._exit(0)

    def _save(self):
        path = os.path.join(os.getcwd(), "db")
        f = None
        for dname, tables in self.database_map.items():
            db_path = os.path.join(path, dname)
            if not os.path.exists(db_path):
                os.makedirs(db_path)
            for tname, table in tables.items():
                file_path = os.path.join(db_path, tname)
                f = open(file_path, 'wb')
                dump(table, f)
                f.close()

    def _show(self, operation_map):
        if operation_map['kind'] == 'databases':
            # operation_map:{
            #     'type' : 'show',
            #     'kind' : 'databases'
            # }
            databases = list(self.database_map.keys())
            # print(databases)
            _print({'databases': databases})
        elif operation_map['kind'] == 'tables':
            # operation_map:{
            #     'type' : 'show',
            #     'kind' : 'tables'
            # }
            if self.currentDatabase is None:
                print("Not use Database!")
                return
            tables_list = list(self.tables_map.keys())
            _print({
                'tables': tables_list
            })

    # 没有写完
    def _drop(self, operation_map):
        if operation_map['kind'] == 'database':
            # operation_map = {
            #       'type': 'drop',
            #       'kind': 'database',
            #       'name': statement[2]
            # }
            if operation_map['name'] not in self.database_map.keys():
                print("No Database Named %s", operation_map['name'])
                return
            self._dropDB(operation_map)
            del self.database_map[operation_map['name']]
            if self.currentDatabase == operation_map['name']:
                self.currentDatabase = None

        elif operation_map['kind'] == 'table':
            # operation_map =  {
            #     'type': 'drop',
            #     'kind': 'table',
            #     'name': statement[2]
            # }
            if self.currentDatabase is None:
                print("Did not Choose Database!")
                return
            if operation_map['name'] not in self.tables_map.keys():
                print("No Table Named %s", operation_map['name'])
                return
            operation_map['database'] = self.currentDatabase
            self._dropTable(operation_map)
            del self.database_map[self.currentDatabase][operation_map['name']]
            self.tables_map = self.database_map[self.currentDatabase]

        elif operation_map['kind'] == 'index':
            if self.currentDatabase is None:
                print("Did not Choose Database!")
                return
            if operation_map['table'] not in self.tables_map.keys():
                print("No Table Named %s", operation_map['table'])
                return
            if self.tables_map[operation_map['table']].dropIndex(operation_map):
                self._updateTable({
                    'database': self.currentDatabase,
                    'name': operation_map['table']
                })

    def _dropTable(self, action):
        # print(action)
        filepath = os.path.join("db", action['database'])
        filepath = os.path.join(filepath, action['name'])
        os.remove(filepath)

    def _dropDB(self, operation_map):
        # print(action)
        folderpath = os.path.join("db", operation_map['name'])
        rmtree(folderpath)

    def _select_join(self, operation_map):
        if self.currentDatabase is None:
            print("No Database!")
            return

        if operation_map.get("conditions"):
            first_table = operation_map['conditions'][0]['field'].split(".")[0]
            first_table_cond_field = operation_map['conditions'][0]['field'].split(".")[1]
            first_table_col = operation_map['join fields'][first_table]
            action_to_table1 = {
                'type': 'search',
                'table': first_table,
                'conditions': [{
                    'field': first_table_cond_field,
                    'cond': operation_map['conditions'][0]['cond'],
                }]
            }
        else:
            first_table = list(operation_map['join fields'].keys())[0]
            first_table_col = operation_map['join fields'][first_table]
            action_to_table1 = {
                'type': 'search',
                'table': first_table,
            }

        first_table_cols = self.tables_map[first_table].col_name_list

        if operation_map['fields'] == '*':
            first_table_fields = '*'
        else:
            first_table_fields = []
            for i in operation_map['fields']:
                if i in first_table_cols:
                    first_table_fields.append(i)
                    if i != operation_map['join fields'][first_table]:
                        operation_map['fields'].remove(i)

        action_to_table1['fields'] = first_table_fields

        # print('read table1')
        res1, type1, _ = self.tables_map[first_table].select(action_to_table1)
        # print(res1)
        # use join fields to search table2 based on the values we select in table1
        for k, v in operation_map['join fields'].items():
            if k != first_table:
                second_table_field = v
                second_table = k
        conditions = []

        for index in res1[first_table_col]:
            condition = {
                'field': second_table_field,
                "cond": {
                    'operation': '=',
                    'value': f'{index}',
                }
            }
            conditions.append(condition)

        action_to_table2 = {
            'type': 'search',
            'table': second_table,
            'conditions_logic': 'OR',
            'fields': operation_map['fields'],
            'conditions': conditions
        }
        # print("read table 2")
        res2, type2, _ = self.tables_map[second_table].select(action_to_table2)
        # print(res2)
        result = {}
        types = {}
        types = merge_dict(types, type1, first_table)
        # 处理结果
        result = merge_result_inner(result, res1, res2, first_table_col, second_table_field, first_table, second_table)
        merge_dict(types, type2, second_table)
        _print(result, types)
