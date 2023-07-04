from re import compile


class MySqlParser:

    def __init__(self):
        # ----------------<editor-fold desc="<code block: map data>">----------------

        self.__operation_map = {
            'SELECT': self.__select,
            'INSERT': self.__insert,
            'UPDATE': self.__update,
            'DELETE': self.__delete,
            'CREATE': self.__create,
            'DROP': self.__drop,
            'USE': self.__use,
            'EXIT': self.__exit,
            'JOIN': self.__join,
            'SHOW': self.__show
        }

        self.__regex_map = {
            'SELECT': r'(SELECT|select) (.*) (FROM|from) (.*)',
            'INSERT': r'(INSERT|insert) (INTO|into) (.*) \((.*)\) (VALUES|values) \((.*)\)',
            'INSERT_2': r'(INSERT|insert) (INTO|into) (.*) (VALUES|values) \((.*)\)',
            'UPDATE': r'(UPDATE|update) (.*) (SET|set) (.*)',
            'DELETE': r'(DELETE|delete) (FROM|from) (.*)',
            'CREATE': r'(CREATE|create) (TABLE|table) (.*) \((.*)\)',
            'CREATE DATABASE': r'(CREATE|create) (DATABASE|database) (.*)',
            'CREATE INDEX': r'(CREATE|create) (INDEX|index) (.*) (ON|on) (.*) \((.*)\)',
            'DROP INDEX': r'(DROP|drop) (INDEX|index) (.*) (ON|on) (.*)',
            'GROUPBY': r'(.*) (GROUP|group) (BY|by) (.*)'
        }

        # ---------------------</editor-fold>---------------------

    # parse the sql
    def parse(self, sql):
        # ----------------<editor-fold desc="<code block: parse the sql and return operation_map{}>">----------------

        base_statement = self.__remove_space(sql.split(' '))
        # print(base_statement)

        # length of sql < 2 and not 'exit', error
        if len(base_statement) < 2 and base_statement[0].lower() != 'exit':
            print('Syntax Error(code 0)')
            return

        if "JOIN" in base_statement or "join" in base_statement:
            operation_type = 'JOIN'
        else:
            # get operation type
            operation_type = base_statement[0].upper()

        # wrong operation type, error
        if operation_type not in self.__operation_map:
            print('Syntax Error(code 1)')
            return

        operation = self.__operation_map[operation_type](base_statement)
        # print(operation)

        if operation is None or 'type' not in operation:
            print('Syntax Error')
            return

        return operation

        # ---------------------</editor-fold>---------------------

    # ----------------<editor-fold desc="<code block: parser function called in parse()>">----------------

    def __select(self, base_statement):
        if " AND " in base_statement and " OR " in base_statement:
            # print()
            return
        # print(base_statement)
        operation = {}
        ret = compile(self.__regex_map["SELECT"]).findall(" ".join(base_statement))
        # print(ret)
        if ret and len(ret[0]) == 4:
            fields = ret[0][1]
            if fields != '*':
                fields = [field.strip() for field in fields.split(',')]
            # print (fields)
            operation["type"] = "search"
            operation["fields"] = fields
            operation["table"] = ret[0][3].strip().split(" ")[0]

            if "GROUP BY" in ret[0][3]:
                groupby = ret[0][3].split("GROUP BY")
                groupby = groupby[1].strip().split(' ')[0]
                operation["groupby"] = groupby
            elif "group by" in ret[0][3]:
                groupby = ret[0][3].split("group by")
                groupby = groupby[1].strip().split(' ')[0]
                operation["groupby"] = groupby

            if 'HAVING' in ret[0][3]:
                operation['having'] = []
                having = ret[0][3].split('HAVING')
                having = having[1].strip().split(' ')
                for i in having:
                    operation['having'].append(i)
            elif 'having' in ret[0][3]:
                operation['having'] = []
                having = ret[0][3].split('having')
                having = having[1].strip().split(' ')
                for i in having:
                    operation['having'].append(i)

            if "ORDER BY" in ret[0][3]:
                orderby = ret[0][3].split("ORDER BY")
                orderby = orderby[1].strip().split(' ')[0]
                operation["orderby"] = orderby
            elif "order by" in ret[0][3]:
                orderby = ret[0][3].split("order by")
                orderby = orderby[1].strip().split(' ')[0]
                operation["orderby"] = orderby

            if "LIMIT" in ret[0][3]:
                limit = ret[0][3].split("LIMIT")
                limit = limit[1].strip().split(' ')[0]
                operation["limit"] = int(limit)
            elif "limit" in ret[0][3]:
                limit = ret[0][3].split("limit")
                limit = limit[1].strip().split(' ')[0]
                operation["limit"] = int(limit)

            # print(ret[0][3])
            if "WHERE" in ret[0][3]:
                where = ret[0][3].split("WHERE")
                if len(where) > 2:
                    print("Syntax Error(code 6)")
                    return
                # print(where[1])
                # condition里的逻辑符不能混用
                # 逻辑子句只能有两条
                if " AND " in where[1]:
                    operation["conditions_logic"] = "AND"
                    operation["conditions"] = []
                    condition_list = where[1].strip().split(' AND ')
                    # print(condition_list)
                    for c in condition_list:
                        field = c.strip().split(' ')
                        sym = field[1]
                        if "'" in field[2] or "\"" in field[2]:
                            value = field[2].replace('"', '').replace("'", '').strip()
                        else:
                            try:
                                value = int(field[2].strip())
                            except:
                                return None
                        operation["conditions"].append({
                            'field': field[0],
                            'cond': {
                                'operation': sym,
                                'value': value
                            }
                        })
                elif " OR " in where[1]:
                    operation["conditions_logic"] = "OR"
                    operation["conditions"] = []
                    condition_list = where[1].strip().split(' OR ')
                    for c in condition_list:
                        field = c.strip().split(' ')
                        if "'" in field[2] or "\"" in field[2]:
                            value = field[2].replace('"', '').replace("'", '').strip()
                        else:
                            try:
                                value = int(field[2].strip())
                            except:
                                return None
                        operation["conditions"].append({
                            'field': field[0],
                            'cond': {
                                'operation': field[1],
                                'value': value
                            }
                        })
                else:
                    operation["conditions"] = []
                    condition = where[1]
                    field = condition.strip().split(' ')
                    if "'" in field[2] or "\"" in field[2]:
                        value = field[2].replace('"', '').replace("'", '').strip()
                    else:
                        try:
                            value = int(field[2].strip())
                        except:
                            return None
                    operation["conditions"].append({
                        'field': field[0],
                        'cond': {
                            'operation': field[1],
                            'value': value
                        }
                    })
            elif "where" in ret[0][3]:
                where = ret[0][3].split("where")
                if len(where) > 2:
                    print("Syntax Error(code 6)")
                    return
                # print(where[1])
                # condition里的逻辑符不能混用
                # 逻辑子句只能有两条
                if " AND " in where[1]:
                    operation["conditions_logic"] = "AND"
                    operation["conditions"] = []
                    condition_list = where[1].strip().split(' AND ')
                    # print(condition_list)
                    for c in condition_list:
                        field = c.strip().split(' ')
                        sym = field[1]
                        if "'" in field[2] or "\"" in field[2]:
                            value = field[2].replace('"', '').replace("'", '').strip()
                        else:
                            try:
                                value = int(field[2].strip())
                            except:
                                return None
                        operation["conditions"].append({
                            'field': field[0],
                            'cond': {
                                'operation': sym,
                                'value': value
                            }
                        })
                elif " OR " in where[1]:
                    operation["conditions_logic"] = "OR"
                    operation["conditions"] = []
                    condition_list = where[1].strip().split(' OR ')
                    for c in condition_list:
                        field = c.strip().split(' ')
                        if "'" in field[2] or "\"" in field[2]:
                            value = field[2].replace('"', '').replace("'", '').strip()
                        else:
                            try:
                                value = int(field[2].strip())
                            except:
                                return None
                        operation["conditions"].append({
                            'field': field[0],
                            'cond': {
                                'operation': field[1],
                                'value': value
                            }
                        })
                else:
                    operation["conditions"] = []
                    condition = where[1]
                    field = condition.strip().split(' ')
                    if "'" in field[2] or "\"" in field[2]:
                        value = field[2].replace('"', '').replace("'", '').strip()
                    else:
                        try:
                            value = int(field[2].strip())
                        except:
                            return None
                    operation["conditions"].append({
                        'field': field[0],
                        'cond': {
                            'operation': field[1],
                            'value': value
                        }
                    })

            if operation.get('having'):
                if not operation.get('groupby'):
                    print('Syntax Error(code 7)')
                    return
                else:
                    if operation['having'][0] not in operation['fields']:
                        print('Syntax Error(code 7)')
                        return



            # 如果有
            # operation = {
            #       type: search,
            #       fields: [cols_name_list]
            #       table: table_name,
            #       groupby: col_name,
            #       having: ['AVG(COL2)', '>', '1'],
            #       orderby: col_name,
            #       limit: num
            #       conditions_logic: OR/AND
            #       condition: [{field: col_name, cond: {operation: >or=or ..., value: num}}, {}, {}, ...]
            # }
            return operation

        return None

    def __insert(self, base_statement):
        operation = {}
        ret = compile(self.__regex_map["INSERT"]).findall(' '.join(base_statement))
        if ret and len(ret[0]) == 6:
            # print(ret[0])
            if len(ret[0][2].split(',')) > 1:
                # print(1)
                return
            if len(ret[0][3].split(',')) != len(ret[0][5].split(',')):
                # print(2)
                return
            operation["type"] = "insert"
            operation["table"] = ret[0][2]

            fields = ret[0][3].split(',')
            values = ret[0][5].split(',')
            data = {}
            for i in range(len(fields)):
                value = values[i]
                if "'" in values[i] or "\"" in values[i]:
                    value = value.replace('"', '').replace("'", '').strip()
                else:
                    try:
                        value = int(value.strip())
                    except:
                        return None
                data[fields[i].strip()] = value

            operation["data"] = data

            return operation
            # operation = {
            #       type: insert,
            #       table: tabel_name,
            #       data: {col_name: col_value, ...,}
            # }

        ret = compile(self.__regex_map["INSERT_2"]).findall(' '.join(base_statement))
        if ret and len(ret[0]) == 5:
            if len(ret[0][2].split(',')) > 1:
                # print(1)
                return
            values = ret[0][4].strip().split(',')
            operation["type"] = "insert"
            operation["table"] = ret[0][2]
            for i in range(len(values)):
                value = values[i]
                if "'" in values[i] or '"' in values[i]:
                    value = value.replace('"', '').replace("'", '').strip()
                else:
                    try:
                        value = int(value.strip())
                    except:
                        return None
                values[i] = value
            operation["data"] = values

            return operation
            # operation = {
            #       type: insert,
            #       table: table_name,
            #       data: [value_list]
            # }

        return

    def __update(self, base_statement):
        operation = {}
        if 'WHERE' in base_statement:
            update_base_statement = base_statement[:base_statement.index('WHERE')]
        elif 'where' in base_statement:
            update_base_statement = base_statement[:base_statement.index('where')]
        else:
            update_base_statement = base_statement
        ret = compile(self.__regex_map["UPDATE"]).findall(' '.join(update_base_statement))
        if ret and len(ret[0]) == 4:
            # print(0)
            if len(ret[0][1].split(' ')) > 1 or len(ret[0][1].split(',')) > 1:
                # print(1)
                return
            operation["type"] = "update"
            operation["table"] = ret[0][1]
            operation["data"] = {}
            sub_statement = ret[0][3].strip().split(',')
            for sub in sub_statement:
                v = sub.strip().split('=')
                if "'" in v[1] or '"' in v[1]:
                    v[1] = v[1].replace('"', '').replace("'", '').strip()
                else:
                    try:
                        v[1] = int(v[1].strip())
                    except:
                        return
                operation["data"][v[0].strip()] = v[1]

            if 'WHERE' in base_statement or 'where' in base_statement:
                operation['conditions'] = []

                if 'WHERE' in base_statement:
                    where_index = base_statement.index('WHERE')
                else:
                    where_index = base_statement.index('where')

                if 'AND' in base_statement or 'and' in base_statement:
                    operation['conditions_logic'] = 'AND'
                elif 'OR' in base_statement or 'or' in base_statement:
                    operation['conditions_logic'] = 'OR'

                start = where_index + 1
                # print(start, len(base_statement))
                while start < len(base_statement):
                    # print(start)
                    cond = {}
                    cond['field'] = base_statement[start]
                    cond['cond'] = {}
                    start += 1
                    cond['cond']['operation'] = base_statement[start]
                    start += 1
                    if "'" in base_statement[start] or "\"" in base_statement[start]:
                        value = base_statement[start].replace('"', '').replace("'", '').strip()
                    else:
                        try:
                            value = int(base_statement[start].strip())
                        except:
                            return None
                    cond['cond']['value'] = value
                    start += 2
                    operation['conditions'].append(cond)

            return operation
            # operation = {
            #       type: update,
            #       table: table_name,
            #       data: {col_name: col_value, ...,}
            # }

        return

    def __delete(self, base_statement):
        operation = {}
        operation['type'] = 'delete'
        operation['table'] = base_statement[2]

        if 'WHERE' in base_statement or 'where' in base_statement:
            operation['conditions'] = []

            if 'WHERE' in base_statement:
                where_index = base_statement.index('WHERE')
            else:
                where_index = base_statement.index('where')

            if 'AND' in base_statement or 'and' in base_statement:
                operation['conditions_logic'] = 'AND'
            elif 'OR' in base_statement or 'or' in base_statement:
                operation['conditions_logic'] = 'OR'

            start = where_index + 1
            # print(start, len(statement))
            while start < len(base_statement):
                # print(start)
                cond = {}
                cond['field'] = base_statement[start]
                cond['cond'] = {}
                start += 1
                cond['cond']['operation'] = base_statement[start]
                start += 1
                if "'" in base_statement[start] or "\"" in base_statement[start]:
                    value = base_statement[start].replace('"', '').replace("'", '').strip()
                else:
                    try:
                        value = int(base_statement[start].strip())
                    except:
                        return None
                cond['cond']['value'] = value
                start += 2
                operation['conditions'].append(cond)

        return operation

    def __create(self, base_statement):
        # init info
        operation = {}

        # CREATE DATABASE
        ret = compile(self.__regex_map["CREATE DATABASE"]).findall(" ".join(base_statement))
        if ret:
            operation["type"] = "create_db"
            operation["name"] = ret[0][2]

            return operation
            # operation = {
            #       type: create_db,
            #       name: database_name
            # }

        # CREATE TABLE
        ret = compile(self.__regex_map["CREATE"]).findall(" ".join(base_statement))
        if ret:
            operation["type"] = "create"
            operation["name"] = ret[0][2]
            operation["col"] = {}
            vars = ret[0][3].split(',')
            for var_type in vars:
                detailed = var_type.strip().split(' ')
                operation['col'][detailed[0]] = []
                for i in range(1, len(detailed)):
                    operation['col'][detailed[0]].append(detailed[i])

            print(operation['col'])

            return operation
            # operation = {
            #       type: create,
            #       name: table_name,
            #       col: {
            #           col0_name: [col0_type_list],
            #           ...
            #           ...
            #           }
            # }

        # CREATE INDEX
        ret = compile(self.__regex_map["CREATE INDEX"]).findall(" ".join(base_statement))
        if ret:
            # print(ret)
            operation["type"] = "create_index"
            operation["name"] = ret[0][2]
            operation["table"] = ret[0][4]
            operation["col"] = ret[0][5]

            return operation
            # operation = {
            #       type = create_index,
            #       name = index_name,
            #       table = table_name
            #       col = col_name
            # }

        # ERROR  
        print("Syntax Error(code 2)")
        return

    def __drop(self, statement):
        kinds = statement[1]
        # print(statement)
        if len(statement) < 3:
            print('Syntax Error')
            return

        if kinds.upper() == 'DATABASE':
            return {
                'type': 'drop',
                'kind': 'database',
                'name': statement[2]
            }

        elif kinds.upper() == 'TABLE':
            return {
                'type': 'drop',
                'kind': 'table',
                'name': statement[2]
            }

        elif kinds.upper() == 'INDEX':
            ret = compile(self.__regex_map["DROP INDEX"]).findall(' '.join(statement))
            # print(ret)
            if ret:
                return {
                    'type': 'drop',
                    'kind': 'index',
                    'name': statement[2],
                    'table': ret[0][4]
                }

        print('Syntax Error(code 3)')
        return

    def __use(self, statement):
        return {
            "type": 'use',
            "database": statement[1]
        }

    def __exit(self, _):
        return {
            'type': 'exit'
        }

    def __join(self, base_statement):
        # print(base_statement)
        ret = compile(self.__regex_map["SELECT"]).findall(" ".join(base_statement))[0]
        # print(ret)
        if ret and len(ret) == 4:
            fields = ret[1]
            join_fields = {}
            if 'where' in ret[3]:
                base = ret[3].split('where')[0]
            elif 'WHERE' in ret[3]:
                base = ret[3].split('WHERE')[0]
            else:
                base = ret[3]
            # print(base)
            left = base.strip().split(" ")
            # print('left: ', left)
            join_field = [left[-1], left[-3]]
            # print('join_field: ', join_field)
            for str in join_field:
                table = str.split(".")[0]
                col = str.split(".")[1]
                join_fields[table] = col
            if fields != '*':
                fields = [field.strip() for field in fields.split(',')]

            operation = {
                'type': 'search join',
                'join type': left[1],
                'tables': left[0],
                'fields': fields,
                'join fields': join_fields,
            }

            if 'WHERE' in base_statement or 'where' in base_statement:
                operation['conditions'] = []

                if 'WHERE' in base_statement:
                    where_index = base_statement.index('WHERE')
                else:
                    where_index = base_statement.index('where')

                if 'AND' in base_statement or 'and' in base_statement:
                    operation['conditions_logic'] = 'AND'
                elif 'OR' in base_statement or 'or' in base_statement:
                    operation['conditions_logic'] = 'OR'

                start = where_index + 1
                # print(start, len(statement))
                while start < len(base_statement):
                    # print(start)
                    cond = {}
                    cond['field'] = base_statement[start]
                    cond['cond'] = {}
                    start += 1
                    cond['cond']['operation'] = base_statement[start]
                    start += 1
                    if "'" in base_statement[start] or "\"" in base_statement[start]:
                        value = base_statement[start].replace('"', '').replace("'", '').strip()
                    else:
                        try:
                            value = int(base_statement[start].strip())
                        except:
                            return None
                    cond['cond']['value'] = value
                    start += 2
                    operation['conditions'].append(cond)

            return operation

    def __show(self, statement):
        kinds = statement[1]

        if kinds.upper() == 'DATABASES':
            return {
                'type': 'show',
                'kind': 'databases'
            }

        if kinds.upper() == 'TABLES':
            return {
                'type': 'show',
                'kind': 'tables'
            }

    # ---------------------</editor-fold>---------------------

    # remove the space in sql
    def __remove_space(self, s):
        res = []
        for word in s:
            if word.strip() == '':
                continue
            res.append(word)
        return res
