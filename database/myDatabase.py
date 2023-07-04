from re import findall
from util import myUtil
from bplus_tree.tree import BPlusTree

class MyTable:
    def __init__(self, table_name, cols_map):
        # cols_map: {
        #     col0_name: [col0_type_list],
        #     ...
        #     ...
        #     }

        # table name
        self.name = table_name
        # all columns' name
        self.col_name_list = []
        # all columns' corresponding variable type and constraint
        self.col_type_list = []
        # a dictionary of list, that used to store each columns' data
        self.col_data_map = {}
        # btrees dictionary for create index
        self.btrees_map = {}

        self.__operator_map = {
            '=': self._equal,
            '>': self._bigger,
            '<': self._smaller,
            '>=': self._biggerAndEqual,
            '<=': self._smallerAndEqual,
        }

        self.__math_func_map = {
            'avg': self._select_avg,
            'count': self._select_count,
            'max': self._select_max,
            'min': self._select_min,
            'sum': self._select_sum,
        }

        # self defined index, used if no primary key given
        self.index = 0

        # store all columns' name and type in this class
        self.__init_cols(cols_map)

        # check if the given columns include the primary key, use '__index__' as the primary key if not provided
        self.primary = self.__check_primary()

        # init col_name_list
        for col_name in self.col_name_list:
            self.col_data_map[col_name] = []

        # if primary key provided, this line is just overwrite the previous step; if not, this line will create a new
        # list(key pair in the dictionary)
        self.col_data_map[self.primary] = []

    def __init_cols(self, cols_map):
        for col_name, col_type in cols_map.items():
            self.col_name_list.append(col_name)
            self.col_type_list.append(col_type)

    def __check_primary(self):
        primary = 'no_primary_key'
        # zip()将这两个列表按索引位置配对，使它们的元素一一对应。
        for col_name, col_type in zip(self.col_name_list, self.col_type_list):
            if 'primary' in col_type:
                if primary == 'no_primary_key':
                    primary = col_name
                else:
                    raise Exception("Error! Duplicate primary key set")
        return primary

    def update(self, operation_map):
        if operation_map.get('conditions') is None:
            print('Syntax Error')
            return
        else:
            cols_list = []
            conditions_list = []
            for conditions in operation_map['conditions']:
                cols_list.append(conditions['field'])
                conditions_list.append(conditions['cond'])

            index_list = []

            for col, con in zip(cols_list, conditions_list):
                if con['operation'] not in self.__operator_map:
                    print('No such operation')
                    return
                print('con: ', con, 'col: ', col)
                index = self.condition_filter(con, col)
                index_list.append(index)

        index = index_list[0]
        # print(index_list)
        if 'conditions_logic' in operation_map.keys():
            if operation_map['conditions_logic'] == 'AND':
                for i in range(1, len(index_list)):
                    index = list(set(index).intersection(index_list[i]))
                index.sort()
            elif operation_map['conditions_logic'] == 'OR':
                for i in range(1, len(index_list)):
                    index = list(set(index).union(index_list[i]))
                index.sort()
            # print(index)

        for i in range(len(index_list)):
            tmp = [val for val in index if val in index_list[i]]
        # print(tmp)

        data = operation_map['data']
        for i in data.keys():
            for j in tmp:
                if self.is_number(data[i]):
                    if self.primary == i and int(data[i]) in self.col_data_map[self.primary]:
                        raise Exception("ERROR!!! Duplicate Primary Key Value Exists!")
                    self.col_data_map[i][j] = int(data[i])

                else:
                    if self.primary == i and data[i] in self.col_data_map[self.primary]:
                        raise Exception("ERROR!!! Duplicate Primary Key Value Exists!")
                    self.col_data_map[i][j] = data[i]

    def is_number(self, s):
        # check if string is numbers
        try:
            float(s)
            return True
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False

    def delete(self, operation_map):
        """
        {
            'type': 'delete',
            'table': table_name,
            conditions_logic: OR/AND
            conditions: [{field: col_name, cond: {operation: >or=or ..., value: num}}, {}, {}, ...]
        }
        """
        if operation_map.get('conditions'):
            col_names = []
            conditions_list = []
            for cond in operation_map['conditions']:
                col_names.append(cond['field'])
                conditions_list.append(cond['cond'])

            index_select_list = []
            for col_name, cond in zip(col_names, conditions_list):
                if cond['operation'] not in self.__operator_map:
                    print("No such operation")
                    return
                print(cond, col_name)
                index = self.condition_filter(cond, col_name)
                index_select_list.append(index)
        else:
            print("No conditions")
            return

        if len(index_select_list) == 1:
            self.__delete(index_select_list[0])
        else:
            index = index_select_list[0]
            if operation_map['conditions_logic'] == 'AND':
                for i in range(len(index_select_list)):
                    index = list(set(index).intersection(index_select_list[i]))
                index.sort()
            elif operation_map['conditions_logic'] == 'OR':
                for i in range(len(index_select_list)):
                    index = list(set(index).union(index_select_list[i]))
                index.sort()
            self.__delete(index)
            return

    def __delete(self, delete_index):
        print(self.col_data_map)
        delete_index.sort(reverse=True)
        print(delete_index)
        for index in delete_index:
            for col in self.col_name_list:
                del self.col_data_map[col][index]
            if self.primary == 'no_primary_key':
                del self.col_data_map[self.primary][index]

    def updateIndex(self):
        if self.btrees_map == {}:
            return
        for name in self.btrees_map.keys():
            self.btrees_map[name]['tree'] = BPlusTree()
            for i in range(len(self.col_data_map[name])):
                self.btrees_map[name]['tree'].insert(self.col_data_map[name][i], i)

    def insert(self, datas):
        # {
        #   col0_name: data
        #   col1_name: data
        # }
        if datas is None:
            print('No data insert')
            return
        # print(self.col_name_list)
        if type(datas) is dict:
            for col in datas.keys():
                if datas.get(self.primary) is None:
                    print("No Primary value")
                    return
                elif col not in self.col_name_list:
                    print("no input variable")
                    return
                else:
                    self.col_data_map[col].append(datas[col])
        elif type(datas) is list:
            if len(self.col_name_list) != len(datas):
                print(self.col_name_list, ' ', datas)
                print("Not valid length")
                return
            else:
                for col_name, data in zip(self.col_name_list, datas):
                    if col_name == self.primary and data in self.col_data_map[self.primary]:
                        raise Exception("Duplicate Primary")
                    else:
                        self.col_data_map[col_name].append(data)

        if self.primary == 'no_primary_key':
            self.col_data_map[self.primary].append(self.index)
            self.index += 1

    def createIndex(self, operation_map):
        # check if the columns is in this table
        if operation_map['col'] not in self.col_name_list:
            print("ERROR! No Column Named '%s'" % (operation_map['col']))
            return False
        # init the name and tree
        if operation_map['col'] in self.btrees_map.keys():
            print('Already Exist index on %s' % (operation_map['col']))
            return False
        self.btrees_map[operation_map['col']] = {
            'name': operation_map['name'],
            'tree': BPlusTree()
        }
        print(self.btrees_map)
        # insert index as value where value as key
        for i in range(len(self.col_data_map[operation_map['col']])):
            self.btrees_map[operation_map['col']]['tree'].insert(self.col_data_map[operation_map['col']][i], i)

        return True

    def dropIndex(self, operation_map):
        cols = []
        for key, value in self.btrees_map.items():
            if value['name'] == operation_map['name']:
                cols.append(key)
        if not cols == []:
            for col in cols:
                del self.btrees_map[col]
            print(self.btrees_map)
            return True
        return False

    def select(self, operation_map):
        # 如果有
        # operation = {
        #       type: search,
        #       field: [cols_name_list]
        #       table: table_name,
        #       groupby: col_name,
        #       orderby: col_name,
        #       limit: num
        #       conditions_logic: OR/AND
        #       conditions: [{field: col_name, cond: {operation: >or=or ..., value: num}}, {}, {}, ...]
        # }
        # print('table data: ', self.col_data_map)
        # print('operation_map: ',operation_map)

        # 先提取出选择的列cols
        if operation_map['fields'] == '*':
            selected_cols_list = self.col_name_list
            math_func_list = None
        else:
            # 如果有math(col)，提取出对应的math()和列
            selected_cols_list, math_func_list = self.check_math(operation_map["fields"])

        # 处理查询条件语句conditions
        if operation_map.get('conditions'):
            selected_index_list = []
            # 计算每条conditions的被选择数据在col_list的下标index，返回selected_index_list[index_list]
            for condition in operation_map["conditions"]:
                # 判断操作符operator是否合法
                if condition['cond']["operation"] not in self.__operator_map:
                    print('Error! Cannot Resolve Given Input')
                    return
                selected_index_list.append(self.condition_filter(condition['cond'], condition['field']))
            # print('selected_index_list: ', selected_index_list)
        else:
            # 计算每条conditions的被选择数据在col_list的下标index，如果没有conditions，返回selected_index_list[[0 -> col的长度]]
            selected_index_list = [[i for i in range(len(self.col_data_map[self.col_name_list[0]]))]]

        # set a condition check for only one constraint
        selected_index = selected_index_list[0]
        # print(selected_index_list)
        # print('index_list_select: ', selected_index)
        # 计算selected_index_list[]里多条index_list经过and/or运算后的结果
        if len(selected_index_list) > 1:
            # 每个condition都会产生一个index列表，根据logic选取并集或者交集
            if operation_map['conditions_logic'] == 'AND':
                for i in range(1, len(selected_index_list)):
                    selected_index = list(set(selected_index).intersection(selected_index_list[i]))
                # print(selected_index)
            elif operation_map['conditions_logic'] == 'OR':
                # get intersection
                for i in range(1, len(selected_index_list)):
                    selected_index = list(set(selected_index).union(selected_index_list[i]))
            selected_index.sort()
            # print('selected_index: ', selected_index)
        # print('selected_index: ', selected_index)
        orderby_col_data_list = None
        if operation_map.get('orderby'):
            if operation_map['orderby'] not in self.col_name_list:
                print('Error! Cannot Resolve Given Column: ', operation_map['orderby'])
                return
            else:
                # 提取出需要orderby列的元素
                orderby_col_data_list = self.col_data_map[operation_map['orderby']]

        # 返回对应的结果
        # 存在math_func时
        if math_func_list and not math_func_list == ['']:
            # 存在group by时
            if "groupby" in operation_map.keys():
                # print('selected_index: ', selected_index)
                result_map = self._select_data_groupby(selected_index, selected_cols_list, math_func_list,
                                                       operation_map['groupby'])
                return result_map, None, orderby_col_data_list
            else:
                result_map = self._select_data_math(selected_index, selected_cols_list, math_func_list)
                return result_map, None, None
        # 不存在math_func时
        else:
            if operation_map.get('groupby'):
                raise Exception("ERROR!!! Cannot Run 'GROUP BY' Without Constraint!")

            result_map = self._select_data(selected_index, selected_cols_list)

            # 获取被选择列的type
            result_type_map = {}
            for col_name in result_map.keys():
                result_type_map[col_name] = self.col_type_list[self.col_name_list.index(col_name)]

            return result_map, result_type_map, orderby_col_data_list
            # result_map = {
            #     col_name: [val0, val1,...]
            #     ...
            # }
            # result_type_map = {
            #     col_name: 'col_type'
            # }

    def _select_data(self, selected_index, selected_cols_list):
        result_map = {}
        for index in selected_index:
            for col in selected_cols_list:
                if not result_map.get(col, False):
                    result_map[col] = []
                result_map[col].append(self.col_data_map[col][index])
        return result_map

    def _select_data_groupby(self, index_select, cols_list, math_func_list, groupby_col_name):
        conditon_res_list = []
        for index in index_select:
            conditon_res_list.append(self.col_data_map[groupby_col_name][index])
        # print(conditon_res_list)

        # 去除groupby_col里的重复data
        col_set = list(set(conditon_res_list))

        # print('函数里的index_select', index_select)
        col_select_map = {}
        for data in col_set:
            col_select_map[data] = []
        # 提取col_set中每个data在table里对应的多个index
        for index in index_select:
            for data in col_set:
                if self.col_data_map[groupby_col_name][index] == data:
                    col_select_map[data].append(index)
        # col_select_map = {'YES': [5, 6, 7], 'No': [4, 8]}

        result_map = {
            groupby_col_name: col_set
        }

        # print(result_map)

        # 处理group by的math_func部分
        for col in col_set:
            for i in range(len(cols_list)):
                if result_map.get(math_func_list[i] + '_' + cols_list[i]) is None:
                    result_map[math_func_list[i] + '_' + cols_list[i]] = []
                if not col_select_map[col]:
                    result_map[math_func_list[i] + '_' + cols_list[i]].append(0)
                else:
                    result_map[math_func_list[i] + '_' + cols_list[i]].append(
                        self.__math_func_map[math_func_list[i]](cols_list[i], col_select_map[col])[0])

        return result_map

    def _select_data_math(self, index_select, cols_list, math_func_list):
        result_map = {}
        # check the filter of selected data
        for i in range(len(cols_list)):
            if cols_list[i] == '*':
                field = self.primary
            else:
                field = cols_list[i]
            # print(f"index_select {index_select}")
            # print(f"filter {filter[i]}, fields:{fields[i]}")
            if math_func_list[i] in self.__math_func_map.keys():
                result_map[f"{math_func_list[i]}_{cols_list[i]}"] = \
                    self.__math_func_map[math_func_list[i]](field, index_select)

        return result_map

    def _select_avg(self, field, index):
        _sum = 0
        for i in index:
            _sum += self.col_data_map[field][i]

        return [_sum / len(index)]

    def _select_count(self, field, index):
        return [len(index)]

    def _select_max(self, field, index):
        _max = self.col_data_map[field][index[0]]
        for i in index:
            if _max < self.col_data_map[field][i]:
                _max = self.col_data_map[field][i]
        return [_max]

    def _select_min(self, field, index):
        _min = self.col_data_map[field][index[0]]
        for i in index:
            if _min > self.col_data_map[field][i]:
                _min = self.col_data_map[field][i]

        return [_min]

    def _select_sum(self, field, index):
        _sum = 0
        for i in index:
            _sum += self.col_data_map[field][i]
        # print(_sum)
        return [_sum]

    def condition_filter(self, cond, field):
        try:
            return self.__operator_map[cond["operation"]](cond, field)
        except Exception:
            print('Error! Cannot Resolve Given Input')
            return

    def _equal(self, cond, col):
        if col in self.btrees_map.keys():
            indexes = self.btrees_map[col]['tree'].search('=', self._format(col, cond["value"]))
            return indexes
        return myUtil.equal_list(self.col_data_map[col], self._format(col, cond["value"]))

    def _bigger(self, cond, col):
        if col in self.btrees_map.keys():
            print('use index search')
            indexes = self.btrees_map[col]['tree'].search('>', self._format(col, cond["value"]))
            return indexes
        return myUtil.more_list(self.col_data_map[col], self._format(col, cond["value"]))

    def _smaller(self, cond, col):
        if col in self.btrees_map.keys():
            indexes = self.btrees_map[col]['tree'].search('<', self._format(col, cond["value"]))
            return indexes
        return myUtil.less_list(self.col_data_map[col], self._format(col, cond["value"]))

    def _biggerAndEqual(self, cond, col):
        if col in self.btrees_map.keys():
            indexs = self.btrees_map[col]['tree'].search('>=', self._format(col, cond["value"]))
            return indexs
        return myUtil.more_equal_list(self.col_data_map[col], self._format(col, cond["value"]))

    def _smallerAndEqual(self, cond, col):
        if col in self.btrees_map.keys():
            indexs = self.btrees_map[col]['tree'].search('<=', self._format(col, cond["value"]))
            return indexs
        return myUtil.less_equal_list(self.col_data_map[col], self._format(col, cond["value"]))
        # return [index for index, v in enumerate(self.data[col]) if v <= float(cond["value"])]

    def _format(self, col, value):
        if self.col_type_list[self.col_name_list.index(col)][0] == 'int':
            return int(value)
        elif self.col_type_list[self.col_name_list.index(col)][0] == 'float':
            return float(value)
        return value

    def check_math(self, fields):
        math_func_list = []
        cols_list = []
        if "(" not in fields[0]:
            return fields, ['']

        for col in fields:
            if "avg" in col.lower():
                math_func_list.append('avg')
            elif "count" in col.lower():
                math_func_list.append('count')
            elif "max" in col.lower():
                math_func_list.append('max')
            elif "min" in col.lower():
                math_func_list.append('min')
            elif "sum" in col.lower():
                math_func_list.append('sum')
            else:
                math_func_list.append("")
            cols_list.append(findall(r"\((.*?)\)", col)[0])

        if "" in math_func_list:
            for ff in math_func_list:
                if ff != "":
                    raise Exception(f"ERROR!!! Cannot select both Column and {ff}")
        return cols_list, math_func_list
