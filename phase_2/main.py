from platform import node
import pprint
from os import curdir
from re import L
from select import select
import Pyro4
from mysql.connector import cursor
import sqlparse
import mysql.connector
from mysql.connector.constants import ServerFlag
import base64
from prettytable import PrettyTable


class HomeDatabase():

    def __init__(self):
        self.home = mysql.connector.connect(
            user="Maxslide", password="iiit123", host="localhost", database="QuarantinedAgain")
        self.cursor = self.home.cursor()

    def Create_temp_table(self, table_name, final_output, columns):
        # Need to decide how to name the joins etc
        table_name = "temp_something"
        # complete the syntax for create table
        creat_table = "CREATE TABLE "+table_name+" "
        self.cursor.execute(creat_table)
        for i in final_output:
            # complete as required by modifying the function a bit, we need to know the column names also which i dont think comes in the execute query output
            # need to figure this out
            insert_into_table = "INSERT INTO " + table_name+" VALUES"
        # At this point we have a temporary table created
        return

    def execute_query(self, query):
        self.cursor.execute(query)
        # print(self.cursor)
        # self.home.commit()
        output_list = []
        for i in self.cursor:
            output_list.append(i)
        return output_list

    def execute_query_final(self,query):
        self.cursor.execute(query)
        output_list = []
        temp = []
        for i in self.cursor.description:
            temp.append(i[0])
        output_list.append(temp)
        for i in self.cursor:
            output_list.append(i)
        return output_list


    def Create_Exectution_Table(self):
        query = "DROP TABLE IF EXISTS Execution_Table;"
        self.cursor.execute(query)
        query = "DROP TABLE IF EXISTS Join_Selectivity"
        self.cursor.execute(query)
        query = "CREATE TABLE Execution_Table ( Table_name varchar(200), Temp int, Site_id int );"
        self.cursor.execute(query)
        query = "CREATE TABLE Join_Selectivity ( Table_name varchar(200), Column_Name varchar(200),  Selectivity_Factor float , Cardinality int );"
        self.cursor.execute(query)
        self.home.commit()
        return
    
    def insert_to_table(self, table_name, values):

        print("In insert to table 213")
        insert = "INSERT INTO " + table_name + " VALUES "
        for i in values[:-1]:
            insert += str(i) + ", "
        insert += str(values[-1]) + " ;"
        print(insert)
        self.cursor.execute(insert)
        self.home.commit()


@Pyro4.expose
class Client():

    def __init__(self, link1, link2,link3):
        self.site_213_link = link1
        self.site_214_link = link2
        self.site_215_link = link3

        self.site_213 = Pyro4.Proxy(link1)
        self.site_214 = Pyro4.Proxy(link2)
        self.site_215 = Pyro4.Proxy(link3)
        self.home = mysql.connector.connect(
            user="Maxslide", password="iiit123", host="localhost", database="QuarantinedAgain")
        self.cursor = self.home.cursor()

    def execute_site_214(self,query):
        self.site_214.execute_query(query)

    def execute_site_213(self,query):
        self.site_213.execute_query(query)
    
    def execute_site_213(self,query):
        self.site_213.execute_query(query)

    def Two_Phase_Commit(self,query):
        # We need to send query to each site to update based on allocation schema
        Table_Name = "Table_Name"
        q = "select Table_Name, Frag_Name, Site_Id from Tables as T, Frag_Table as F, Allocation as A Where T.Table_Id = F.Table_Id and F.Frag_Id = A.Frag_Id  and Table_Name = " +Table_Name+";"
        self.cursor.execute(q)
        print('begin_commit')
        for i in self.cursor:
            #table_name = i[0]
            #Frag_Name = i[1]
            #Site_Id = i[2]

            pass
        return

        

link1 = "PYRO:Graph@10.3.5.213:9090"
link2 = "PYRO:Graph@10.3.5.214:9090"
link3 = "PYRO:Graph@10.3.5.215:9090"
obj_1 = Client(link1,link2,link3)
print(obj_1.site_213.check_connection())
print(obj_1.site_214.check_connection())
print(obj_1.site_215.check_connection())

site_obj = {
    1 : obj_1.site_215,
    2 : obj_1.site_214,
    3 : obj_1.site_213
}
site_link = {
    1 : link3,
    2 : link2,
    3 : link1
}

# obj_1.site_214.Send_Create_Table("TestingTable",obj_1.site_213_link)
# obj_1.site_214.Send_Create_Table("TestingTable",obj_1.site_215_link)

obj = HomeDatabase()
# obj.execute_query('Select * From Frag_Table')
print("Enter Query : ")
query = input()
query = query.strip(';')
# conevrt this into query tree


parsed_query = sqlparse.parse(sqlparse.format(
    query, keyword_case='upper'))[0].tokens
print('\n\n\nPARSED QUERY : ')
print(sqlparse.format(query, reindent=True, keyword_case='upper'))
print('\n\n-----------------------------------------------------------------------------------------\n\n')

print("Note : In all of the query tree below, the root node will be the highest numbered node, the tree is built using bottom up approach. \n Please node the Condition key in each of the node (which is a dictionary), defines the selection statements for that node.")
print("note that the select statements which are pushed down the tree (in our case up the indexing, since built via bottom up), will go into the list of this condition.")
print("Refer to a detailed example of a big query mentioned in the pdf.\n\n")

token_list = []
for i in parsed_query:
    if not i.is_whitespace:
        try:
            token = []
            for j in i.get_identifiers():
                token.append(j)
            token_list.append(token)
        except:
            token_list.append(i)
# pprint.pprint(token_list)
# for i in token_list:
#     print(str(i))
    # if(type(i) == list):
    #     for j in i:
    #         print(j)


# Where -> Joins, Condition
# [Select, From, Join, Join]
# [t1,t2,t3, join(t1,t2,condition),join(t3,codition), Condition(table_name,condition), Condition(table_name,condition)...., SELECT(Columns)]
hash_table = {}
tree_nodes = []
edges = []
global_where_tokens = []


def get_table_names(token_list):
    for i in range(len(token_list)):
        if str(token_list[i]) == 'FROM':
            for j in token_list[i+1]:
                hash_table[str(j)] = len(tree_nodes)
                tree_nodes.append({
                    'Key': 'Table',
                    'Value': str(j),
                    'Condition': []
                })
            break

Columns_Used = []
def check_join(token):
    if ('=' in token and '.' in token):
        conditions = token.strip().split('=')
        join = []
        for i in conditions:
            check = i.strip().split('.')
            # print(check)
            try:
                table = hash_table[check[0].strip()]
                table = check[0].strip()
                column = check[1].strip()
                Columns_Used.append(column)
                join.append([table, column])
            except:
                pass
        # print(join)
        if len(join) == 2:
            return join
    return []


def get_conditions(token_list):
    # join = []
    others = []
    # if len(hash_table) > 1:
    # we need to get the join conditions now, this can happen from the where wala clause:
    for i in token_list:
        if 'WHERE' in str(i):
            i = str(i)
            _, condition = i.strip().split('WHERE')
            condition = condition.strip()
            where_tokens = condition.split('AND')
            # global_where_tokens = where_tokens
            for bleh in where_tokens:
                global_where_tokens.append(bleh)
            for j in where_tokens:
                join = check_join(j.strip())
                if len(join) == 2:
                    # print('here')
                    flag = 0
                    for l in range(len(tree_nodes)):
                        k = tree_nodes[l]
                        if(k['Key'] == 'Join'):
                            for j in k['Condition']:
                                # print(j,join)
                                if(join[0][0] == j[0] or join[1][0] == j[0]):
                                    flag = 1
                                    edges.append([len(tree_nodes), l])
                                    if (join[0][0] != j[0]):
                                        edges.append(
                                            [len(tree_nodes), hash_table[join[0][0]]])
                                    else:
                                        edges.append(
                                            [len(tree_nodes), hash_table[join[1][0]]])
                                    tree_nodes.append({
                                        'Key': 'Join',
                                        'Value': str(join[0][0]) + '_' + str(join[1][0]),
                                        'Condition': join
                                    })
                        if flag == 1:
                            break
                    if flag == 0:
                        # print(join)
                        edges.append([len(tree_nodes), hash_table[join[0][0]]])
                        edges.append([len(tree_nodes), hash_table[join[1][0]]])
                        tree_nodes.append({
                            'Key': 'Join',
                            'Value': str(join[0][0]) + '_' + str(join[1][0]),
                            'Condition': join
                        })
                else:
                    others.append(j.strip())
            break
    return others


def get_Project(token_list):
    for l in range(len(token_list)):
        i = token_list[l]
        if str(i) == 'SELECT':
            project_list = []
            if type(token_list[l+1]) != list:
                project_list.append(str(token_list[l+1]))
                return project_list
            else:
                for j in token_list[l+1]:
                    project_list.append(str(j))
                return project_list


get_table_names(token_list)
join_flag = len(tree_nodes)
conditions_left = get_conditions(token_list)
edges.append([len(tree_nodes) - 1, len(tree_nodes)])
tree_nodes.append({
    'Key': 'Select',
    'Condition': conditions_left
})
flag = 0
# print(token_list)
i = 0
while i < len(token_list):
    # print(token_list[i].get_type())
    if (flag == 1 and i + 1 < len(token_list)) or 'GROUP BY' in str(token_list[i]) or 'HAVING' in str(token_list[i]):
        condition = []
        if type(token_list[i+1]) != list:
            condition.append(str(token_list[i+1]))
        else:
            for j in token_list[i+1]:
                condition.append(str(j))
        edges.append([len(tree_nodes) - 1, len(tree_nodes)])
        tree_nodes.append({
            'Key': str(token_list[i]),
            'Condition': condition
        })
        i = i+1
    try:
        # print(token_list[i])
        if 'WHERE' in str(token_list[i]):
            flag = 1
    except:
        pass
    i += 1
edges.append([len(tree_nodes) - 1, len(tree_nodes)])

tree_nodes.append({
    'Key': 'Project',
    'Condition': get_Project(token_list)
})

# pprint.pprint(tree_nodes)
print('Decomposed Query Tree\n')
for i in range(len(tree_nodes)):
    print(str(i), "->", tree_nodes[i])

print('Edge List -> ')

print(edges)


#  Need to move the select statements down

delete_list = []
for i in tree_nodes:
    if i['Key'] == 'Select':
        for j in range(len(i['Condition'])):
            if 'OR' in i['Condition'][j]:
                continue
            # operator_list = ['=','>=','<=','>','<']
            tok = i['Condition'][j].strip().split()
            # print(tok)
            for k in tok:
                if('.' in k):
                    try:
                        table, column = k.strip().split('.')
                        ind = hash_table[table]
                        tree_nodes[ind]['Condition'].append(i['Condition'][j])
                        delete_list.append(i['Condition'][j])
                    except:
                        print('Incorrect Query')
                        exit()
        break

for i in range(len(tree_nodes)):
    if(tree_nodes[i]['Key'] == 'Select'):
        for j in delete_list:
            tree_nodes[i]['Condition'].remove(j)
        break

print('\n\n-----------------------------------------------------------------------------------------\n\n')

print('Decomposed Query Tree with Heuristic optimisations\n')

for i in range(len(tree_nodes)):
    print(str(i), "->", tree_nodes[i])

print('Edge List -> ')

print(edges)

# Rewritten Query tree

# Localisation
# {
#     'Key'
#     'Condition'
#     'Value'
# }


# [{'Condition': [], 'Key': 'Table', 'Value': 't1'},
#  {'Condition': ["t2.name == 'Manas'"], 'Key': 'Table', 'Value': 't2'},
#  {'Condition': [], 'Key': 'Table', 'Value': 't3'},
#  {'Condition': [['t1', 'id'], ['t2', 'id']], 'Key': 'Join', 'Value': 't1_t2'},
#  {'Condition': [['t2', 'name'], ['t3', 'name']],
#   'Key': 'Join',
#   'Value': 'To_Another_Join'},
#  {'Condition': ["(t1.col > 5 OR t3.city == 'Bangalore')"], 'Key': 'Select'},
#  {'Condition': ['A', 'B', 'C'], 'Key': 'Project'}]

# +----------+------------+------------+-----------+----------+---------+-----------+------------------------+----------+
# | Table_Id | Table_Name | Columns_No | Frag_Type | Frags_No | Frag_Id | Frag_Name | Frag_Condition         | Table_Id |
# +----------+------------+------------+-----------+----------+---------+-----------+------------------------+----------+
# |        3 | Course     |          4 | HF        |        3 |       7 | Course1   |  Course_Type = "CSE"   |        3 |
# |        3 | Course     |          4 | HF        |        3 |       8 | Course2   |  Course_Type = "ECE"   |        3 |
# |        3 | Course     |          4 | HF        |        3 |       9 | Course3   |  Course_Type = "HSME"  |        3 |
# +----------+------------+------------+-----------+----------+---------+-----------+------------------------+----------+

if(join_flag == 1):
    pass

localised_tree_nodes = []
localised_edges = []


def Check_Anti(value, check_val, op, op_check):
    if op == '=':
        # print(check_val, value)
        try:
            v = float(value)
            if op_check == '=':
                if check_val == value:
                    return 0
                else:
                    return 1
            else:
                mini = -1000000000
                maxi = 1000000000
                if op_check == '>':
                    mini = float(check_val) + 1
                elif op_check == '>=':
                    mini = float(check_val)
                elif op_check == '<':
                    maxi = float(check_val) - 1
                else:
                    maxi = float(check_val)
                if (v >= mini and v <= maxi):
                    return 0
                else:
                    return 1

        except:
            # print("Here in the except block",check_val.strip('"').strip('"'), value.strip('"').strip('"'))
            if check_val.strip('"').strip("'") == value.strip('"').strip("'"):
                # print("CHECKED HERE")
                return 0
            else:
                # print("Check here for anti true")
                return 1
    else:
        mini_v = -1000000000
        maxi_v = 1000000000
        if op == '>':
            mini_v = float(value) + 1
        elif op == '>=':
            mini_v = float(value)
        elif op == '<':
            maxi_v = float(value) - 1
        else:
            maxi_v = float(value)

        mini = -1000000000
        maxi = 1000000000
        if op_check == '>':
            mini = float(check_val) + 1
        elif op_check == '>=':
            mini = float(check_val)
        elif op_check == '<':
            maxi = float(check_val) - 1
        elif op_check == '<=':
            maxi = float(check_val)
        else:
            mini = float(check_val)
            maxi = float(check_val)

        if(maxi_v < mini or mini_v > maxi):
            return 1
        else:
            return 0


project_columns = []
for i in tree_nodes:
    if i['Key'] == 'Project':
        project_columns = i['Condition']

# print(project_columns)


def Assign_frag():
    hash_frag = {}
    for i in tree_nodes:
        if i['Key'] == 'Table':
            # print(i['Value'])
            hash_frag[i['Value']] = []
            frag_list = obj.execute_query(
                "select * From Tables , Frag_Table Where Tables.Table_Id = Frag_Table.Table_Id AND Table_Name = '" + i['Value'].strip()+"';")
            for j in frag_list:
                frag_type = j[3]
                conditions = j[7]
                # print(frag_type,conditions)
                flag = 0
                if frag_type == 'HF':
                    conditions = conditions.split('AND')
                    for k in conditions:
                        l = k.split('OR')
                        op_list = ['>=', '<=', '>', '<', '=']
                        or_flag_anti = 0
                        for con in l:
                            pre_flag = 0
                            op = ""
                            for operator in op_list:
                                if con.find(operator) != -1:
                                    op = operator
                                    break
                            column, value = con.split(op)
                            column = column.strip()
                            value = value.strip()
                            # print("HERE", i['Condition'])
                            col_check_flag = 1
                            for check_con in i['Condition']:
                                check = check_con.split('.')[1].strip()
                                # print("Check_Cond", check)
                                op_check = ""
                                for operator in op_list:
                                    if check.find(operator) != -1:
                                        op_check = operator
                                        break
                                check_col, check_val = check.split(op_check)
                                check_col = check_col.strip()
                                check_val = check_val.strip()
                                if check_col == column:
                                    # print("Equal_Column_Check ",column,value,check_val)
                                    v = Check_Anti(
                                        value, check_val, op, op_check)
                                    if v == 1:
                                        pre_flag = 1
                            # print("Checking pre flag -> -> ", con, pre_flag, i['Condition'])
                            or_flag_anti += pre_flag
                        # print("checking or flag", k, or_flag_anti)
                        # sum of or flagt
                        if or_flag_anti == len(l):
                            flag = 1
                    if flag == 0:
                        hash_frag[i['Value']].append(len(localised_tree_nodes))
                        newcon = []
                        for cn in i['Condition']:
                            coni = cn.split('.')
                            coni[0] = j[6]
                            ch = ""
                            for hg in coni[:-1]:
                                ch += hg +'.'
                            ch += coni[-1]
                            newcon.append(ch)
                            
                        localised_tree_nodes.append({
                            'Key': 'Table_Fragment',
                            'Value': j[6],
                            'Table_Name': i['Value'],
                            'Condition': newcon
                        })

                elif frag_type == 'DHF':
                    pass
                else:
                    columns_vf = conditions.strip().split()
                    flag = 1
                    for col in project_columns:
                        if col in columns_vf:
                            flag = 0
                    for col in Columns_Used:
                        if col in columns_vf:
                            flag = 0
                    if flag == 0:
                        hash_frag[i['Value']].append(len(localised_tree_nodes))
                        newcon = []
                        for cn in i['Condition']:
                            coni = cn.split('.')
                            coni[0] = j[6]
                            ch = ""
                            for hg in coni[:-1]:
                                ch += hg +'.'
                            ch += coni[-1]
                            newcon.append(ch)
                        localised_tree_nodes.append({
                            'Key': 'Table_Fragment',
                            'Value': j[6],
                            'Table_Name': i['Value'],
                            'Condition': newcon
                        })

    to_remove = []
    for i in tree_nodes:
        if i['Key'] == 'Table':
            # print(i['Value'])
            frag_list = obj.execute_query(
                "select * From Tables , Frag_Table Where Tables.Table_Id = Frag_Table.Table_Id AND Table_Name = '" + i['Value'].strip()+"';")
            union_list = []
            union_con = set()
            for j in frag_list:
                frag_type = j[3]
                conditions = j[7]
                # print(frag_type,conditions)
                flag = 0
                if frag_type == 'DHF':
                    _, _, parent = conditions.strip().split()
                    p_flag = 0
                    # print("JOIN PARENT DHF -> ",parent)
                    for k in range(len(localised_tree_nodes)):
                        # print(localised_tree_nodes[k])
                        if(localised_tree_nodes[k]['Value'] == parent):
                            p_flag = 1
                            hash_frag[i['Value']].append(
                                len(localised_tree_nodes))
                            newcon = []
                            for cn in i['Condition']:
                                coni = cn.split('.')
                                coni[0] = j[6]
                                ch = ""
                                for hg in coni[:-1]:
                                    ch += hg +'.'
                                ch += coni[-1]
                                newcon.append(ch)
                            localised_tree_nodes.append({
                                'Key': 'Table_Fragment',
                                'Value': j[6],
                                'Table_Name': i['Value'],
                                'Condition': newcon
                            })
                            localised_edges.append(
                                [len(localised_tree_nodes), len(localised_tree_nodes) - 1])
                            localised_edges.append(
                                [len(localised_tree_nodes), k])
                            union_list.append(len(localised_tree_nodes))
                            union_con.add(
                                localised_tree_nodes[k]['Table_Name'])
                            union_con.add(i['Value'])
                            j_con = ""
                            value_join = ""
                            # print("Global where tokens",global_where_tokens)

                            for tok in global_where_tokens:
                                joins = check_join(tok)
                                if(len(joins) == 2):
                                    t1 = joins[0][0]
                                    t2 = joins[1][0]
                                    # print("HERE PRINTING -> ",i['Value'],localised_tree_nodes[k]['Table_Name'])
                                    if((t1 == i['Value'] or t2 == i['Value']) and (t2 == localised_tree_nodes[k]['Table_Name'] or t1 == localised_tree_nodes[k]['Table_Name'])):
                                        to_remove.append(tok)
                                        j_con = localised_tree_nodes[k]['Value'] + "."
                                        value_join = localised_tree_nodes[k]['Value'] + "_" + j[6]
                                        if(localised_tree_nodes[k]['Table_Name'] == t1):
                                            j_con += joins[0][1]
                                        else:
                                            j_con += joins[1][1]
                                        j_con += " = " + j[6] + "."
                                        if(i['Value'] == t1):
                                            j_con += joins[0][1]
                                        else:
                                            j_con += joins[1][1]
                                        break

                            localised_tree_nodes.append({
                                'Key': 'Join',
                                'Value': value_join,
                                'Condition': j_con,
                                'Union_Con': []
                            })
                    if(p_flag == 0):
                        hash_frag[i['Value']].append(len(localised_tree_nodes))
                        newcon = []
                        for cn in i['Condition']:
                            coni = cn.split('.')
                            coni[0] = j[6] + '.'
                            ch = ""
                            for hg in coni[:-1]:
                                ch += hg +'.'
                            ch += coni[-1]
                            newcon.append(ch)
                        localised_tree_nodes.append({
                            'Key': 'Table_Fragment',
                            'Value': j[6],
                            'Table_Name': i['Value'],
                            'Condition': newcon
                        })

                else:
                    if(len(union_list) != 0):
                        print("Union List", union_list)
                        val = "Union_"
                        for un in union_list:
                            localised_edges.append([len(localised_edges), un])
                            val += localised_tree_nodes[un]['Value'] + "_"
                        val = val.strip('_')
                        localised_tree_nodes.append({
                            'Key': 'Union',
                            'Value': val,
                            'Condition': list(union_con),
                            'Union_Con' : list(union_con)
                        })
                        union_list = []
                        union_con = set()

            if(len(union_list) != 0):
                print("Union List", union_list)
                val = "Union_"
                for un in union_list:
                    localised_edges.append([len(localised_tree_nodes), un])
                    val += localised_tree_nodes[un]['Value'] + '_'
                val = val.strip('_')
                localised_tree_nodes.append({
                    'Key': 'Union',
                    'Value': val,
                    'Condition': list(union_con),
                    'Union_Con' : list(union_con)
                })
                union_list = []
                union_con = set()

    # print(hash_frag)
    for i in global_where_tokens:
        if i not in to_remove:
            joins = check_join(i)
            if len(joins) == 2:
                # print('Printing the join condition', i)
                t1 = -1
                t2 = -1
                join_con = []
                for k in range(len(localised_tree_nodes)):
                    if localised_tree_nodes[k]['Key'] == 'Union':
                        if joins[0][0] in localised_tree_nodes[k]['Condition']:
                            t1 = k
                            for loc in localised_tree_nodes[k]['Condition']:
                                join_con.append(loc)
                        if joins[1][0] in localised_tree_nodes[k]['Condition']:
                            t2 = k
                            for loc in localised_tree_nodes[k]['Condition']:
                                join_con.append(loc)
                    if localised_tree_nodes[k]['Key'] == 'Join':
                        if joins[0][0] in localised_tree_nodes[k]['Union_Con']:
                            t1 = k
                            for loc in localised_tree_nodes[k]['Union_Con']:
                                join_con.append(loc)
                        if joins[1][0] in localised_tree_nodes[k]['Union_Con']:
                            t2 = k
                            for loc in localised_tree_nodes[k]['Union_Con']:
                                join_con.append(loc)
                if(t1 == -1):
                    condition_tab = []
                    join_con.append(joins[0][0])
                    for tab in tree_nodes:
                        if tab['Key'] == 'Table' and tab['Value'] == joins[0][0]:
                            condition_tab = tab['Condition']
                            break
                    val = "Union_Frag_"
                    t_name = ""
                    f = 0
                    for k in hash_frag[joins[0][0]]:
                        if f == 0:
                            t_name = localised_tree_nodes[k]['Table_Name']
                            val += localised_tree_nodes[k]['Table_Name']
                            f = 1
                        localised_edges.append([len(localised_tree_nodes), k])
                    t1 = len(localised_tree_nodes)
                    localised_tree_nodes.append({
                        'Key': 'Union_Frag',
                        'Value': val,
                        'Condition': condition_tab,
                        'Union_Con': [t_name]
                    })
                if(t2 == -1):
                    condition_tab = []
                    join_con.append(joins[1][0])
                    for tab in tree_nodes:
                        if tab['Key'] == 'Table' and tab['Value'] == joins[1][0]:
                            condition_tab = tab['Condition']
                            break
                    val = "Union_Frag_"
                    t_name = ""
                    f = 0
                    for k in hash_frag[joins[1][0]]:
                        if f == 0:
                            val += localised_tree_nodes[k]['Table_Name']
                            t_name = localised_tree_nodes[k]['Table_Name']
                            f = 1
                        localised_edges.append([len(localised_tree_nodes), k])
                    t2 = len(localised_tree_nodes)
                    localised_tree_nodes.append({
                        'Key': 'Union_Frag',
                        'Value': val,
                        'Condition': condition_tab,
                        'Union_Con': [t_name]
                    })
                localised_edges.append([len(localised_tree_nodes), t1])
                localised_edges.append([len(localised_tree_nodes), t2])
                cond = i.split('=')
                new_cond = [cond[0], cond[1]]
                l = 0
                for st in cond :
                    name,c = st.split('.')
                    if(name.strip() in localised_tree_nodes[t1]['Union_Con']):
                        name = localised_tree_nodes[t1]['Value']
                    elif (name.strip() in localised_tree_nodes[t2]['Union_Con']):
                        name = localised_tree_nodes[t2]['Value']
                    new_cond[l] = name + "." + c
                    l += 1
                    

                localised_tree_nodes.append({
                    'Key': 'Join',
                    'Value': joins[0][0] + '_' + joins[1][0],
                    'Condition': new_cond[0] + " = " + new_cond[1],
                    'Union_Con': join_con
                })
    return

try:
    Assign_frag()
except:
    print("Incorrect Query")
    exit(0)
flag = 0
if(join_flag == 1):
    # print(tree_nodes)
    for i in range(len(localised_tree_nodes)):
        localised_edges.append([i,len(localised_tree_nodes)])
    val = localised_tree_nodes[len(localised_tree_nodes) - 1]['Value']
    localised_tree_nodes.append({
        'Key': 'Union_Frag',
        'Value': "Union_Frag_" + val,
        'Condition': []
    })

for i in tree_nodes:
    if i['Key'] == 'Select' or flag == 1:
        flag = 1
        localised_edges.append(
            [len(localised_tree_nodes), len(localised_tree_nodes) - 1])
        localised_tree_nodes.append(i)
print('\n\n-----------------------------------------------------------------------------------------\n\n')

print('Final Optimised Localised Query Tree\n')

for i in range(len(localised_tree_nodes)):
    print(str(i), "->", localised_tree_nodes[i])

print('Edge List -> ')
print(localised_edges)

# Select reserve_id,name,city,price from Room, Guest, Reserve Where Room.reserve_id = Reserve.reserve_id and Room.reserve_id = Guest.reserve_id and Room.city = 'Mumbai' and Guest.guest_id < 20 and Room.reserve_id > 2 Group by name,price Having price > 3


temp_tables = []
queries = []
num = len(localised_tree_nodes) - 1
nodes = localised_tree_nodes
def remove_temp():
    query = "Select * From Execution_Table;"
    out = obj.execute_query(query)
    for i in out:
        if(i[1] == 1):
            print("DROP TABLE IF EXISTS "+i[0].strip()+";")
            a = site_obj[i[2]].execute_query("DROP TABLE IF EXISTS "+i[0].strip()+";")
        else :
            continue
    print("FINISHED")
    return

# remove_temp()
    
obj.Create_Exectution_Table()
def Key_col(table):
    query = "Select Frag_Condition from Frag_Table where Frag_Name = '" + table + "';"
    out = obj.execute_query(query)
    papa = out[0][0].strip().split()[0].strip()
    print(out)
    print("Key col",papa )
    return papa

def Frag_Type(table):
    query = "Select Frag_Type from Tables where Table_Name = '" + table + "';"
    out = obj.execute_query(query)
    # [('val',)]
    print(out)
    if(out[0][0].strip()=="DHF" or out[0][0].strip()=="HF"):
        return True
    else:
        return False

def create_graph(n,edges):
    adj = []
    for i in range(n):
        temp = []
        for j in range(n):
            temp.append(0)
        adj.append(temp)
    vis = []
    for i in range(n):
        vis.append(0)
    # print(adj)
    for edge in edges:
        # print(edge)
        u = edge[0]
        v = edge[1]
        adj[u][v] = 1
        adj[v][u] = 1
    return adj,vis

adj,vis = create_graph(num+1, localised_edges)
def get_site(table):
    query = "Select Site_Id from Allocation,Frag_Table where Allocation.Frag_Id = Frag_Table.Frag_Id AND Frag_Name = '" + table + "';"
    out = obj.execute_query(query)
    sites = []
    for i in range(len(out)):
        sites.append(out[i][0])
    return sites

def get_site_temp(table):
    query = "Select Site_id from Execution_Table where Table_name = '" + table + "';"
    out = obj.execute_query(query)
    sites = []
    for i in range(len(out)):
        sites.append(int(out[i][0]))
    return sites

def get_size(site_id,table):
    query = "Select COUNT(*) From "+table+";"
    print(query)
    print(site_id)
    out = site_obj[site_id].execute_query_output(query)
    return int(out[0][0])


def dfs(n):
    opt = nodes[n]['Key']
    if opt == 'Table_Fragment':
        vis[n] = 1
        sites = get_site(nodes[n]['Value'])
        ins = []
        for i in sites:
            tup = (nodes[n]['Value'],0,i)
            ins.append(tup)
        obj.insert_to_table('Execution_Table',ins)
        return nodes[n]
    
    elif opt == 'Union_Frag':
        child = []
        vis[n]=1
        for i in range(num+1):
            if vis[i] == 0 and adj[n][i] == 1:
                child.append(dfs(i))
        
        query_site = 0
        if(Frag_Type(child[0]['Table_Name'])):
            query = 'Create Table ' + nodes[n]['Value'] + ' AS ('
            select = 'Select * From '
            where = ' Where '
            union = ' Union '
            And = ' And '
            
            for i in range(len(child)-1):
                query += select
                frag_name = child[i]['Value']
                frag_cond = child[i]['Condition']
                query += frag_name 
                cond = len(frag_cond)
                if cond:
                    query += where
                    for j in range(cond-1):
                        query += frag_cond[j] 
                        query += And
                    query += frag_cond[cond-1]
                query += union
                
            i = len(child)-1
            query += select
            frag_name = child[i]['Value']
            frag_cond = child[i]['Condition']
            query += frag_name 
            cond = len(frag_cond)
            if cond:
                query += where
                for j in range(cond-1):
                    query += frag_cond[j] 
                    query += And
                query += frag_cond[cond-1]

            query += ');'
            # Sending table
            dict_child = {}
            hash_site = {1 : 0, 2:0 , 3:0}
            for i in child:
                dict_child[i['Value']] = get_site_temp(i['Value'])

            cost_site = {1: 0, 2:0,3:0}
            for keys in hash_site:
                for chil in dict_child:
                    if keys not in dict_child[chil]:
                        cost_site[keys] += get_size(dict_child[chil][0],chil)
            min_key = 1
            min_sum = cost_site[1]
            for keys in cost_site:
                if(cost_site[keys] < min_sum):
                    min_sum = cost_site[keys]
                    min_key = keys
            
            for chil in dict_child:
                if min_key not in dict_child[chil]:
                    si = dict_child[chil][0]
                    site_obj[si].Send_Create_Table(chil,site_link[min_key])
                    ins = []
                    ins.append((chil,1,min_key))
                    obj.insert_to_table('Execution_Table', ins)
            # Query to execute on min_key
            query_site = min_key
            # End sending table
            
        else:
            query = 'Create Table ' + nodes[n]['Value'] + ' AS (Select * From '
            inner_join = ' Inner Join '
            on = ' ON '
            using = ' USING '
            equal = ' = '
            key_col = Key_col(child[0]['Value'])
            if(len(child) >= 2):

                for i in range(1,len(child)):
                    query += '('
                
                query += child[0]['Value'] + inner_join + child[1]['Value'] +  using
                query += '(' +key_col +')'

                for i in range(2,len(child)):
                    query += ')' + inner_join + child[i]['Value'] + using
                    query += '('+key_col+')'
                query += ')'
            else:
                query += child[0]['Value']
            query += ');'

            dict_child = {}
            hash_site = {1 : 0, 2:0 , 3:0}
            for i in child:
                dict_child[i['Value']] = get_site_temp(i['Value'])

            cost_site = {1: 0, 2:0,3:0}
            for keys in hash_site:
                for chil in dict_child:
                    if keys not in dict_child[chil]:
                        cost_site[keys] += get_size(dict_child[chil][0],chil)
            min_key = 1
            min_sum = cost_site[1]
            for keys in cost_site:
                if(cost_site[keys] < min_sum):
                    min_sum = cost_site[keys]
                    min_key = keys
            
            for chil in dict_child:
                if min_key not in dict_child[chil]:
                    si = dict_child[chil][0]
                    site_obj[si].Send_Create_Table(chil,site_link[min_key])
                    ins = []
                    ins.append((chil,1,min_key))
                    obj.insert_to_table('Execution_Table', ins)
            # Query to execute on min_key
            query_site = min_key
            # End sending table

        temp_tables.append(nodes[n]['Value'])
        print("Here in dfs query",query)
        obj.insert_to_table('Execution_Table',[(nodes[n]['Value'],1,query_site)])
        site_obj[query_site].execute_query(query)
        queries.append([query,query_site])
        return nodes[n]
        
    elif opt == 'Union':
        child = []
        vis[n]=1
        for i in range(num+1):
            if vis[i] == 0 and adj[n][i] == 1:
                child.append(dfs(i))
        
        query = 'Create Table ' + nodes[n]['Value'] + ' AS ('
        select = 'Select * From '
        where = ' Where '
        union = ' Union '
        And = ' And '
        
        for i in range(len(child)-1):
            query += select
            frag_name = child[i]['Value']
            query += frag_name 
            query += union
        
        i = len(child)-1
        query += select
        frag_name = child[i]['Value']
        query += frag_name
        frag_cond = child[i]['Condition']
        cond = len(frag_cond)
        query += ');'

        # sending table
        dict_child = {}
        hash_site = {1 : 0, 2:0 , 3:0}
        for i in child:
            dict_child[i['Value']] = get_site_temp(i['Value'])

        cost_site = {1: 0, 2:0,3:0}
        for keys in hash_site:
            for chil in dict_child:
                if keys not in dict_child[chil]:
                    cost_site[keys] += get_size(dict_child[chil][0],chil)
        min_key = 1
        min_sum = cost_site[1]
        for keys in cost_site:
            if(cost_site[keys] < min_sum):
                min_sum = cost_site[keys]
                min_key = keys
        print("UNION", dict_child)
        for chil in dict_child:
            if min_key not in dict_child[chil]:
                si = dict_child[chil][0]
                print("UNION", chil, min_key) 
                site_obj[si].Send_Create_Table(chil,site_link[min_key])
                ins = []
                ins.append((chil,1,min_key))
                obj.insert_to_table('Execution_Table', ins)
        # Query to execute on min_key
        query_site = min_key
        # End sending table
        print("Here in dfs query",query)
        temp_tables.append(nodes[n]['Value'])
        obj.insert_to_table('Execution_Table',[(nodes[n]['Value'],1,query_site)])
        site_obj[query_site].execute_query(query)
        queries.append([query,query_site])
        return nodes[n]
    
    elif opt == 'Join':
        child = []
        vis[n] = 1
        for i in range(num+1):
            if vis[i] == 0 and adj[n][i] == 1:
                child.append(dfs(i))
        
        condition = nodes[n]['Condition']
        cond = (condition.strip()).split('=')
        left = (cond[0].strip()).split('.')
        right = (cond[1].strip()).split('.')
        query = ''
        condi_flag = 0
        where = ' Where '
        exec_ins = [] 
        if left[1] == right[1]:
            query = 'Create Table ' + nodes[n]['Value'] + ' AS (Select * From '
            query += child[0]['Value'] + ' Inner Join ' + child[1]['Value'] + ' using('
            query += right[1]
            query += ')'
            
            if((len(child[0]['Condition'])!=0 and child[0]['Key'] == 'Table_Fragment') or (len(child[1]['Condition'])!= 0 and child[1]['Key'] == 'Table_Fragment')):
                query += where
            manas = []
            if(child[0]['Key'] == 'Table_Fragment'):
                for i in child[0]['Condition']:
                    manas.append(i)
            if(child[1]['Key'] == 'Table_Fragment'):
                for i in child[1]['Condition']:
                    manas.append(i)
            if(len(manas) >= 1):
                for i in manas[:-1]:
                    query += i + ' AND '
                query += manas[-1]

            condi_flag = 1
        else:
            query = 'Create Table ' + nodes[n]['Value'] + ' AS ('
            select = 'Select * From '
            where = ' Where '
            query += select + child[0]['Value'] + ',' + child[1]['Value']
            query += where
            query += nodes[n]['Condition']
            if((len(child[0]['Condition'])!=0 and child[0]['Key'] == 'Table_Fragment') or (len(child[1]['Condition'])!= 0 and child[1]['Key'] == 'Table_Fragment')):
                query += ' AND '
            manas = []
            if(child[0]['Key'] == 'Table_Fragment'):
                for i in child[0]['Condition']:
                    manas.append(i)
            if(child[1]['Key'] == 'Table_Fragment'):
                for i in child[1]['Condition']:
                    manas.append(i)
            if(len(manas) >= 1):
                for i in manas[:-1]:
                    query += i + ' AND '
                query += manas[-1]
        query += ');'

        # sending table
        dict_child = {}
        hash_site = {1 : 0, 2:0 , 3:0}
        for i in child:
            dict_child[i['Value']] = get_site_temp(i['Value'])
        print(dict_child)
        cost_site = {1: 0, 2:0,3:0}
        for keys in hash_site:
            for chil in dict_child:
                if keys not in dict_child[chil]:
                    cost_site[keys] += get_size(dict_child[chil][0],chil)
        min_key = 0
        min_sum = cost_site[1]
        if(cost_site[1] == 0):
            min_key = 1
        elif(cost_site[2] == 0):
            min_key = 2
        elif(cost_site[3] == 0):
            min_key = 3

        if(min_key == 0):
            semi_join_dict_child = {}
            flag = 0
            ins = []
            for chil in dict_child:
                si = dict_child[chil][0]
                if flag == 0:
                    que = "Select Count(DISTINCT "+left[1]+" ) From " +chil+";" 
                    query_semi = "Create TABLE "+chil +"_SJ"+" AS (Select Distinct "+left[1]+" FROM "+chil+" );"
                    site_obj[si].execute_query(query_semi)
                    exec_ins.append((chil +"_SJ",1,si))
                    flag = 1
                    out = site_obj[si].execute_query_output(que)
                    sizz =get_size(si,chil)                
                    ins.append((chil,left[1],float(out[0][0]),sizz))
                    semi_join_dict_child[chil] = [float(out[0][0]), sizz,si,left[1]]
                else :
                    que = "Select Count(DISTINCT "+right[1]+" ) From " +chil+";" 
                    query_semi = "Create TABLE "+chil +"_SJ"+" AS (Select Distinct "+right[1]+" FROM "+chil+" );"
                    site_obj[si].execute_query(query_semi)
                    exec_ins.append((chil +"_SJ",1,si))
                    out = site_obj[si].execute_query_output(que)  
                    sizz = get_size(si,chil)   
                    semi_join_dict_child[chil] = [float(out[0][0]), sizz,si,right[1]]
                    ins.append((chil,left[1],float(out[0][0]),sizz))
            obj.insert_to_table('Join_Selectivity',ins) 
            for key1 in semi_join_dict_child:
                for key2 in semi_join_dict_child:
                    if(key1 != key2):
                        siit = semi_join_dict_child[key1][2]
                        siit2 = semi_join_dict_child[key2][2]
                        cond1 = semi_join_dict_child[key1][3]
                        cond2 = semi_join_dict_child[key2][3]
                        site_obj[siit].Send_Create_Table(key1 + "_SJ", site_link[siit2])
                        exec_ins.append((key1 + "_SJ",1,siit2))
                        if(condi_flag == 1):
                            query = 'Create Table ' + key2 +'_SJOut' + ' AS (Select * From '
                            query += key2 + ' Inner Join ' + key1 +'_SJ'+  ' using('
                            query += right[1]
                            query += ')'
                            query += ');'
                            print("Query : ",query)
                            site_obj[siit2].execute_query(query)
                            print('exec : ' ,)
                            exec_ins.append((key2 + "_SJOut",1,siit2))
                        else :
                            query = 'Create Table ' + key2 +'_SJOut' + ' AS (Select * From '
                            query += key2 + " , " + key1 + '_SJ' + " Where "+key1 + "_SJ." + cond1 + " = " +key2 +"." +cond2 + ");"
                            print("Query : ",query)
                            site_obj[siit2].execute_query(query)
                            exec_ins.append((key2 + "_SJOut",1,siit2))

            min_key = 0
            min_sum = 0
            # print(semi_join_dict_child)
            for key in semi_join_dict_child:
                min_key = semi_join_dict_child[key][2]
                min_sum = get_size(min_key,key + "_SJOut")
            
            for key in semi_join_dict_child:
                k = semi_join_dict_child[key][2]
                s = get_size(k,key + "_SJOut")
                if(s > min_sum):
                    min_sum = s
                    min_key = k
            
            for key in semi_join_dict_child:
                if semi_join_dict_child[key][2] != min_key:
                    si = semi_join_dict_child[key][2]
                    site_obj[si].Send_Create_Table(key + "_SJOut",site_link[min_key])
                    exec_ins.append((key + "_SJOut",1, min_key))

            obj.insert_to_table("Execution_Table",exec_ins)
            key1 = ''
            key2 = ''
            for key in semi_join_dict_child:
                if len(key1) == 0:
                    key1 = key
                else:
                    key2 = key
            if condi_flag == 1:
                query = 'Create Table ' + nodes[n]['Value'] + ' AS (Select * From '
                query += key2 +"_SJOut"+ ' Inner Join ' + key1 +'_SJOut'+  ' using('
                query += right[1]
                query += ')'
                if((len(child[0]['Condition'])!=0 and child[0]['Key'] == 'Table_Fragment') or (len(child[1]['Condition'])!= 0 and child[1]['Key'] == 'Table_Fragment')):
                    query += where
                manas = []
                if(child[0]['Key'] == 'Table_Fragment'):
                    for i in child[0]['Condition']:
                        manas.append(i)
                if(child[1]['Key'] == 'Table_Fragment'):
                    for i in child[1]['Condition']:
                        manas.append(i)
                if(len(manas) >= 1):
                    for i in manas[:-1]:
                        temp = i.split('.')
                        temp[0]+= '_SJOut'
                        qui = ''
                        for i in temp[:-1]:
                            qui += temp + '.'
                        qui += temp[-1]
                        query += qui + ' AND '
                    temp = manas[-1].split('.')
                    temp[0]+= '_SJOut'
                    qui = ''
                    for i in temp[:-1]:
                        qui += temp + '.'
                    qui += temp[-1]
                    query += qui
            else:
                query = 'Create Table ' + nodes[n]['Value'] + ' AS (Select * From '
                query += key2 + "_SJOut"+ " , " + key1 + '_SJOut' + " Where "+key1 + "_SJOut." + cond1 + " = " +key2 +"_SJOut." +cond2
                if((len(child[0]['Condition'])!=0 and child[0]['Key'] == 'Table_Fragment') or (len(child[1]['Condition'])!= 0 and child[1]['Key'] == 'Table_Fragment')):
                    query += ' AND '
                manas = []
                if(child[0]['Key'] == 'Table_Fragment'):
                    for i in child[0]['Condition']:
                        manas.append(i)
                if(child[1]['Key'] == 'Table_Fragment'):
                    for i in child[1]['Condition']:
                        manas.append(i)
                if(len(manas) >= 1):
                    for i in manas[:-1]:
                        temp = i.split('.')
                        temp[0]+= '_SJOut'
                        qui = ''
                        for i in temp[:-1]:
                            qui += temp + '.'
                        qui += temp[-1]
                        query += qui + ' AND '
                    temp = manas[-1].split('.')
                    temp[0]+= '_SJOut'
                    qui = ''
                    for i in temp[:-1]:
                        qui += temp + '.'
                    qui += temp[-1]
                    query += qui

            query +=');'
            query_site = min_key
            queries.append([query,query_site])
            print("Here in dfs query",query)
            obj.insert_to_table("Execution_Table",[(nodes[n]['Value'],1,query_site)])
            site_obj[query_site].execute_query(query)

        else:
            query_site = min_key
            temp_tables.append(nodes[n]['Value'])
            print("Here in dfs query",query)
            obj.insert_to_table("Execution_Table",[(nodes[n]['Value'],1,query_site)])
            site_obj[query_site].execute_query(query)
            queries.append(query)

        # End sending table


        
        return nodes[n]


def final_query():
    i = num
    query = ''
    query1 = ''
    query2 = ''
    query3 = ''
    # print(vis, len(vis))
    # print(adj)
    while i > 0 and nodes[i]['Key'] != 'Select':
        opt = nodes[i]['Key']
        vis[i] = 1
        if  opt == 'Project':
            query = 'Select '
            col = nodes[i]['Condition']
            for j in range(len(col)):
                query += col[j]
                query += ','
            query = query[:-1]
            query += ' from '
        
        elif opt == 'GROUP BY':
            query2 = ' ' + opt + ' '
            col = nodes[i]['Condition']
            for j in range(len(col)):
                query2 += col[j]
                query2 += ','
            query2 = query2[:-1]
        
        elif opt == 'HAVING':
            query3 = ' ' + opt + ' '
            cond = nodes[i]['Condition']
            for j in range(len(cond)-1):
                query3 += cond[j]
                query3 += ' and '
            query3 += cond[len(cond)-1]
        i -= 1

    
    if nodes[i]['Key'] == 'Select':
        vis[i] = 1
        temp = dfs(i-1)
        query1 = temp['Value']
        print("Final",query1)
        site_i = get_site_temp(query1)
        if(site_i[0] == 1):
            pass
        else:
            obj.insert_to_table('Execution_Table',[(query1,1,1)])
            site_obj[site_i[0]].Send_Create_Table(query1,site_link[1])
        cond = nodes[i]['Condition']
        if len(cond) > 0:
            query1 += ' where '
            for j in range(len(cond)-1):
                query1 += cond[j]
                query1 += ' and '
            query1 += cond[len(cond)-1]
    
    query += query1 + query2 + query3 + ';'
    print(query)
    out = obj.execute_query_final(query)
    print("EXECUTED QUERY")
    myTable = PrettyTable(list(out[0]))
    for i in out[1:]:
        myTable.add_row(list(i))
    print(myTable)
    # print("-------------------------------------------\n\n\n\n")
    queries.append(query)
    return queries


try:
    temp = final_query()
except :
    print("Execution Error")
remove_temp()
# print(queries

# Select Course_Code,Phone_No,Last_Name From Teaches,Faculty From where Last_Name = "Agarwal" and Teaches.Faculty_Id = Faculty.Faculty_Id;
# Select AVG(Grade) From Opts,Course where Course.Course_Code = Opts.Course_Code and Course.Course_Type = "HSME"
# Select reserve_id,name,city,price,sum(price) from Room, Guest, Reserve Where Room.reserve_id = Reserve.reserve_id and Room.reserve_id = Guest.reserve_id and Room.city = 'Mumbai' and Guest.guest_id < 20 and Room.reserve_id > 2 Group by name,price Having price > 3
