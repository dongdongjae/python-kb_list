import psycopg2
from db_params import db_params
from collections import OrderedDict
import timeit

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

def create_list_tables(conn):
    idx = 0

    cond_data, sent_data, map_data, map_cond_list, map_sent_list = read_data_from_db(
        conn)

    map_cond_id_list = list(OrderedDict.fromkeys(
        item[:3] for item in map_cond_list))
    cond_map = [{'id': row[0], 'name_kor': row[4],
                 'value_range': row[7]}for row in cond_data]
    sent_map = [{'id': row[0], 'item_str': row[2]} for row in sent_data]

    filter_map_sent_list = []
    filter_map_data = []

    for sent_id, map_row in zip(map_sent_list, map_data):
        filter_row = [map_cond_list[j]
                      for j in range(len(map_row)) if map_row[j] == "Y"]

        if filter_row:
            filter_map_data.append(filter_row)
            filter_map_sent_list.append(sent_id)

    map_data_con_id = []

    for row in filter_map_data:
        row_con_id = []

        for cond_id in map_cond_id_list:
            id_list = [item for item in row if cond_id == item[:3]]

            if id_list:
                row_con_id.append(id_list)

        if row_con_id:
            map_data_con_id.append(row_con_id)

    map_range_list = []

    for rows in map_data_con_id:
        rows_list = []

        for row in rows:
            row_list = [j['value_range']
                        for i in row for j in cond_map if j['id'] == i]
            rows_list.append(row_list)

        map_range_list.append(rows_list)

    header = [['idx'] + map_cond_id_list + ['sentence_id', 'sentence_kor']]
    create_table(conn, table_name='kb_v2_list', header_data=header[0])

    for i in range(len(map_range_list)):
        map_range_row = map_range_list[i]
        map_sent_id = filter_map_sent_list[i]

        map_values_comb = combine_arrays(map_range_row)

        for row in map_values_comb:
            new_row = [[idx := idx + 1] + row + [next((obj['id'] for obj in sent_map if obj['id'] == map_sent_id), None)] + [next(
                (obj['item_str'] for obj in sent_map if obj['id'] == map_sent_id), None)]]

            print(new_row)
            insert_row_in_table(conn, 'kb_v2_list', new_row[0])


def create_table(conn, table_name, header_data):
    cursor = conn.cursor()

    create_table_query = f'''CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'"{column}" TEXT' for column in header_data])});'''
    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()


def insert_row_in_table(conn, table_name, row_data):
    cursor = conn.cursor()

    insert_data_query = f'''INSERT INTO {table_name} VALUES {tuple(row_data)};'''
    cursor.execute(insert_data_query)
    conn.commit()


def combine_arrays(arr):
    if not arr:
        return []

    result = [[item] for item in arr[0]]

    for current_array in arr[1:]:
        temp_result = []

        for item in result:
            for current_item in current_array:
                temp_result.append(item + [current_item])

        result = temp_result

    return result


def read_data_from_db(conn):
    cond_num_rows, cond_num_columns = get_table_size(conn, 'condition')
    sent_num_rows, sent_num_columns = get_table_size(conn, 'sentence')
    map_num_rows, map_num_columns = get_table_size(conn, 'kb_v2_map')

    cond_data = get_values_from_table(
        conn, 'condition', 1, 1, cond_num_rows, cond_num_columns)
    sent_data = get_values_from_table(
        conn, 'sentence', 1, 1, sent_num_rows, sent_num_columns)
    map_data = get_values_from_table(
        conn, 'kb_v2_map', 2, 3, map_num_rows, map_num_columns)
    map_cond_list = flatten_list(get_columns_from_table(
        conn, 'kb_v2_map')[2:])
    map_sent_list = flatten_list(get_values_from_table(
        conn, 'kb_v2_map', 2, 1, map_num_rows, 1))

    return cond_data, sent_data, map_data, map_cond_list, map_sent_list


def get_values_from_table(conn, table_name, start_row, start_col, end_row, end_col):
    result = []

    cursor = conn.cursor()

    select_query = f"""
        SELECT *
        FROM {table_name}
        OFFSET {start_row - 1} 
        LIMIT {end_row - start_row + 1};
    """

    cursor.execute(select_query)
    rows = cursor.fetchall()

    for row in rows:
        result.append(list(row[start_col - 1:end_col]))

    cursor.close()
    return result


def get_columns_from_table(conn, table_name):
    result = []
    cursor = conn.cursor()
    columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    cursor.execute(columns_query)

    columns_info = cursor.fetchall()

    for column in columns_info:
        result.append(column[0])

    cursor.close()
    return result


def get_table_size(conn, table_name):
    cursor = conn.cursor()

    size_query = f"""
        SELECT 
            (SELECT COUNT(*) FROM {table_name}) as num_rows,
            (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{table_name}') as num_columns
    """

    cursor.execute(size_query)

    size_info = cursor.fetchone()

    cursor.close()

    return size_info


def flatten_list(nested_list):
    flat_list = []
    for item in nested_list:

        if isinstance(item, list):
            flat_list.extend(flatten_list(item))
        else:
            flat_list.append(item)
    return flat_list


elapsed_time = timeit.timeit(create_list_tables(conn), number=1)

print(f"실행 소요시간: {elapsed_time}초")

cursor.close()
conn.close()
