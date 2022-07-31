# Authors: Konstantin Fünfgelt & Edward Kreutzarek
import sys
import numpy as np
import time
from collections import defaultdict
import multiprocessing


def get_triplet(line):
    # split based on tab
    triplet = line.split("\t")
    # remove unwanted string at the end
    triplet[2] = triplet[2].replace(" .\n", "")
    # return subject, properties, object
    return triplet[0], triplet[1], triplet[2]


def load_data_as_table(path):
    database = {}
    str_to_int = {}
    string_id = 1
    # reading rdf file
    with open(path, "r") as file:
        for line in file:
            subj, prop, obj = get_triplet(line)
            # map each unique string to interger
            if subj not in str_to_int:
                str_to_int[subj] = string_id
                string_id += 1
            if obj not in str_to_int:
                str_to_int[obj] = string_id
                string_id += 1
            # create for each property a key and append each entry as a list
            if prop not in database:
                database[prop] = [[str_to_int[subj], str_to_int[obj]]]
            else:
                database[prop].append([str_to_int[subj], str_to_int[obj]])
    # convert list to numpy arrays
    for key, value in database.items():
        database[key] = np.array(value, dtype=np.uint64)
    return str_to_int, database


# Algorithm was inspired by following source: https://rosettacode.org/wiki/Hash_join#Python
def hash_join(table_1, index_1, table_2, index_2):
    # check for smaller table to start with to avoid look up in big dictionary
    t_1, t_2, ind_1, ind_2, switched = (table_1, table_2, index_1, index_2, False) if sys.getsizeof(
        table_1) < sys.getsizeof(table_2) else (table_2, table_1, index_2, index_1, True)
    h = defaultdict(list)
    # hash phase: hash index column of smaller table and append entry as value
    for s in t_1:
        h[s[ind_1]].append(s)
    # join phase iterate over big table and look up in small table for matches
    if not switched:
        # return concatenated matches
        return np.array([np.concatenate((s, r)) for r in t_2 for s in h[r[ind_2]]])
    return np.array([np.concatenate((r, s)) for r in t_2 for s in h[r[ind_2]]])


def sort_table(arguments, output):
    table, index, p_name = arguments[0], arguments[1], arguments[2]
    # sorting table and map it to process in t
    output[p_name] = table[table[:, index].argsort()]
    return table[table[:, index].argsort()]


def parallel_sort(table_1, index_1, table_2, index_2):
    manager = multiprocessing.Manager()
    # result dict for the differentiation of the processes
    output = manager.dict()
    # creating two processes for two sort actions
    p1 = multiprocessing.Process(target=sort_table, args=((table_1, index_1, "p1"), output))
    p2 = multiprocessing.Process(target=sort_table, args=((table_2, index_2, "p2"), output))
    jobs = [p1, p2]
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    return output["p1"], output["p2"]


# Algorithm was inspired by following source: https://www.dcs.ed.ac.uk/home/tz/phd/thesis/node20.htm
def sort_merge_join(table_1, index_1, table_2, index_2, parallel=False):
    # sort values by index column
    # without parallel sort
    if not parallel:
        table_1 = table_1[table_1[:, index_1].argsort()]
        table_2 = table_2[table_2[:, index_2].argsort()]
    else:
        table_1, table_2 = parallel_sort(table_1, index_1, table_2, index_2)

    # init indicies
    r = 0
    q = 0
    # max length of tables
    r_max = len(table_1) - 1
    q_max = len(table_2) - 1
    res = []
    while r <= r_max and q <= q_max:
        # compare values dependent on column of both tables and increment each index variable until element match
        if table_1[r][index_1] > table_2[q][index_2]:
            q += 1
        elif table_1[r][index_1] < table_2[q][index_2]:
            r += 1
        else:
            # append matching entries to result table
            res.append(np.concatenate((table_1[r], table_2[q])))
            # check if next value of table 2 match value of table 1
            q_prime = q + 1
            while q_prime <= q_max and table_1[r][index_1] == table_2[q_prime][index_2]:
                # append match to result table and increment index variable of table 2
                res.append(np.concatenate((table_1[r], table_2[q_prime])))
                q_prime += 1

            # check if next value of table1 match value of table 2
            r_prime = r + 1
            while r_prime <= r_max and table_1[r_prime][index_1] == table_2[q][index_2]:
                # append match to result table and increment index variable of table 1
                res.append(np.concatenate((table_1[r_prime], table_2[q])))
                r_prime += 1

            # increment indices if no more matches found
            r += 1
            q += 1
    return np.array(res)


def query_join(join_func, database, props, hash=False):
    # for hash join convert tables into generators --> memory effiecency
    if hash:
        follows = (i for i in database[props[0]])
        friend = (i for i in database[props[1]])
        likes = (i for i in database[props[2]])
        hasreview = (i for i in database[props[3]])
    else:
        follows = database[props[0]]
        friend = database[props[1]]
        likes = database[props[2]]
        hasreview = database[props[3]]
    st = time.time()
    # sql query: friendOf.subject = follows.object
    join_1 = join_func(friend, 0, follows, 1)
    # joining first join on friendOf.object = likes.subject
    join_2 = join_func(join_1, 1, likes, 0)
    # joining second join on likes.object = hasReview.subject
    # and selecting only demanded columns
    final_join = join_func(join_2[:, [1, 2, 3, 4, 5]], 4, hasreview, 0)
    et = time.time()
    ex_time = et - st
    # print("Execution time: \t", ex_time )
    # final_join = final_join[:,[1, 2, 3, 5, 7]]
    return final_join, ex_time


if __name__ == '__main__':
    # load database#
    id_dict, database = load_data_as_table('100k.txt')
    # properties for diffrent datasets
    props_100_k = ['wsdbm:follows', 'wsdbm:friendOf', 'wsdbm:likes', 'rev:hasReview']
    props_10_mio = [r"<http://db.uwaterloo.ca/~galuc/wsdbm/follows>", r"<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>",
                    r"<http://db.uwaterloo.ca/~galuc/wsdbm/likes>", r"<http://purl.org/stuff/rev#hasReview>"]

    # sort-merge join execution
    # hash join execution
    res, ex_time = query_join(hash_join, database, props_100_k, True)
    print("hash join runtime: ", ex_time)
    print("hash join length of Table:", len(res))
    res, ex_time = query_join(sort_merge_join, database, props_100_k)
    print("sort-merge join runtime: ", ex_time)
    print("sort-merge join length of Table:", len(res))
    # 10 Million dataset --> Memory Error
    # id_dict, database = load_data_as_table('watdiv.10M.nt')
    # res = query_join(hash_join, database, props_10_mio, True)
    # res = query_join(sort_merge_join, database, props_10_mio)
