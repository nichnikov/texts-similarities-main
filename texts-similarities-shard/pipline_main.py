import os
import time
import json
from uuid import uuid4
from hashlib import md5
import pandas as pd
from texts_processing import TextsTokenizer, TextsVectorsBoW
from storage import Worker


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def data_prepare(json_data):
    """преобразует входящие словари в список кортежей"""
    queries_in = []
    for d in json_data:
        queries_in += [(d["locale"], d["moduleId"], str(uuid4()),
                        d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]
    return queries_in


# PATH = r"/home/an/Data/Dropbox/data/fast_answers" # Home
PATH = r"/home/alexey/Data/Dropbox/data/fast_answers"  # Office
# file_name = "data_all_with_locale.json"
# file_name = "qa-full-ru.json"
# file_name = "qa-full-ua.json"
file_name = "qa.json"

with open(os.path.join(PATH, file_name), "r") as f:
    json_data = json.load(f)

queries_in = []
for d in json_data["data"]:
    queries_in += [(d["locale"], d["moduleId"], str(uuid4()),
                    d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]

print(len(queries_in))
main = Worker(20000, 33000)

chunks = [x for x in chunks(queries_in, 20000)]
for num, chunk in enumerate(chunks):
    main.add(chunk)
    print(num + 1, "/", len(chunks))

print("matrices quantity:", len(main.matrix_list.ids_matrix_list))

for d in json_data["data"][:100]:
    test_search_data = [(d["locale"], d["moduleId"], str(uuid4()),
                         d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]
    t = time.time()
    r = main.search(test_search_data)
    print("search time:", time.time() - t)
    print(r.shape)
    # r.to_csv("test_result.csv", index=False)

print("matrices quantity:", len(main.matrix_list.ids_matrix_list))