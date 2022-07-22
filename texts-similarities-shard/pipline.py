import os
import time
import json
from uuid import uuid4
from hashlib import md5
import pandas as pd
from texts_processing import TextsTokenizer, TextsVectorsBoW
from storage import IdsMatrix, search_func, MatricesList, TextsStorage


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# PATH = r"/home/an/Data/Dropbox/data/fast_answers" # Home
PATH = r"/home/alexey/Data/Dropbox/data/fast_answers"  # Office
# file_name = "data_all_with_locale.json"
# file_name = "qa-full-ru.json"
# file_name = "qa-full-ua.json"
file_name = "qa.json"

with open(os.path.join(PATH, file_name), "r") as f:
    json_data = json.load(f)

queries_in = []
for d in json_data["data"][:3000]:
    queries_in += [(md5(str((d["locale"], d["moduleId"])).encode("utf8")).hexdigest(), str(uuid4()),
                    d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]

# s_id == system id
# q_id == query id
# a_id == answer id
tokenizer = TextsTokenizer()
vectorizer = TextsVectorsBoW(30000)

s_ids, q_ids, a_ids, m_ids, txs, p_ids = zip(*queries_in)
tkns = tokenizer(txs)
vcrs = vectorizer(tkns)

print("len(vcrs):", len(vcrs))
ids_matrix = IdsMatrix()
ids_matrix.add(zip(q_ids, vcrs))

print("matrix.shape:", ids_matrix.matrix.shape)
print("len matrix.ids:", len(ids_matrix.ids))

test_vcs = list(zip(q_ids[3:7], vcrs[3:7]))
res = ids_matrix.search(test_vcs, 0.9)
print("res:\n", res)

prms = {"vectors_ids": q_ids[3:7],
        "vectors": vcrs[3:7],
        "matrix": ids_matrix.matrix,
        "matrix_ids": ids_matrix.ids,
        "score": 0.95}

res2 = search_func(prms)
print("res2:\n", res2)

ids_matrix.delete(q_ids[3:7])
res3 = ids_matrix.search(test_vcs, 0.9)
print("res3:\n", res3)

main = MatricesList(5000)
ids_vecs = list(zip(q_ids, vcrs))
main.add(ids_vecs)
print("len main.ids_matrix_list:", len(main.ids_matrix_list))

t = time.time()
s_r = main.search(zip(q_ids[:1000], vcrs[:1000]))
print("search_time:", time.time() - t)
print("len s_r:", len(s_r))

df_columns = ["queryId", "answerId", "moduleId", "cluster", "pubIds"]
txt_storage = TextsStorage(df_columns)
df = pd.DataFrame(zip(q_ids, a_ids, m_ids, txs, p_ids), columns=df_columns)

txt_storage.add(df)
print(txt_storage.data)
