import os
import time
import requests
import json


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


#PATH = r"/home/an/Data/Dropbox/data/fast_answers" # Home
PATH = r"/home/alexey/Data/Dropbox/data/fast_answers" # Office
# file_name = "data_all_with_locale.json"
# file_name = "qa-full-ru.json"
# file_name = "qa-full-ua.json"
file_name = "qa.json"

with open(os.path.join(PATH, file_name), "r") as f:
    initial_data_json = json.load(f)

for i in initial_data_json:
    print(i)

print(initial_data_json["data"][:10])
print(type(initial_data_json["data"]))
print(len(initial_data_json["data"]))

initial_data = initial_data_json["data"][:5000]

# print(initial_data)
# initial_data = initial_data["data"]
# print(initial_data[0])
# sending_data = {"data": [["292b4373-61c0-440b-b4d7-9f4c9b27a5ba", 613, 85, "приказ об отзыве из отпуска во время болезни", [10, 12, 16, 58, 189, 191, 222]]], "operation": "search"}

#SERVISE_URL = "http://srv01.lingua.dev.msk2.sl.amedia.tech:8000/api/" # prod
#SERVISE_URL = "http://srv01.lingua.dev.msk2.sl.amedia.tech:7000/api/" # dev
#SERVISE_URL = "http://srv01.lingua.dev.msk2.sl.amedia.tech:8080/api/" # prod
#SERVISE_URL = "http://0.0.0.0:7000/api/"
#SERVISE_URL = "http://127.0.0.1:5000/api/"
SERVISE_URL = "http://0.0.0.0:8080/api/"

#do = "add"
#do = "search_a_lot"
#do = "search_one"
#do = "update"
# do = "delete"
#do = "delete_all"

for do in ["search_a_lot"]:
    if do == "add":
        splited_data = [x for x in chunks(initial_data, 10000)]
        k = 1
        for chunk in splited_data:
            print(len(chunk))
            sending_data = {"data": chunk, "score": 0.99, "operation": "add"}
            r = requests.post(SERVISE_URL, json=sending_data)
            print(k, "/", len(splited_data))
            k += 1
            print(r)
            print(r.text)
            # print(r.content)

    if do == "delete":
        del_data = initial_data # [initial_data[1]]
        print(del_data)
        t = time.time()
        sending_data = {"data": del_data, "score": 0.99, "operation": "delete"}
        r = requests.post(SERVISE_URL, json=sending_data)
        delta = time.time() - t
        print("deleting time:", delta)
        print(r)
        print(r.text)
        print(r.content)

    if do == "update":
        del_data = [initial_data[3]]
        print(del_data)
        t = time.time()
        sending_data = {"data": del_data, "score": 0.99, "operation": "update"}
        r = requests.post(SERVISE_URL, json=sending_data)
        delta = time.time() - t
        print("updating time:", delta)

    elif do == "search_one":
        searched_data = [initial_data[0]]
        print(searched_data, "\n")
        clusters = []
        for d in searched_data:
            clusters += d["clusters"]
        t = time.time()
        sending_data = {"data": searched_data, "operation": "search", "score": 0.99}
        r = requests.post(SERVISE_URL, json=sending_data)
        delta = time.time() - t
        print(r.json())
        print("clusters quantity:", len(clusters), "searching time:", delta)
        # with open(os.path.join("data", "test.json"), "w") as f:
        #    json.dump(sending_data, f, ensure_ascii=False)

    elif do == "search_a_lot":
        secs = 0
        k = 0
        for i in range(100):
            print(k)
            searched_data = [initial_data[i]]
            print(searched_data)
            # print(searched_data, "\n")
            clusters = []
            for d in searched_data:
                clusters += d["clusters"]
            print("clusters quantity:", len(clusters))
            t = time.time()
            sending_data = {"data": searched_data, "operation": "search", "score": 0.99}
            r = requests.post(SERVISE_URL, json=sending_data)
            delta = time.time() - t
            secs += delta
            print(r.json())
            print("clusters quantity:", len(clusters), "searching time:", delta)
            k += 1

        print("time evolution:", secs, secs/k)

    # 16:30:41,268 dispatcher.utils INFO [128897, 128897, 128891]
    elif do == "delete_all":
        sending_data = {"data": [], "operation": "delete_all"}
