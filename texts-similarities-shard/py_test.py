l = []

if not l:
    print("empty list")

if l:
    print("not empty list")

ids_vectors = [("a", 1), ("b", 2), ("c", 3)]
assert ids_vectors is not None, "tuples must have data"
ids, vectors = zip(*ids_vectors)
print(ids, vectors)

ids_vectors_ = []
assert ids_vectors_ != [], "tuples must have data"
ids, vectors = zip(*ids_vectors_)
print(ids, vectors)
