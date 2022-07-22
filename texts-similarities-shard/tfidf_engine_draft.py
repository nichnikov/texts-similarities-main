import os
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from texts_processing import TextsTokenizer, TextsVectorsTfIdf
from scipy.sparse import hstack, vstack
from storage import IdsMatrix


"""Data loading"""
test_df = pd.read_csv(os.path.join("data", "queries_chat_testing.csv"), sep="\t")
test_texts = list(test_df["query"])

"""Model creating"""
tokenizer = TextsTokenizer()
etalons_tokens = tokenizer(test_texts)

tfidf_model = TextsVectorsTfIdf(3500)
tfidf_model.model_fill(etalons_tokens)
ids_matrix = IdsMatrix()
# ids_matrix.add([])

vcs = tfidf_model.tokens2vectors(etalons_tokens)
ids_vcs = [(i, v) for i, v in enumerate(vcs)]
ids_matrix.add(ids_vcs)
print(ids_matrix.matrix.shape)
ids_matrix.delete([0, 1, 2, 3, 4, 5, 6, 100, 1005, 5000])
print(ids_matrix.matrix.shape)
print(ids_matrix.ids[:10])