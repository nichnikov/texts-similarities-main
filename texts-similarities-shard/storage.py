import logging
import pandas as pd
import time
from scipy.sparse import hstack, vstack
from collections import namedtuple
from itertools import chain
from multiprocessing import Pool
from sklearn.metrics.pairwise import cosine_similarity
from texts_processing import TextsVectorsBoW, TextsTokenizer


logger = logging.getLogger("seacher")
logger.setLevel(logging.INFO)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def search_func(searched_data: {}):
    """searched_vectors tuples must be like (query_id, query_vector)
    search_in_ids - ids of vectors for searching in
    search_in_matrix - matrix for for searching in
    """
    vectors_ids = searched_data["vectors_ids"]
    vectors = searched_data["vectors"]
    matrix = searched_data["matrix"]
    matrix_ids = searched_data["matrix_ids"]
    score = searched_data["score"]
    searched_matrix = hstack(vectors).T
    if matrix is None:
        return []
    else:
        try:
            t = time.time()
            matrix_scores = cosine_similarity(searched_matrix, matrix, dense_output=False)
            search_results = [[(v_id, matrix_ids[mrx_i], sc) for mrx_i, sc in zip(scores.indices, scores.data)
                               if sc >= score] for v_id, scores in zip(vectors_ids, matrix_scores)]
            print("cosine_similarity and search_results time:", time.time() - t)
            logger.info('searching successfully completed')
            return [x for x in chain(*search_results) if x]
        except Exception as e:
            logger.error('Failed queries search in MainSearcher.search: ' + str(e))
            return []


class Worker:
    """объект для оперирования MatricesList и TextsStorage"""
    def __init__(self, max_shard_size: int, vocabulary_size: int):
        self.columns = ["locale", "ModuleId", "queryId", "answerId", "moduleId", "cluster", "pubIds"]
        self.max_shard_size = max_shard_size
        self.text_storage = TextsStorage(self.columns)
        self.matrix_list = MatricesList(self.max_shard_size)
        self.tokenizator = TextsTokenizer()
        self.vectorizator = TextsVectorsBoW(vocabulary_size)

    def vectors_maker(self, data: [()]):
        lc, md, q_ids, a_i, m_i, txs, p_ids = zip(*data)
        tokens = self.tokenizator(txs)
        vectors = self.vectorizator(tokens)
        return list(zip(q_ids, vectors))

    def add(self, data: [()]):
        ids_vectors = self.vectors_maker(data)
        self.matrix_list.add(ids_vectors)
        self.text_storage.add(data)

    def delete(self, ids: []):
        self.matrix_list.delete(ids)
        self.text_storage.delete(ids, by_column="queryId")

    def delete_all(self):
        self.text_storage = TextsStorage(self.columns)
        self.matrix_list = MatricesList(self.max_shard_size)

    def update(self, data: [()]):
        lc, md, q_ids, a_i, m_i, txs, p_ids = zip(*data)
        self.delete(q_ids)
        self.add(data)

    def search(self, searched_data: [()], score=0.99):
        ids_vectors = self.vectors_maker(searched_data)
        searched_df = pd.DataFrame(searched_data, columns=self.columns)
        t1 = time.time()
        search_results = self.matrix_list.search(ids_vectors, score)
        print("matrix_list.search time:", time.time() - t1)
        searched_ids, founded_ids, scores = zip(*search_results)
        search_results_df = pd.DataFrame(search_results, columns=["queryId", "founded_ids", "scores"])
        t2 = time.time()
        found_data_df = self.text_storage.search(founded_ids, by_column="queryId")
        print("text_storage.search time:", time.time() - t2)
        t2 = time.time()
        searched_df = pd.merge(searched_df, search_results_df, on="queryId")
        searched_df.rename(columns={"queryId": "searched_queryId", "answerId": "searched_answerId",
                                    "cluster": "searched_cluster"}, inplace=True)
        result_df = pd.merge(searched_df[["searched_queryId", "searched_answerId", "searched_cluster",
                                          "founded_ids", "scores"]],
                             found_data_df, left_on="founded_ids", right_on="queryId")
        print("DataFrame manipulation time:", time.time() - t2)
        print("search result:", result_df.shape)
        return result_df.shape


class MatricesList:
    """"""

    def __init__(self, max_size):
        self.ids_matrix_list = [IdsMatrix()]
        self.max_size = max_size

    def delete_all(self):
        """"""
        self.ids_matrix_list.clear()
        self.ids_matrix_list = [IdsMatrix()]

    def add(self, ids_vectors):
        """"""
        input_chunks = [x for x in chunks(ids_vectors, self.max_size)]
        for chunk in input_chunks:
            is_matrices_full = True
            for im in self.ids_matrix_list:
                if len(im.ids) < self.max_size:
                    im.add(chunk)
                    is_matrices_full = False
            if is_matrices_full:
                """adding new queries_matrix"""
                im = IdsMatrix()
                im.add(chunk)
                self.ids_matrix_list.append(im)

    def delete(self, ids: []):
        """"""
        for ids_matrix in self.ids_matrix_list:
            if set(tuple(ids)) & set(tuple(ids_matrix.ids)):
                ids_matrix.delete(ids)

    def search(self, searched_vectors: [()], min_score=0.99):
        """searched_vectors: [(vector_id, vector)]"""
        vectors_ids, vectors = zip(*searched_vectors)
        serched_data = [{"vectors_ids": vectors_ids,
                         "vectors": vectors,
                         "matrix": mx.matrix,
                         "matrix_ids": mx.ids,
                         "score": min_score} for mx in self.ids_matrix_list]
        """
        search_result = []
        for d in serched_data:
            search_result.append(search_func(d))"""
        pool = Pool()
        search_result = pool.map(search_func, serched_data)
        pool.close()
        pool.join()
        return [x for x in chain(*search_result) if x]


class IdsMatrix:
    """"""

    def __init__(self):
        self.ids = []
        self.matrix = None

    def add(self, ids_vectors: [()]):
        """tuples must be like (text_id, text_vector)"""
        assert ids_vectors != [], "tuples must have data"
        ids, vectors = zip(*ids_vectors)
        self.ids += ids
        if self.matrix is None:
            self.matrix = hstack(vectors).T
        else:
            self.matrix = vstack((self.matrix, hstack(vectors).T))

    def delete(self, delete_ids: []):
        """tuples must be like (text_id, text_vector)"""
        ids_vectors = [(i, v) for i, v in zip(self.ids, self.matrix) if i not in delete_ids]
        if ids_vectors:
            self.ids, vectors = zip(*ids_vectors)
            self.matrix = vstack(vectors)
        else:
            self.matrix = None

    def update(self, ids_vectors: [()]):
        """tuples must be like (text_id, text_vector)"""
        self.delete([i for i, v in ids_vectors])
        self.add(ids_vectors)

    def search(self, ids_vectors: [()], score: float):
        """tuples must be like (query_id, query_vector)"""
        searched_ids, vectors = zip(*ids_vectors)
        searched_matrix = hstack(vectors).T

        if self.matrix is None:
            return []

        try:
            matrix_scores = cosine_similarity(searched_matrix, self.matrix, dense_output=False)
            ResultItem = namedtuple("ResultItem", "SearchedTextId, FoundTextId, Score")
            search_results = [[ResultItem(q_id, self.ids[i], sc) for i, sc in zip(scores.indices, scores.data)
                               if sc >= score] for scores, q_id in zip(matrix_scores, searched_ids)]
            logger.info('searching successfully completed')
            return [x for x in chain(*search_results) if x]
        except Exception as e:
            logger.error('Failed queries search in MainSearcher.search: ' + str(e))
            return []


class TextsStorage:
    """["queryId", "answerId", "moduleId", "cluster", "pubIds"]"""
    def __init__(self, columns: [str]):
        self.columns = columns
        self.data = pd.DataFrame({}, columns=columns)

    def add(self, input_data: [()]):
        """dictionary must include all attributes"""
        input_data_df = pd.DataFrame(input_data, columns=self.columns)
        self.data = pd.concat([self.data, input_data_df])

    def delete(self, items: [], by_column: str):
        """dictionary must include all attributes"""
        self.data = self.data[~self.data[by_column].isin(items)]

    def search(self, item_ids: [], by_column: str):
        """Возвращает текст вопроса с метаданными по входящему списку query_ids"""
        return self.data[self.data[by_column].isin(item_ids)]
