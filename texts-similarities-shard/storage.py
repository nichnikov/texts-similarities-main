from scipy.sparse import hstack, vstack


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




'''
class TextsStorage:
    """"""
    def __init__(self):
        self.queries = pd.DataFrame({}, columns=["queryId", "answerId", "moduleId", "cluster", "pubIds", "tokens"])
        self.query_ids = []
        self.answer_ids = []

    def add(self, input_queries: pd.DataFrame):
        """dictionary must include all attributes"""
        self.queries = pd.concat([self.queries, input_queries])
        self.query_ids = list(self.queries["queryId"])
        self.answer_ids = list(set(self.queries["answerId"]))

    def delete(self, items: [{}], what="queries"):
        """dictionary must include all attributes"""
        if what == "queries":
            self.queries = self.queries[~self.queries["queryId"].isin(items)]
        else:
            self.queries = self.queries[~self.queries["answerId"].isin(items)]
        self.query_ids = list(self.queries["queryId"])
        self.answer_ids = list(set(self.queries["answerId"]))

    def search(self, item_ids: [], what="queries"):
        """Возвращает текст вопроса с метаданными по входящему списку query_ids"""
        if what == "queries":
            return self.queries[self.queries["queryId"].isin(item_ids)]
        else:
            return self.queries[self.queries["answerId"].isin(item_ids)]
'''