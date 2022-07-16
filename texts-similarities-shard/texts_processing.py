import re
import copy
from pymystem3 import Mystem
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from gensim.matutils import corpus2csc
import time


class TextsTokenizer:
    """Tokenizer"""
    def __init__(self):
        self.m = Mystem()

    def texts2tokens(self, texts: [str]) -> [str]:
        """Lemmatization for texts in list. It returns list with lemmatized texts."""
        t = time.time()
        text_ = "\n".join(texts)
        text_ = re.sub(r"[^\w\n\s]", " ", text_)
        lm_texts = "".join(self.m.lemmatize(text_))
        print("texts lemmatization time:", time.time() - t)
        return [lm_q.split() for lm_q in lm_texts.split("\n")][:-1]

    def __call__(self, texts: [str]):
        return self.texts2tokens(texts)


class TextsVectorsBoW:
    """"""
    def __init__(self, max_dict_size: int):
        self.dictionary = None
        self.max_dict_size = max_dict_size

    def tokens2corpus(self, tokens: []):
        """queries2vectors new_queries tuple: (text, query_id)
        return new vectors with query ids for sending in searcher"""

        if self.dictionary is None:
            gensim_dict_ = Dictionary(tokens)
            assert len(gensim_dict_) <= self.max_dict_size, "len(gensim_dict) must be less then max_dict_size"
            self.dictionary = Dictionary(tokens)
        else:
            gensim_dict_ = copy.deepcopy(self.dictionary)
            gensim_dict_.add_documents(tokens)
            if len(gensim_dict_) <= self.max_dict_size:
                self.dictionary = gensim_dict_
        return [self.dictionary.doc2bow(lm_q) for lm_q in tokens]

    def tokens2vectors(self, tokens: []):
        """"""
        corpus = self.tokens2corpus(tokens)
        return [corpus2csc([x], num_terms=self.max_dict_size) for x in corpus]

    def __call__(self, new_tokens):
        return self.tokens2vectors(new_tokens)

class TextsVectorsTfIdf(TextsVectorsBoW):
    """"""
    def __init__(self, max_dict_size):
        super().__init__(max_dict_size)
        self.tfidf_model = None

    def model_fill(self, tokens: []):
        """"""
        assert self.tfidf_model is None, "the model is already filled"
        corpus = super().tokens2corpus(tokens)
        self.tfidf_model = TfidfModel(corpus)

    def tokens2vectors(self, tokens: []):
        """"""
        vectors = super().tokens2corpus(tokens)
        return [corpus2csc([x], num_terms=self.max_dict_size) for x in self.tfidf_model[vectors]]

    def __call__(self, new_tokens):
        return self.tokens2vectors(new_tokens)

"""
class TextsVectorsTfIdf(TextsVectorsBoW):
    """"""
    def __init__(self, max_dict_size: int):
        super().__init__(max_dict_size)
        self.tfidf_model = None

    def model_fill(self, tokens: []):
        """"""
        assert self.tfidf_model is None, "the model is already filled"
        corpus = super().tokens2corpus(tokens)
        self.tfidf_model = TfidfModel(corpus)

    def tokens2vectors(self, tokens: []):
        """"""
        vectors = super().tokens2corpus(tokens)
        return self.tfidf_model[vectors]

    def __call__(self, new_tokens):
        return self.tokens2vectors(new_tokens)"""

if __name__ == "__main__":
    c2 = TextsVectorsTfIdf(10)
    tokens = [["мама", "мыла", "раму"], ["мама", "мыла", "раму", "деревянную"],
              ["мама", "ноги", "раму"]]

    c2.model_fill(tokens)
    for i in c2.dictionary:
        print(i, c2.dictionary[i])

    print(c2.tfidf_model)
    tokens2 = ["мама", "мыла", "раму", "деревянную"]
    d2 = c2.tokens2vectors([tokens2])
    print(len(c2.dictionary))
    print(d2)
    for i in c2.dictionary:
        print(i, c2.dictionary[i])

    print("c2.tfidf_model:", c2.tfidf_model)
    print([v for v in d2])
