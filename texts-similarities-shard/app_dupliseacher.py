from flask import Flask, jsonify, request
from flask_restplus import Api, Resource, fields
from storage import Worker
from uuid import uuid4
from itertools import chain, groupby
from collections import namedtuple
from hashlib import md5
from waitress import serve
import logging


def data_prepare(json_data):
    """преобразует входящие словари в список кортежей"""
    queries_in = []
    for d in json_data:
        queries_in += [(d["locale"], d["moduleId"], str(uuid4()),
                        d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]
    return queries_in


def resulting_report(searched_data, result_tuples, found_dicts_l, locale: str):
    """"""

    def grouping(similarity_items, searched_queries, searched_answers_moduls, locale: str):
        """"""
        return [{"id": k1, "locale": locale, "moduleId": searched_answers_moduls[k1], "clustersWithDuplicate":
            [{"cluster": searched_queries[k2]["cluster"], "duplicates":
                [{"cluster": x2.FoundText, "id": x2.FoundAnswerId, "moduleId": x2.FoundModuleId,
                  "pubId": x2.FoundPubIds} for x2 in v2]}
             for k2, v2 in
             groupby(sorted([x1 for x1 in v1], key=lambda c: c.SearchedQueryId), lambda d: d.SearchedQueryId)]}
                for k1, v1 in
                groupby(sorted(similarity_items, key=lambda a: a.SearchedAnswerId), lambda b: b.SearchedAnswerId)]

    ResultItem = namedtuple("ResultItem", "SearchedAnswerId, "
                                          "SearchedText, "
                                          "SearchedQueryId, "
                                          "SearchedModuleId, "
                                          "SearchedPubIds, "
                                          "FoundAnswerId, "
                                          "FoundText, "
                                          "FoundQueryId, "
                                          "FoundModuleId, "
                                          "FoundPubIds")

    searched_dict = {q_i: {"answerId": a_i,
                           "moduleId": m_i,
                           "cluster": cl,
                           "pubIds": p_i} for q_i, a_i, m_i, cl, p_i, vcs in searched_data}

    searched_answers_moduls = {a_i: m_i for q_i, a_i, m_i, cl, p_i, vcs in searched_data}

    found_dict = {d["queryId"]: d for d in chain(*found_dicts_l)}
    similarity_items = [ResultItem(searched_dict[sa_i]["answerId"],
                                   searched_dict[sa_i]["cluster"],
                                   sa_i,
                                   searched_dict[sa_i]["moduleId"],
                                   searched_dict[sa_i]["pubIds"],
                                   found_dict[fa_i]["answerId"],
                                   found_dict[fa_i]["cluster"],
                                   fa_i,
                                   found_dict[fa_i]["moduleId"],
                                   found_dict[fa_i]["pubIds"]) for sa_i, fa_i, sc in result_tuples]

    return grouping(similarity_items, searched_dict, searched_answers_moduls, locale)


def result_aggregate(respons):
    result_tuples_list = []
    result_dicts_list = []
    for x in respons:
        if x:
            result_tuples_list += x[0]
            result_dicts_list += x[1]
    return result_tuples_list, result_dicts_list



logger = logging.getLogger("app_duplisearcher")
logger.setLevel(logging.DEBUG)


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
api = Api(app)

name_space = api.namespace('api', 'На вход поступает JSON, возвращает JSON')

query = name_space.model("One Query",
                         {"id": fields.String(description="query's Id", required=True),
                          "cluster": fields.String(description="query's text", required=True)})

input_data = name_space.model("Input JSONs",
                              {"score": fields.Float(description="The similarity coefficient", required=True),
                               "data": fields.List(fields.Nested(query)),
                               "operation": fields.String(description="add/update/delete/search/del_all",
                                                          required=True)})

main = Worker(50000, 33000)


@name_space.route('/')
class CollectionHandling(Resource):
    """Service searches duplicates and adding and delete data in collection."""

    @name_space.expect(input_data)
    def post(self):
        """POST method on input JSON file with scores, operation type and lists of fast answers."""
        json_data = request.json

        if json_data["data"]:
            queries = data_prepare(json_data["data"])
            if json_data["operation"] == "add":
                main.add(queries)
                logger.info("quantity:" + str([len(m.ids) for m in main.matrix_list.ids_matrix_list]))
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list.ids_matrix_list])})

            elif json_data["operation"] == "delete":
                lc, md, q_i, a_i, m_i, txs, p_ids = zip(*queries)
                main.delete(list(set(q_i)))
                logger.info("quantity:" + str([len(m.ids) for m in main.matrix_list.ids_matrix_list]))
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list.ids_matrix_list])})

            elif json_data["operation"] == "update":
                main.update(queries)
                logger.info("data were updated")
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list.ids_matrix_list])})

            elif json_data["operation"] == "search":
                try:
                    if "score" in json_data:
                        search_results = main.search(queries, json_data["score"])
                    else:
                        search_results = main.search(queries)
                    return jsonify(search_results)
                except:
                    return jsonify([])
        else:
            if json_data["operation"] == "delete_all":
                main.delete_all()
                logger.info("quantity:" + str([len(m.ids) for m in main.matrix_list.ids_matrix_list]))
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list.ids_matrix_list])})


if __name__ == "__main__":
    # serve(app, host="0.0.0.0", port=8080)
    app.run(host='0.0.0.0', port=8080)
