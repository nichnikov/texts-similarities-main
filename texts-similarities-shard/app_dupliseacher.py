from flask import Flask, jsonify, request
from flask_restplus import Api, Resource, fields
from storage import Worker
from uuid import uuid4
from hashlib import md5
from waitress import serve
import logging


def data_prepare(json_data):
    """преобразует входящие словари в список кортежей"""
    queries_in = []
    for d in json_data["data"]:
        queries_in += [(d["locale"], d["moduleId"], str(uuid4()),
                        d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]
    return queries_in


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
        queries = data_prepare(json_data["data"])

        if json_data["data"]:
            queries = data_prepare(json_data["data"])
            lc, md, q_i, a_i, m_i, txs, p_ids = zip(*queries)
            data = json_data["data"]
            if json_data["operation"] == "add":
                main.add(data)
                logger.info("quantity:" + str([len(m.ids) for m in main.matrix_list]))
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list])})

            elif json_data["operation"] == "delete":
                q_i, a_i, m_i, txs, p_ids = zip(*data)
                main.delete(list(set(q_i)))
                logger.info("quantity:" + str([len(m.ids) for m in main.matrix_list]))
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list])})

            elif json_data["operation"] == "update":
                main.update(data)
                logger.info("data were updated")
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list])})

            elif json_data["operation"] == "search":
                try:
                    if "score" in json_data:
                        search_results = main.search(data, json_data["score"])
                    else:
                        search_results = main.search(data)
                    return jsonify(search_results)
                except:
                    return jsonify([])
        else:
            if json_data["operation"] == "delete_all":
                main.delete_all()
                logger.info("quantity:" + str([len(m.ids) for m in main.matrix_list]))
                return jsonify({"quantity": sum([len(m.ids) for m in main.matrix_list])})


if __name__ == "__main__":
    # serve(app, host="0.0.0.0", port=8080)
    app.run(host='0.0.0.0', port=8080)
