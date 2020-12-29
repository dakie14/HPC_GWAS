######  SERVER  ######
from flask import Flask, request

gwas_man = None
app = Flask("__GWASServer__")

@app.route('/covariates', methods=['GET'])
def get_covariates():
    global gwas_man
    return gwas_man.get_covariates().to_json(orient="records")


@app.route('/batch', methods=['GET'])
def get_batch():
    global gwas_man
    batch_size = request.args.get("batch_size")
    data = gwas_man.get_batch(batch_size)
    resp = app.make_response("")
    resp.status_code = 200
    resp.data = data
    return resp


@app.route('/result', methods=['POST'])
def store_result():
    global gwas_man
    gwas_man.store_result(request.get_data())
    return "OK"


def init_server(host, port, gwas_manager):
    global gwas_man
    gwas_man = gwas_manager
    app.run(
        host=host,
        port=port,
        debug=False
    )

