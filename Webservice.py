from ProcessLogger import ProcessLogger

######  SERVER  ######
from flask import Flask, request

##### OVERWITE FLASK LOGGING #####
def secho(text, file=None, nl=None, err=None, color=None, **styles):
    pass

def echo(text, file=None, nl=None, err=None, color=None, **styles):
    pass

def quiet():
    import logging
    import click

    # Set Flask to only output errors
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    # Disable setup output
    click.echo = echo
    click.secho = secho

##### SERVER SETUP #####

manager = None
logger = ProcessLogger("__GWASServer__")
app = Flask("__GWASServer__")

@app.route('/status', methods=['GET'])
def get_status():
    return app.make_null_session()

@app.route('/covariates', methods=['GET'])
def get_covariates():
    global manager
    return manager.get_covariates().to_json(orient="records")


@app.route('/batch', methods=['GET'])
def get_batch():
    global manager
    batch_size = int(request.args.get("batch_size"))
    data = manager.get_batch(batch_size)
    resp = app.make_response("")
    resp.status_code = 200
    resp.data = data
    return resp


@app.route('/result', methods=['POST'])
def store_result():
    global manager
    manager.store_result(request.get_data())
    return "OK"


def init_service(host, port, service_manager, verbose=False):
    if not verbose:
        quiet()

    global manager
    manager = service_manager

    app.run(
        host=host,
        port=port,
        debug=False
    )

