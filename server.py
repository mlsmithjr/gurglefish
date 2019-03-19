__author__ = 'mark'
from flask import Flask, render_template, request, abort
from flask_cors import cross_origin, CORS
from flask_socketio import emit
from db.mdatadb import MDEngine
import json

import services.api


app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = 'secret!'

app.debug = True


@app.route("/")
def hello():
    return render_template('index.html')


@app.route("/home")
def home():
    return render_template('home.html')


@app.route("/createdb")
def createdb():
    return render_template('createdb.html')


@app.route("/services/envlist")
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def service_envlist():
    payload = services.api.envlist()
    return json.dumps(payload)


@app.route("/services/verifydb", methods=["POST"])
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def verifydb():
    if request.json is None or 'dbuser' not in request.json:
        abort(400)
    result = services.api.verifydb(request.json)
    return json.dumps(result)


@app.route("/services/verifysf", methods=["POST"])
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def verifysf():
    if request.json is None or 'login' not in request.json:
        abort(400)
    result = services.api.verify_salesforce(request.json)
    return json.dumps(result)


@app.route("/services/sobjects/<envname>")
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def service_sobjects(envname):
    payload = services.api.sobjects(envname)
    return json.dumps(payload)


@app.route("/services/sobject/<envname>/<sobject>/enable")
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def service_sobject_enable(envname, sobject):
    payload = services.api.enable_sobject(envname, sobject)
    return json.dumps(payload)


@app.route("/services/sobject/<envname>/<sobject>/disable")
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def service_sobject_disable(envname, sobject):
    payload = services.api.disable_sobject(envname, sobject)
    return json.dumps(payload)


#@socketio.on('enable-check')
#@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def handle_message(envname, sobject_name):
    print('checking ' + sobject_name)
    ok, reason = services.api.check_if_can_enable(envname, sobject_name)
    print(f'ok={ok}, reason={reason}')
    emit('enable-check-result', { 'can_enable': ok, 'sobject': sobject_name, 'reason': reason })


if __name__ == "__main__":
    global mde

    mde = MDEngine()
 #   socketio.run(app)
    app.run()