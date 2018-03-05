__author__ = 'mark'
from flask_cors import cross_origin, CORS
from flask import Flask, render_template
from db.mdatadb import MDEngine
import json

# import services
import services.api
#from services.DBList import RestDBList
#from services.TestEnv import TestEnv
#from services.SaveEnv import SaveEnv
#from services.Tables import GetTables

app = Flask(__name__)
CORS(app)

#api = Api(app)
app.debug = True

#api.add_resource(RestDBList, '/services/envlist')
#api.add_resource(TestEnv, '/services/testEnv')
#api.add_resource(SaveEnv, '/services/saveEnv')
#api.add_resource(GetTables, '/services/tables')



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

@app.route("/services/sobjects/<envname>")
@cross_origin(origin='localhost',headers=['Content-Type','Authorization'])
def service_sobjects(envname):
    payload = services.api.sobjects(envname)
    return json.dumps(payload)


if __name__ == "__main__":
    global mde

    mde = MDEngine()
    app.run()
