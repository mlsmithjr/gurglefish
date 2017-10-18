

__author__ = 'mark'
from flask import Flask, render_template
from flask_restful import Resource, Api
from flask_cors import CORS, cross_origin
from db.mdatadb import MDEngine

# import services
from services.DBList import RestDBList
from services.TestEnv import TestEnv
from services.SaveEnv import SaveEnv
from services.Tables import GetTables

app = Flask(__name__)
CORS(app)           # TEMPORARY ! ! !
api = Api(app)
app.debug = True

api.add_resource(RestDBList, '/services/envlist')
api.add_resource(TestEnv, '/services/testEnv')
api.add_resource(SaveEnv, '/services/saveEnv')
api.add_resource(GetTables, '/services/tables')



@app.route("/")
def hello():
    return "Hello World!"

@app.route("/home")
def home():
    return render_template('home.html')

@app.route("/createdb")
def createdb():
    return render_template('createdb.html')




if __name__ == "__main__":
    global mde

    mde = MDEngine()
    app.run()
