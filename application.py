import pickle
import numpy as np
import pandas as pd
from flask import Flask,render_template,request,jsonify
from src.logger import get_logger
from src.feature_store import RedisFeatureStore
from sklearn.preprocessing import StandardScaler


logger = get_logger(__name__)


app = Flask(__name__ , template_folder="templates")

MODEL_PATH = "artifacts/models/random_forest_model.pkl"
with open(MODEL_PATH , 'rb') as model_file:
    model = pickle.load(model_file)

FEATURE_NAMES = ['age', 'fare', 'pclass', 'sex', 'embarked', 'Familysize', 'Isalone',
       'HasCabin', 'Title', 'Pclass_fare', 'age_fare']


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/predict' , methods=['POST'])
def predict():
    try:
        data = request.form
        Age = float(data["Age"])
        Fare = float(data["Fare"])
        Pclass = int(data["Pclass"])
        Sex = int(data["Sex"])
        Embarked = int(data["Embarked"])
        Familysize = int(data["Familysize"])
        Isalone = int(data["Isalone"])
        HasCabin = int(data["HasCabin"])
        Title = int(data["Title"])
        Pclass_Fare = float(data["Pclass_Fare"])
        Age_Fare = float(data["Age_Fare"])

        features = pd.DataFrame([[Age,Fare,Pclass,Sex,Embarked,Familysize,Isalone,HasCabin,Title,Pclass_Fare,Age_Fare]] , columns=FEATURE_NAMES)


        prediction = model.predict(features)[0]

        result = 'Survived' if prediction==1 else 'Did Not Survive'

        return render_template('index.html' , prediction_text = f"The predictions is : {result}")
    
    except Exception as e:
        return jsonify({'error' : str(e)})


if __name__ == "__main__":
    app.run(debug=True)



