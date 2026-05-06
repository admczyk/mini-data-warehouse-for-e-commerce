from airflow.decorators import task
from airflow.models import Variable
import requests

WEBSITE_PATH = Variable.get("WEBSITE_PATH")

def clean_user_data(data):
    users = data["users"]
    clean_users = []

    for u in users:
        clean_users.append({
            "id": u["id"],
            "firstName": u["firstName"],
            "lastName": u["lastName"],
            "gender": u["gender"],
            "birthDate": u["birthDate"],
            "city": u["address"]["city"],
            "state": u["address"]["state"],
            "stateCode": u["address"]["stateCode"],
            "postalCode": u["address"]["postalCode"],
            "country": u["address"]["country"]
        })

    return clean_users

@task
def get_data(category):
    url = f"{WEBSITE_PATH}{category}?limit=0"
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        if category == "users":
            data = clean_user_data(data)

        return data
    except Exception as e:
        raise e
