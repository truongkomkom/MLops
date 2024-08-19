from pymongo.mongo_client import MongoClient
uri = "mongodb+srv://truongkomkom1:AhwYlfKHSbJISkV7@cluster0.6ssm4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Create a new client and connect to the server
client = MongoClient(uri)

database = client["youtubecomunity"]
collection = database["session"]
data = {
    "coursename": "genai",
    "instrutorname": "sunny",
    "modeofsession": "english"
}
collection.insert_one(data)