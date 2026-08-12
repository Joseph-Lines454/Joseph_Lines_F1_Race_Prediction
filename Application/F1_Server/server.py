# Python 3 server example

import time
import requests
import json
import http.client
from pymongo import MongoClient
import asyncio
from fastapi import FastAPI, WebSocket, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from datetime import date
from pydantic import BaseModel
import paho.mqtt.client as mqtt
import ssl
from urllib.request import urlopen
import json
import requests
import aiohttp
import os
import bcrypt
import uuid
import os
#CONNECTION MANAGER TO ALLOW FOR OUR WEBSOCEKTS TO BE USED

app = FastAPI()


origins = [
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
    )




#Check if the data is live somehow - make a request and see the status of the session

def CheckCookies(checkCookies):
  print("Check Cookies")
  if checkCookies != None:
    data = UserData.find({'SessionID': checkCookies})
    data = list(data)
    print(data)
    if data != []:
      print("Here!")
      return True
    else:
      print("WE DONT HAVE A COOKIE!")
      return False
  return False

RAPIDAPI = http.client.HTTPSConnection("f1-live-pulse.p.rapidapi.com")
headersLIVE = {
    'x-rapidapi-key': "",
    'x-rapidapi-host': "f1-live-pulse.p.rapidapi.com",
    'Content-Type': "application/json"
    }


async def Run():
  print()

class ConnectionManager:
  def __init__(self):
    self.active_connections: list[WebSocket] = []

  async def connect(self,websocket: WebSocket):
    await websocket.accept()
    print("We should have connected to the websocket!!!")
    self.active_connections.append(websocket)

  async def disconnect(self, websocket: WebSocket):
    self.active_connections.remove(websocket)
  async def send_personal_message(self,message: str, websocket: WebSocket):
    await websocket.send_text(message)
  async def broadcast(self,message):
    print("We are broadcasting the message")
    print("Active connections:", len(self.active_connections))
    for connection in self.active_connections:
      await connection.send_json(message)
    print("Working??")


manager = ConnectionManager()
loop = None
#This is going to be used for live telem
token_url = "https://api.openf1.org/token"
paramsLive_Telem = {
    "username": "joelines194@gmail.com",
    "password": ""
}
response_F1= requests.post(token_url,data=paramsLive_Telem)

#This is working

if response_F1.status_code == 200:
  response = response_F1.json()
  access_token = response["access_token"]
else:
  #print(response_F1.text)
  print(None)

#Hyprace is going to be used for everything else

response = http.client.HTTPSConnection("hyprace-api.p.rapidapi.com")
headers = {
    'x-rapidapi-key': "",
    'x-rapidapi-host': "hyprace-api.p.rapidapi.com",
   'Accept': "application/json",
    }



#clientDatabase = MongoClient()

#Get the current year
Year = date.today().year

#clientDatabase = MongoClient("mongodb://username:password@database:27017/Drivers?authSource=admin")
clientDatabase = MongoClient(os.getenv("MONGO_URL"))
newdatabase = clientDatabase["f1db"]
Driver_Lap_Times = newdatabase["Driver_Lap_Times"]
Races = newdatabase['Races']
#we need to store the standings after each race in the database
Drivers_Standings = newdatabase["Drivers_Standings"]
Teams_Standings = newdatabase["Teams_Standings"]
Driver_Names = newdatabase["Driver_Names"]
Team_Names = newdatabase["Team_Names"]
SessionData = newdatabase["SessionData"]
RaceResults = newdatabase["RaceResults"]
UserData = newdatabase["loginInfo"]
def on_connect(client, userdata, flags, rc, properties = None):
  print("we are here!!")
  if rc == 0:
        #we can change all of this shit
        print("Connected to OpenF1 MQTT broker")
        #client.subscribe("v1/location")
        #client.subscribe("v1/laps")
        client.subscribe("v1/laps")
        client.subscribe("v1/race_control")
        client.subscribe("v1/sessions")
        client.subscribe("v1/overtakes")
        print(client.subscribe("v1/laps"))
        print(client.subscribe("v1/race_control"))
        print(client.subscribe("v1/sessions"))
        print(client.subscribe("v1/overtakes"))
        #client.subscribe("#")
  else:
    print(f"Failed to connect, return code {rc}")
  
#AFTER THE RACE HAS FINISHED WE NEED TO BE UPDATING STANDINGS AS WELL AS UPDATING RACE RESULTS - THIS IS IMPERITIVE - Also need to get the ML model running after qualifying
"""
SessionType = ""
output_file = "/usr/app/src/race_data.jsonl"
os.makedirs(os.path.dirname(output_file), exist_ok=True)
def on_message(client,userdata,msg):

  #global SessionType
  print("We have recived a message for OpenF1 API!!!")
  data = json.loads(msg.payload.decode())
  topic = msg.topic
  print(data)
  jsondata = {"topic" : topic, "data": data}
  with open(output_file, "a") as f:
    f.write(json.dumps(jsondata))

  #Get the session type based on the ID for the live timings
 
  SendData({"topic": topic, "data" : data})
#def PresentRace_Data(RaceData):



#This is looking like it is done which is fantastic
def PresentQualifyingData(DataQualifying):
  #Convert the lap time back to zero
  x = (SessionData.find_one({"driver_number" : DataQualifying["driver_number"]}))
  print(x)
  
  if "message" in DataQualifying and "qualifying_phase" in DataQualifying:
    if DataQualifying["message"] == "SESSION FINISHED":
      print("delete all data from that collection")
      global SessionType
      SessionType = ""
      SessionData.delete_many({})
      print("success maybe???")
      #we delete all data from qualifying
  else:
    x = list(SessionData.find({"driver_number" : DataQualifying["driver_number"]}))
    print(x)
    
    if x == [] and DataQualifying["lap_duration"] != None:
      SessionData.insert_one(DataQualifying)
    
    
    elif x[0]["lap_duration"] <= DataQualifying["lap_duration"] and DataQualifying["lap_duration"] != None :
      #Aim of this is to delete laptimes which are old - and replace with new ones
      SessionData.delete_one({"driver_number" : x[0]["driver_number"]})
      SessionData.insert_one(DataQualifying)
      print("Success!!")
    #WE NEED TO TRIGER A WEBSOCKET HERE to get data back -
    
  GetCurrentSessionsData()

def GetCurrentSessionsData():
  print("We are here and need to be")
  x = list(SessionData.find())
  print(x)
  asyncio.run_coroutine_threadsafe(manager.broadcast({"type" : "qyalifying_update", "data": x }), loop)

def SendData(myData):
  asyncio.run_coroutine_threadsafe(manager.broadcast(myData), loop)
#def UpdateSessionStatus():

#We know what this data will look like
def PresentPracticeData():
  print("This is where we present practice data")
  #We need to put all of the data into the database then based on new data compare that drivers old times and see if it is faster then replace and send a response to the client

mqtt_broker = "mqtt.openf1.org"
mqtt_port = 8883
mqtt_username = "joelines194@gmail.com"

client = mqtt.Client()
client.username_pw_set(username=mqtt_username, password=access_token)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.tls_insecure_set(False)
client.on_connect = on_connect
client.on_message = on_message

"""
def GetStandings(newData):
  data = []
  teams = []
  for i in newData["items"]:  
    teams.append({
        "name": i["name"],
        "team_position": i["constructors"][0]["standing"]["position"],
        "points": i["constructors"][0]["standing"]["points"]
      })
    for j in i["drivers"]:
          if j["drivingLevel"] == "GrandPrix":
            if "standing" in j:
              data.append({
                "name" : j["firstName"] + " " + j["lastName"],
                "id" : j["id"],
                "position": j["standing"]["position"],
                "points": j["standing"]["points"]
              }
              )
  return [data,teams]

# THIS is ready for the race in China - This is important
async def UpdateStandings():
  season = await CurrentSeason()
  response = requests.get(f"https://hyprace-api.p.rapidapi.com/v2/seasons/{season}/teams?pageSize=25", headers=headers)

  data = response
  newData = data.json()
  dataStore = GetStandings(newData)
  #we need to add the date each time that we store the data
 
  for i in dataStore[0]:      
    i["timestamp"] = str(datetime.today().strftime("%y-%m-%d"))
  x = Drivers_Standings.insert_many(dataStore[0])

  for j in dataStore[1]:
    j["timestamp"] = str(datetime.today().strftime("%y-%m-%d"))
  x = Teams_Standings.insert_many(dataStore[1])
  print("We are here!!")

#Function to get the standings back

#This is the function which got all of the races and stored them in the database
async def functionStoreRaces():
  page = 1
  while True:
    response = requests.get(f"https://hyprace-api.p.rapidapi.com/v2/grands-prix?pageNumber={page}",headers=headers)
    data = response
    newData = data.json()
    #print(newData)
    #Loop through all items
    mylist = []
    for item in newData["items"]:
      items = {"Raceid" : item["id"],"round" : item["round"],"name" : item["name"],"season" : item["season"]["year"], "Eventid": item["schedule"][0]["id"] }
      mylist.append(items)
      #print(items)
   # print(newData["hasNext"])
    
    x = Races.insert_many(mylist)
   
    
    if newData["hasNext"] != True:
      break
    
    page = page + 1

#if i get this done tommorow,that might be better - because more API requests
async def GetRaceResults():

  

  #We need to get all of the race results from the Races section first off, then loop through and apply all of the items that we need 
  #we could also send a request to get the drivers name but i think it would be better downloading becuase i highly doubt we are going to have enough API requests for that
  x = list(Races.find({}))
  myList = []
  for item in x:
    #print(item)
    response = requests.get(f"https://hyprace-api.p.rapidapi.com/v2/grands-prix/{item["Raceid"]}/races/{item["Eventid"]}/results", headers=headers)

    #print("Right place")
    #c0c47b04-21d3-4765-c8fa-08d94ab130d2
    data  = response.json()
    #print(data)
    #print(data)
    if (data["participations"] != [] ):
        for individual_results in data["participations"]:
          #we need to sort through the data, would be better if it was individual rows          
          if "time" in individual_results["result"] and "gapToLeader" not in individual_results["result"]:
            items = {"Raceid" : item["Raceid"],"Eventid": item["Eventid"], "Driverid" : individual_results["driverId"], "Teamid" : individual_results["teamId"], "FinishedPostion" : individual_results["result"]["finishedPosition"], "Finaltime": individual_results["result"]["time"],"points": individual_results["result"]["points"],"laps": individual_results["result"]["laps"],"gapToLeader": 0 }
            myList.append(items)
          if "time" in individual_results["result"] and "gapToLeader" in individual_results["result"]:
            print(individual_results["result"])
            items = {"Raceid" : item["Raceid"],"Eventid": item["Eventid"], "Driverid" : individual_results["driverId"], "Teamid" : individual_results["teamId"], "FinishedPostion" : individual_results["result"]["finishedPosition"], "Finaltime": individual_results["result"]["time"],"points": individual_results["result"]["points"],"laps": individual_results["result"]["laps"],"gapToLeader": individual_results["result"]["gapToLeader"] }
            myList.append(items)
          else:
            items = {"Raceid" : item["Raceid"],"Eventid": item["Eventid"], "Driverid" : individual_results["driverId"], "Teamid" : individual_results["teamId"], "FinishedPostion" : individual_results["result"]["finishedPosition"], "Finaltime": "N/A","points": individual_results["result"]["points"],"laps": individual_results["result"]["laps"],"gapToLeader": "N/A" }
            myList.append(items)
          #print(items) 
          

      
    
  xin = RaceResults.insert_many(myList)
  #print("Indatabase")
    
       
  
async def GetTeams():
  #print("We want to go through all of the pages and put the teams inside of the mongodb database")
 # print("73ee4826-7cf7-410e-a4c0-eb48b2f4ae79")
  page = 1
  
  while True:
    response = requests.get(f"https://hyprace-api.p.rapidapi.com//v2/teams?pageSize=25&pageNumber={page}", headers=headers)
    data = response
    newData = data.json()
    #print(newData)
    #Loop through all items
    mylist = []
    
    for item in newData["items"]:
      if "fullName" in item:
        items = {"teamid" : item["id"], "fullName" : item["fullName"], "color" : item["color"], "country" : item["country"]["name"], "countryshort" : item["country"]["alphaThreeCode"]}
        mylist.append(items)
        print(items)
      else:
        items = {"teamid" : item["id"], "fullName" : item["name"]}
        mylist.append(items)
    #print(newData["hasNext"])
    
    x = Team_Names.insert_many(mylist)
   
    
    if newData["hasNext"] != True:
      break
    
    page = page + 1
    


async def GetDrivers():
  """
  #response = requests.get("https://hyprace-api.p.rapidapi.com/v2/drivers/8a99b5a0-8e1a-4bbc-2155-08d9161fe7c5", headers=headers)
  response = requests.get("https://hyprace-api.p.rapidapi.com//v2/drivers?pageSize=25&pageNumber=1", headers=headers)
  
  data = response
  newData = data.json()

  print(newData)
  print("e78d2503-7af4-4f96-1ec3-08d9161fe7c5")
  """
  page = 1
  while True:
    response = requests.get(f"https://hyprace-api.p.rapidapi.com//v2/drivers?pageSize=25&pageNumber={page}", headers=headers)
    data = response
    newData = data.json()
    print(newData)
    #Loop through all items
    mylist = []
    for item in newData["items"]:
      items = {"driverid" : item["id"], "firstName" : item["firstName"], "lastName" : item["lastName"], "birthdate" : item["birthDate"], "country" : item["country"]["name"], "countryshort" : item["country"]["alphaThreeCode"]}
      mylist.append(items)
      #print(items)
    #print(newData["hasNext"])
    
    x = Driver_Names.insert_many(mylist)
   
    
    if newData["hasNext"] != True:
      break
    
    page = page + 1
  

#We probably will be fine just storing them  
async def CurrentSeason():
  response = requests.get("https://hyprace-api.p.rapidapi.com/v2/seasons?year=2026", headers=headers)
  data = response
  newData = data.json()
  return newData["items"][0]["id"]

async def CheckAPIStrings():
  response = requests.get("https://hyprace-api.p.rapidapi.com/v2/grands-prix/87cbb3ec-830b-4fa4-849d-6db01c1461fb/races/3969ed3f-e874-4d18-a621-f3e2bfe060d3/results", headers=headers)

  print("Right place")
  #c0c47b04-21d3-4765-c8fa-08d94ab130d2
  data  = response.json()
  #print(data)

  
  #print(response.json())

class GetRaceR(BaseModel):
  EventID: str
  RaceID: str

class SeasonYearData(BaseModel):
  year: int




@app.on_event("startup")
async def startup():
  print("Startup")
"""
async def startup_mqtt():
  
  try:
    client.connect(mqtt_broker, mqtt_port, 60)
    client.loop_start()
    global loop
    loop = asyncio.get_running_loop()
    print("MQTT connection started")
  except Exception as e:
        print("MQTT failed:", e)
  """
  #responsea = requests.get(f"https://hyprace-api.p.rapidapi.com/timingData", headers=headers)
  #response = responsea.json()
  #if (response["sessionStatus"] != "Ends"):
  #  asyncio.run(RaceRunning)
  #asyncio.create_task(poll_hyprace())
@app.get("/F1_Statistics")
async def root(request: Request,response: Response):
  CheckCookiesOut = CheckCookies(request.cookies.get("session_cookie"))
  print("Cookie " + str(request.cookies.get("session_cookie")))
  if CheckCookiesOut != False:
    season = await CurrentSeason()
    #await CheckAPIStrings()
    response = requests.get(f"https://hyprace-api.p.rapidapi.com/v2/seasons/{season}/teams?pageSize=25", headers=headers)
    #await UpdateStandings()
    data = response
    newData = data.json()

    data = []
    sendJSON = GetStandings(newData)
    response.status_code = 200
    return sendJSON
  else:
    response.status_code = 401
    return None
#Might need to return the races in order but we can do this no problem
@app.get("/F1_Standings_Over_Time")
async def root(request: Request,response: Response):
  CheckCookiesOut = CheckCookies(request.cookies.get("session_cookie"))
  print("Cookie " + str(request.cookies.get("session_cookie")))
  if CheckCookiesOut != False:

    #code that gets all of the data needed back from the database
    x = list(Drivers_Standings.find({"timestamp" : {"$regex": "26"}}).sort({"timestamp": 1}))
    list(x)

    for data in x:
      data["_id"] = str(data["_id"])

    y = list(Teams_Standings.find({"timestamp" : {"$regex": "26"}}).sort({"timestamp": 1}))
    list(y)
    for data in y:
      data["_id"] = str(data["_id"])

    data = [x,y]

    return data
  else:
    #should raise an error here because yeah important
    response.status_code = 401
    return None


#Historical is looking good also need to get race results based on id, need to store in database

async def WebsocketSend_Test():
  #print("This is running")
  #print (f"Broadcasting to {len(manager.active_connections)} clients")
  if loop:
    print("There is a loop")
    asyncio.run_coroutine_threadsafe(manager.broadcast({"type" : "qualifying_update", "data": "This is somedatahahaha" }), loop)
  else:
    print("No loop")




@app.post("/GetData")
def root(Data: GetRaceR, request: Request,response: Response):
  CheckCookiesOut = CheckCookies(request.cookies.get("session_cookie"))
  if CheckCookiesOut == True:
    x = list(RaceResults.find({'Raceid': Data.RaceID, "Eventid" : Data.EventID }))
    print("We are here!")
    myList = []
    #Here we need to get the driver name as well as the team name
      
    for d in x:
      TN = Team_Names.find_one({"teamid" : d["Teamid"]})
      DN = Driver_Names.find_one({"driverid" : d["Driverid"]})
      DriverData = {"raceid" : d["Raceid"], "Eventid" : d["Eventid"], "Driverid" : d["Driverid"], "FinishedPosition" : d["FinishedPostion"], "Finaltime" : d["Finaltime"],"DriverName" : DN["firstName"] + " " + DN["lastName"], "country": DN["countryshort"], "fullName" : TN["fullName"], "colour" : TN["color"], "points": d["points"]  }
        
      myList.append(DriverData)
        #print(d)
    return myList
  else:
    response.status_code = 401
    return None

@app.post("/Historical")
async def root(Data: SeasonYearData):
  
  #Get the data based on the year
  x = list(Races.find({'season': Data.year}))
  x = list(x)
  for races in x:
    races["_id"] = str(races["_id"])
    
  return x






@app.websocket("/ws")
async def websocket_endpoint(websocket:WebSocket):
  try:
    await websocket.accept()
    manager.active_connections.append(websocket)
    while True:
        print("Here!!adwdw")
        data = await websocket.receive_text()
        
        await websocket.send_text(f"Message text was {data}")
  except:
     manager.active_connections.remove(websocket)

  
@app.get("/testaba")
def root():
  #await GetTeams()
  #await GetRaceResults()
  
  #await WebsocketSend_Test()
  #print("We have reached test")
  GetCurrentSessionsData()

@app.get("/F1_Race_Predictions")
def root():
  response = requests.get("https://hyprace-api.p.rapidapi.com/v2/grands-prix/260d5205-5003-4038-a392-f23c4f57e6b4", headers=headers)

  #c0c47b04-21d3-4765-c8fa-08d94ab130d2
  data  = response.json()
  print(data)
  #print(data)
  return "We have successfully Quried the other server!!!"


@app.get("/TestMLModels")
def root(request: Request,response: Response):

  print(request.cookies.get("session_cookie"))
  CheckCookiesOut = CheckCookies(request.cookies.get("session_cookie"))
  print("We made it here!")
  if CheckCookiesOut == True:
    #Get the event data, circuit data then we can map it and pass to the ML server
    #We are going to use the data from china - we need to get qualifying data
    response = requests.get("https://hyprace-api.p.rapidapi.com/v2/grands-prix/c267fde7-de50-4d56-a2fc-1313439daa43/qualifying/83b18780-2a95-49ea-ab9e-75d9733b716a/results", headers=headers)
    #print(response)

    

    data  = response.json()
    response2 = requests.get("https://hyprace-api.p.rapidapi.com/v2/grands-prix/c267fde7-de50-4d56-a2fc-1313439daa43", headers = headers)
    data2 = response2.json()
    #print(data2)
    MyList = []
    
    for item in data["results"]:
      
      if "q1" in item:
        items = {"driverId" : str(item.get("driverId")),"teamId": str(item.get("teamId")), "q1" : str(item.get("q1")), "gridposition": str(item.get("position")),"circuitId": str(data2.get("circuitId")),"date": str(data2["schedule"][1]["startDate"])}
        #MyList.append(items)

      if "q2" in item:
        items = {"driverId" : str(item.get("driverId")),"teamId": str(item.get("teamId")), "q1" : str(item.get("q1")), "q2" :  str(item.get("q2")), "gridposition": str(item.get("position")), "circuitId": str(data2.get("circuitId")),"date": str(data2["schedule"][1]["startDate"])}
        #MyList.append(items)

      if "q3" in item:
        items = {"driverId" : str(item.get("driverId")),"teamId": str(item.get("teamId")), "q1" : str(item.get("q1")), "q2" : str(item.get("q2")), "q3": str(item.get("q3")), "gridposition": str(item.get("position")), "circuitId": str(data2.get("circuitId")),"date": str(data2["schedule"][1]["startDate"]) }
        #MyList.append(items)
      MyList.append(items)
      print(items)
    
  
    
    #x = list(Team_Names.find({'teamid': item.get("teamId")}))
    #x = list(x)
    #print(str(item.get("teamId")) + "and the name: " + str(x[0]["fullName"]))
  
  #we need to get the weather results
  #2026-03-14T07:00:00Z start date for qualifying
  
  #now we need to go through each of the vairables

  #CircuitID - a18e0166-6188-4b0f-7c87-08d9161fe87b
  
  #Name Chinese Grand Prix
  
    response = requests.post("http://host.docker.internal:2000/MLModelPerformance",json=MyList)
    #response = requests.get("http://host.docker.internal:2000/MLModelPerformance")
    #c0c47b04-21d3-4765-c8fa-08d94ab130d2
    data  = response.json()
    print(data)
    #GetDriverNames(data)
    #print(data)
    response.status_code = 200
    return GetDriverNames(data)

  else:
    response.status_code = 401
    return None

  
def GetDriverNames(DriverData):
  DriverList = []
  print("We are gettin drivernames")
  count = 0
  for x in DriverData:
    name = Driver_Names.find({"driverid": DriverData[count]["DriverName"]})
    name = list(name)
    #DriverList.append({"DriverName": Driver_Names.find({"driverid": DriverData[0]["DriverName"]})})
    DriverList.append({"DriverName": name[0]["firstName"] + " " + name[0]["lastName"], "DriverPosition":  DriverData[count]["DriverPositon"]})
    print( DriverList[count])
    count = count + 1

  return DriverList


class Credentials(BaseModel):
  username: str
  password: str
  email: str


class CredentialsLogin(BaseModel):
  username: str
  password: str


#now we need to implement cookies

@app.post("/Login")
async def root(Cred: CredentialsLogin, response: Response):
  UserName = UserData.find({'username': Cred.username})
  UserName = list(UserName)
  try:
    hashedPasswordCheck = UserName[0]["password"]
  
  
    if bcrypt.checkpw(Cred.password.encode("utf-8"), hashedPasswordCheck):
      Cookies = str(uuid.uuid4())
      response.set_cookie(key="session_cookie", value=Cookies, httponly=True,samesite="lax",secure=False)
      data = UserData.update_one({"username": Cred.username}, {"$set" : {"SessionID": Cookies}})
      print(data)
      response.status_code = 200
      return "Passwords Match"
    else:
      response.status_code = 401
      return "Incorrect Username/Password"
  except Exception as e:
    print(e)
    response.status_code = 401
    return "Incorrect Username/Password"


@app.post("/Register")
async def root(Cred: Credentials,request: Request,response: Response):
  print("We have Registered into the application")
  try:
    UserName = UserData.find({'username': Cred.username})
    UserName = list(UserName)

    #We need to hash the password

    

    # if username is found, do x
    if UserName != []:
      print("This username is already in the database!")
      response.status_code = 401
      return "This username is already in the database"
    else:
      hashed_password = bcrypt.hashpw(Cred.password.encode('utf8'), bcrypt.gensalt())
      UserData.insert_one({'username': Cred.username, 'password': hashed_password, 'email': Cred.email})
      response.status_code = 200
      return "Valid login"
  except:
    response.status_code = 401
    return "This username/email"
    return None
  
  #check if username is already taken


