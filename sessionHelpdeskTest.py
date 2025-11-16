"""
File to test the functionment of the Sessions and HelpDesk
To test the BLPOP of getHelpRequest, launch the file send_request.py after this one in another terminal
"""

from sessions import Sessions
from help_desk import HelpDesk

# Test of the sessions 
print("---------------------- Sessions ----------------------")

## Create an instance to use sessions
newSession = Sessions()

## I - Create sessions
print("")
print("----------- Creating sessions -----------")
print("")

print("Creating session with a missing key")
user1 = {"username":"dubiefe", "password":"1234", "privileges":"admin"}
try:
    newSession.create(**user1)
except Exception as e:
    print(f"Exception : {e}")
print("")

print("Creating session with a succesfull creation")
user2 = {"username":"dubiefe", "fullname":"emilie", "password":"1234", "privileges":"admin"}
try:
    newSession.create(**user2)
except Exception as e:
    print(f"Exception : {e}")
print("")

print("Creating session with a dupplicated username")
user3 = {"username":"dubiefe", "fullname":"emilie", "password":"1234", "privileges":"admin"}
try:
    newSession.create(**user3)
except Exception as e:
    print(f"Exception : {e}")
print("")

## II - Read sessions
print("")
print("----------- Reading sessions -----------")
print("")

print("Reading an existing session: ", newSession.read("dubiefe"))
print("Reading a non existing session: ", newSession.read("damien"))

## II - Update sessions
print("")
print("----------- Updating sessions -----------")
print("")

print("Updating an existing session: ", newSession.read("dubiefe"))
newData = {"fullname":"emilie dubief"}
newSession.update("dubiefe", **newData)
print("Updating the fullname with emilie dubief")
print("Updated session: ", newSession.read("dubiefe"))
print("")

print("Updating a non-existing session: ", newSession.read("damien"))
newData = {"fullname":"damien dubief"}
newSession.update("damien", **newData)
print("")

## III - Login with username and password
print("")
print("----------- Login with username and password -----------")
print("")

login1 = newSession.login("dubiefe","123")
print("Login with wrong info: ", login1)
login2 = newSession.login("dubiefe","1234")
print("Login with correct info: ", login2)

## IV - Login with token
print("")
print("----------- Login with token -----------")
print("")

print("Login with an existing token: ", newSession.login_token(login2["token"]))
print("Login with a non-existing token: ", newSession.login_token("123"))

## V - Delete session
print("")
print("----------- Delete session -----------")
print("")

newSession.delete("dubiefe")
print("Reading the deleted session: ", newSession.read("dubiefe"))


# Test of the helpdesk 
print("")
print("---------------------- HelpDesk ----------------------")
print("")

## Create an instance to use helpdesk
newHelpDesk = HelpDesk()

## I - Post help requests
newHelpDesk.postHelpRequest("dubiefe", "Hello world", 4)
newHelpDesk.postHelpRequest("david", "Hello world", 5)
newHelpDesk.postHelpRequest("thomas", "Hello world", 1)
newHelpDesk.postHelpRequest("claire", "Hello world", 6)
print("")

## II - Get help requests
print(newHelpDesk.getHelpRequest())
print(newHelpDesk.getHelpRequest())
print(newHelpDesk.getHelpRequest())
print(newHelpDesk.getHelpRequest())
print(newHelpDesk.getHelpRequest())
