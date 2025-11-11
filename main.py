from session import Session

newSession = Session()

user1 = {"username":"dubiefe", "password":"1234", "privileges":"admin"}
user2 = {"username":"dubiefe", "fullname":"emilie", "password":"1234", "privileges":"admin"}
user3 = {"username":"test", "fullname":"emilie", "password":"1234", "privileges":"admin"}

try:
    newSession.create(**user1)
except Exception as e:
    print(f"Exception : {e}")

try:
    newSession.create(**user2)
except Exception as e:
    print(f"Exception : {e}")

try:
    newSession.create(**user3)
except Exception as e:
    print(f"Exception : {e}")



print(newSession.read("damein"))
print(newSession.read("dubiefe"))

newData = {"fullname":"emilie dubief"}
newSession.update("dubiefe", **newData)

newSession.delete("test")

login1 = newSession.login("dubiefe","123")
login2 = newSession.login("dubiefe","1234")

print(login1)
print(login2)

print(newSession.login_token(login2["token"]))

