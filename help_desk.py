import redis
import uuid

class HelpDesk:
    """
    HelpDesk class
    Deal with help requests in redis

    Attributes
    ----------
    _database
        Connection to the redis database

    Methods
    -------
    postHelpRequest(self, username : str, message : str, priority : int)
        Post a help request in redis with a priority, a username and a message
    getHelpRequest(self) -> dict[str, str]
        Get the help request in redis with the highest priority, delete the request after reading
        Wait infinitly until a request arrives
    """

    _database = None

    def __init__(self):
        """
        Initialize the connection with redis
        """

        self._database = redis.Redis(host='localhost', port=6379, db=0)

    def postHelpRequest(self, username : str, message : str, priority : int):
        """
        Post a help request from a user with a priority

        Parameters:
        -----------
        username: str
            String with the username of the sender of the help request
        message: str
            String with the message of the help request
        priority: int
            int of the priority of the help request, 1 is the smallest priority
        """

        # Create Id for the request
        token = str(uuid.uuid4())
        helpRequestId = f"{priority:05d}:{token}"

        # Store message and user in an object with the id as the key
        self._database.hset(helpRequestId, mapping={"username":username, "message":message})

        # Store id in the list of request and sort the list
        self._database.lpush("HelpResquests", helpRequestId)
        self._database.sort("HelpResquests", alpha=True, store="HelpResquests")

        print(f"Request <{message}> from {username} stored with a priority of {priority}")

    def getHelpRequest(self) -> dict[str, str]:
        """
        Get the help request with the highest pritority
        If there are no requests, just wait
        Delete the request hanlded

        Returns:
        -----------
        dict[str, str]
            Dictionnary with the username and the message linked to the help request
        """

        # Get the highest request, wait if there are none
        print("Searching for a help request...")
        helpRequestId = self._database.brpop("HelpResquests", timeout=0)[1].decode()

        # Get the key linked to the id 
        print(f"Help request of priority {helpRequestId[0:5]} found")
        helpRequestUsername = self._database.hget(helpRequestId, "username")
        helpRequestMessage = self._database.hget(helpRequestId, "message")

        # Delete the key
        self._database.delete(helpRequestId)

        # Return result
        return {"username":helpRequestUsername.decode(), "message":helpRequestMessage.decode()}