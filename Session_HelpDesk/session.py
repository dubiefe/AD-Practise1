import redis
import uuid

class Session:

    _database = None
    _required_vars: set[str] = ["username", "fullname", "password", "privileges"]

    def __init__(self):
        self._database = redis.Redis(host='localhost', port=6379, db=0)

    def create(self, **kwargs : dict[str, str | dict]):
        """
        Create a new session in redis with all the values in kwargs.
        Check if all the necessary data in in kwargs.
        Check if a session has already been created with the username

        Exceptions:
        -----------
        Throw an exception if a key is missing in kwargs
        Throw an exception if a user using the same username already exists

        Parameters:
        -----------
        kwargs : dict[str, str | dict]
            Dictionary with the user's attributes
        """

        # Check if we have all data
        for var in self._required_vars:
            if var not in kwargs.keys():
                raise Exception(f"The key {var} is missing")
        
        # Check if the username already exists before creating
        if self.read(kwargs["username"]) != {}:
            raise Exception(f"The username {kwargs["username"]} already exists, use another one")

        else :
            # Create user
            id_user = f"user:{kwargs["username"]}"
            self._database.hset(id_user, mapping=kwargs)


    def read(self, username : str):
        """
        Read a session in redis according to the username

        Parameters:
        -----------
        username: str
            String with the username of the session we want to read
        """

        # Search user
        return self._database.hgetall(f"user:{username}")
    
    def update(self, username : str, **kwargs : dict[str, str | dict]):
        """
        Update a session in redis according to the username

        Parameters:
        -----------
        username: str
            String with the username of the session we want to update
        kwargs : dict[str, str | dict]
            Dictionary with the user's attributes to change
        """

        # Update session
        self._database.hset(f"user:{username}", mapping=kwargs)

    def delete(self, username : str):
        """
        Delete a session in redis according to the username

        Parameters:
        -----------
        username: str
            String with the username of the session we want to delete
        """

        # Delete session
        self._database.delete(f"user:{username}")

    def login(self, username : str, password : str):
        """
        Login to a session in redis according to the username
        Create a token with a lifetime of 1 month
        Return the privilege of the user and the token

        Parameters:
        -----------
        username: str
            String with the username of the session we want to login
        password: str
            String with the password of the session we want to login

        Return:
        -------
        -1 if the connetionc data are not good
        Or a dictionary with the privilege and the token of the user
        """

        # Check the password
        truePassword = self._database.hget(f"user:{username}", "password")
        if truePassword == None:
            return -1
        if truePassword.decode() != password:
            return -1

        # Get the privilege
        privileges = self._database.hget(f"user:{username}", "privileges")
        # Create token with expiration date
        token = str(uuid.uuid4())
        id_token = f"token:{token}"
        self._database.setex(id_token, 2592000, username)

        # Return the dictionary
        return {"privileges":privileges.decode(), "token":token}
    

    def login_token(self, token : str):
        """
        Login to a session in redis according to a token
        Return the privilege of the user

        Parameters:
        -----------
        token: str
            String with the token of the session we want to login

        Return:
        -------
        -1 if the token is not related to a session
        Or a the privilege of the user
        """

        # Check the token
        usernameFound = self._database.get(f"token:{token}")
        if usernameFound == None:
            return -1

        # Get the privilege
        privileges = self._database.hget(f"user:{usernameFound.decode()}", "privileges")

        # Return the dictionary
        return privileges.decode()