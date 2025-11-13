"""
File to test the fucntionment of BLPOP of the getHelpRequest
Launch it after the main.py
"""

from help_desk import HelpDesk

## Create an instance to use helpdesk
newHelpDesk = HelpDesk()

## Post a request to deblock
newHelpDesk.postHelpRequest("dubiefe", "Hello world", 4)