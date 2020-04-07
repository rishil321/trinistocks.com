from django.shortcuts import render
import logging
import traceback

#Django Tables
    
#CONSTANTS
ALERTMESSAGE = "Sorry! An error was encountered while processing your request."

# Global variables?
logger = logging.getLogger(__name__)

# Create functions used by the views here

# Create your views here.
def landingpage(request):
    try:
        errors = ""
        logger.info("Landing page was called")
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)    
    # Now add our context data and return a response
    context = {
        'errors':errors,
    }
    return render(request, "central/base.html", context)