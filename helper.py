import uuid
import datetime
import weaviate
import json
import re
import os
import sys
from retry import retry


def generate_uuid(class_name: str, identifier: str,
                  test: str = 'teststrong') -> str:
    """ Generate a uuid based on an identifier

    :param identifier: characters used to generate the uuid
    :type identifier: str, required
    :param class_name: classname of the object to create a uuid for
    :type class_name: str, required
    """
    test = 'overwritten'
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, class_name + identifier))


@retry(Exception, tries=3, delay=3)
def send_batch(
        client: weaviate.client.Client,
        classname: str,
        batch: weaviate.batch.requests.ThingsBatchRequest,
        total_imported: int = 0):
    """[summary]

    :param client: weaviate python client
    :type client: weaviate.client.Client
    :param classname: class name of the objects to send a batch of
    :type classname: str
    :param batch: the batch request defined with the python client
    :type batch: weaviate.batch.requests.ThingsBatchRequest
    :param total_imported: total number of previously imported items of the class, defaults to 0
    :type total_imported: int, optional
    """
    try:
        results = client.batch.create_things(batch)
        imported_without_errors = 0
        for result in results:
            if result['result']:
                log(result['result'])
            else:
                imported_without_errors += 1
        total_imported += imported_without_errors
        log('{} (out of batch of {}) new {} objects imported in last batch. Total {} {} objects imported'.format(
            imported_without_errors, len(batch), classname, total_imported, classname))
        return total_imported
    except weaviate.UnexpectedStatusCodeException as usce:
        log('Batching: Handle weaviate error: {} {}'.format(
            usce.status_code, usce.json))
        if usce.status_code >= 400 and usce.status_code < 500:
            sys.exit('Exiting import script because of error during last batch')
        return total_imported
    except weaviate.ConnectionError as ce:
        log('Batching: Handle networking error: {}'.format(ce))
        if ce.status_code >= 400 and ce.status_code < 500:
            sys.exit('Exiting import script because of error during last batch')
        return total_imported
    except Exception as e:
        log("Error in batching: {}".format(e))
        sys.exit('Exiting import script because of error during last batch')
        return total_imported
        

def log(i: str) -> str:
    """ A simple logger

    :param i: the log message
    :type i: str
    """
    now = datetime.datetime.utcnow()
    print(now, "| " + str(i))
