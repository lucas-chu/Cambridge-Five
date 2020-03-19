import boto3
import time

def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    })

    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)

    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

# Document
for name in [
'KV 4_472',
'KV 4_473',
'KV 4_474',
'KV 4_475',
'KV-4-185_1',
'KV-4-185_2',
'KV-4-186_1',
'KV-4-186_2',
'KV-4-187_1',
'KV-4-187_2',
'KV-4-188_1',
'KV-4-188_2',
'KV-4-189_1',
'KV-4-190_1',
'KV-4-190_2',
'KV-4-191_1',
'KV-4-191_2',
'KV-4-192_1',
'KV-4-192_2',
'KV-4-192_3',
'KV-4-193_1',
'KV-4-193_2',
'KV-4-193_3',
'KV-4-194_1',
'KV-4-194_2',
'KV-4-195_1',
'KV-4-195_2',
'KV-4-195_3',
'KV-4-196_1',
'KV-4-196_2',
'KV-4-196_3',
] :
    s3BucketName = "textract-console-us-east-2-2d72048a-f4fb-4929-bb46-ddf477393c88"
    documentName = name + '.pdf'

    jobId = startJob(s3BucketName, documentName)
    print(name)
    print("Started job with id: {}".format(jobId))
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)

    #print(response)

    # Print detected text
    f = open(name + '.txt','wb')
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                f.write (item["Text"] + " ")
    f.close()
