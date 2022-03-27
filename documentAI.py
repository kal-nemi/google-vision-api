import os
import io
import json
import re
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
from gtts import gTTS



def get_text(doc_element: dict, document: dict):
    """
    Document AI identifies form fields by their offsets
    in document text. This function converts offsets
    to text snippets.
    """

    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
        # print(response)  
    return response
    

def quickstart(project_id: str, location: str, processor_id: str):
    # project_id: str, location: str, processor_id: str, file_path: str

    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = {}
    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    storage_client = storage.Client()
    
    
    GCS_INPUT_BUCKET = 'test-fastapi'
    
    
    
    blobs = storage_client.list_blobs(GCS_INPUT_BUCKET)
    input_configs = []
    print("Input Files:")
    for blob in blobs:
        if ".pdf" in blob.name:
            file_path = "gs://{bucket}/{name}".format(bucket = GCS_INPUT_BUCKET, name = blob.name)
            print(file_path)
            input_config = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source=file_path, mime_type="application/pdf")
            input_configs.append(input_config)
    
    GCS_OUTPUT_URI  = "test-fastapi"
    
    destination_uri = f"gs://{GCS_OUTPUT_URI}/"

    # # Where to write results
    output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(
        gcs_destination=destination_uri
    )
    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    request = documentai.types.document_processor_service.BatchProcessRequest(
        name=name,
        input_configs=input_configs,
        output_config=output_config,
    )

    operation = client.batch_process_documents(request)
    
    operation.result(timeout=420)

    match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
    output_bucket = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(output_bucket)
    blob_list = list(bucket.list_blobs(prefix=prefix))
    for i, blob in enumerate(blob_list):
        # If JSON file, download the contents of this blob as a bytes object.
     if ".json" in blob.name:
            blob_as_bytes = blob.download_as_string()
            print("downloaded")

            document = documentai.types.Document.from_json(blob_as_bytes)
            print(f"Fetched file {i + 1}")

            # For a full list of Document object attributes, please reference this page:
            # https://cloud.google.com/document-ai/docs/reference/rpc/google.cloud.documentai.v1beta3#document
            #  Read the text recognition output from the processor
            document_pages = document.pages
            keys = []
            keysConf = []
            values = []
            valuesConf = []
            
            # Grab each key/value pair and their corresponding confidence scores.
            for page in document_pages:
                for form_field in page.form_fields:
                    fieldName=get_text(form_field.field_name,document)
                    keys.append(fieldName.replace(':', ''))
                    nameConfidence = round(form_field.field_name.confidence,4)
                    keysConf.append(nameConfidence)
                    fieldValue = get_text(form_field.field_value,document)
                    values.append(fieldValue.replace(':', ''))
                    valueConfidence = round(form_field.field_value.confidence,4)
                    valuesConf.append(valueConfidence)
            
        
        
    else:
        print(f"Skipping non-supported file type {blob.name}")


    # Read the file into memory
    # with open(file_path, "rb") as image:
    #     image_content = image.read()

    # document = {"content": file_path, "mime_type": "application/pdf"}
    # 

    # # Configure the process request
    # request = {"name": name, "raw_document": document}

    # result = client.process_document(request=request)
   
    
    # document = result.document
    
    

    # document_pages = document.pages
    

    # For a full list of Document object attributes, please reference this page: https://googleapis.dev/python/documentai/latest/_modules/google/cloud/documentai_v1beta3/types/document.html#Document

    # # Read the text recognition output from the processor
    # print("The document contains the following paragraphs:")
    
    # for page in document_pages:
    #     paragraphs = page.paragraphs
        
    #     for paragraph in paragraphs:
            
    #         paragraph_text = get_text(paragraph.layout, document)
            
    #         print(f"Paragraph text: {paragraph_text}")
    #         # with io.open("speech.txt",'a+',encoding='utf-8') as f:

    #         #     f.write(paragraph_text)
    
    
    
            
# f = open("speech.txt",'w+')
# f.truncate()

project_id= 'amiable-venture-317208'
location = 'us'                      # Format is 'us' or 'eu'
processor_id = '6364174224ab24e1'    # Create processor in Cloud Console
# file_path = 'G:\metalearning.pdf'    # give a file of maximum 10 pages.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "restate_keys.json"

doc = quickstart('amiable-venture-317208','us' ,'6364174224ab24e1' ) 
print(doc+type(doc))
# 'amiable-venture-317208','us' ,'6364174224ab24e1','G:\paper.pdf'

# string = io.open('speech.txt','r',encoding='utf-8').read()
# newstring = re.sub('[^a-zA-Z0-9\n\.]', ' ', string)
# open('speech.txt', 'w').write(newstring)

# with open("speech.txt") as file:
#     line = file.read()

# ob = gTTS(text=line, lang='en', )

# ob.save("converted_audio.mp3")

