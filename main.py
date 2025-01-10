import os
import re
import pdfplumber
import docx
import pandas as pd
#import spacy
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from typing import List

import uvicorn
from pymongo import MongoClient
import warnings
import google.generativeai as genai
import os
import json
import asyncio
from bson import ObjectId


warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")


# MongoDB Client Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["candidate_profiles"]
collection = db["profiles"]
app = FastAPI()
lock = asyncio.Lock()
import time

# new add code
# Helper Function to Check for Duplicate Email or Phone
def check_duplicate_email_or_phone(email: str, phone: str):
    """
    Check if the email or phone already exists in the collection.
    
    :param(parameter) email: The email address to check.
    :param phone: The phone to check.
    :return: The existing resume document if found, else None.
    """
    # Query the collection to find a document where either the email or phone matches
    existing_resume = collection.find_one({"$or": [{"email": email}, {"phone": phone}]})
    return existing_resume  # Return the found document or None if no match

# new add code
# Function to Update Existing Resume
def update_existing_resume(existing_resume, new_data):
    """
    Update the fields of an existing resume, excluding email and phone.
    
    :param existing_resume: The document to update.
    :param new_data: The new data to apply to the resume.
    :return: The updated resume document.
    """
    # Filter out 'email' and 'phone' from new_data to avoid updating these fields
    update_data = {key: value for key, value in new_data.items() if key not in ["email", "phone"]}
    # Update the document in the collection based on its '_id'
    collection.update_one({"_id": existing_resume["_id"]}, {"$set": update_data})
    updated_resume = collection.find_one({"_id": existing_resume["_id"]})  # Fetch updated document
    return updated_resume



# Function to write log
def write_log(message: str):
    with open("log.txt", "a") as log_file:
        log_file.write(f"{message}\n")
    print(message)


# Function to extract text from PDF
def extract_text_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            return " ".join(page.extract_text() for page in pdf.pages)
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return ""


# Function to extract text from DOCX
def extract_text_from_docx(file_path):
    try:
        doc = docx.Document(file_path)
        return " ".join([para.text for para in doc.paragraphs])
    except Exception as e:
        print(f"Error extracting text from DOCX: {e}")
        return ""


def gemini_call(text_, json_format, model, query):
    try:
        print('Gemini model call')
        response = model.generate_content(f"resume:{text_}, json_format:{json_format} query:{query}")
        print('raw data parsing')
        raw_text = response.candidates[0].content.parts[0].text
        match = re.search(r'```json\n(.*?)\n```', raw_text, re.DOTALL)

        if match:
            print('load json file')
            embedded_json_text = match.group(1)
            parsed_dict = json.loads(embedded_json_text)
            return parsed_dict
        else:
            return {"error": "No JSON found in response"}
    
    except Exception as e:
        print(f"Error Gemini: {e}")
        return {"Error": "Gemini failed"}
    

async def convert_into_text(file_path):
    try:
        print("Convert file data into binary text")
        if file_path.endswith(".pdf"):
            text = extract_text_from_pdf(file_path)
        elif file_path.endswith(".docx"):
            text = extract_text_from_docx(file_path)
        else:
            return {"Error": "Unsupported file format"}
        return text

    except Exception as e:
        print(f"Error parsing resume: {e}")
        return {"Error": "Parsing failed"}

def gemini_configure():
    my_api_key ="AIzaSyCjTbS1IhdZTN-4mzsh8_EPVLxpNjHiJyE"
    genai.configure(api_key=my_api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    return model

async def read_files():
    print('Query and JSON file reading')
 
    with open(f'jsonLayout.json', 'r') as blank_resume:
        json_format = json.load(blank_resume)

    with open('query.txt', 'r') as file:
        query = file.read()

    return json_format, query


async def write_resume_binary(file: UploadFile):
    if file is None:
        raise ValueError("The file parameter is None.")

    file_name = file.filename if hasattr(file, "filename") else "uploaded_file"
    file_location = f"temp/{file_name}"
    os.makedirs("temp", exist_ok=True)  # Ensure 'temp' folder exists

    try:
        file_data = await file.read()  # Read the file content
        with open(file_location, "wb") as f:
            f.write(file_data)  # Write content to a temporary file
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing file: {e}")
    finally:
        file.close()  # Ensure the file is closed properly

    return file_location





async def fetch_data(file, results):
    print('Query and JSON file reading')

    # Read the files for JSON and query configuration
    file_read = asyncio.create_task(read_files())

    # Write file to disk
    file_location = await write_resume_binary(file)  # Await directly, no need for `create_task`

    print(f'File created at location: {file_location}')
    text = await convert_into_text(file_location)

    print('Gemini configuration')
    model = gemini_configure()

    json_format, query = await file_read
    print('Calling Gemini')
    result = gemini_call(text, json_format, model, query)

    # Handle result data (e.g., insert/update in DB)
    email = result.get('email')
    phone = result.get('phone')

    

   
   
    # new add code
    # Main Logic
    existing_resume = check_duplicate_email_or_phone(email, phone)
    
    #new add code
    if existing_resume:
        # Update the existing resume if duplicate is found
        print(f"Duplicate found for email: {email} or phone: {phone}. Updating the existing record.")

         # Update the existing resume with the new data
        updated_resume = update_existing_resume(existing_resume, result)

        # Convert the '_id' field to a string for further processing

        updated_resume['_id'] = str(updated_resume['_id'])
        
        # Add the updated resume to the results list

        results.append(updated_resume)
    else:
        # No duplicate found: Insert a new resume into the databas
        print('Insert data in DB')

        # Insert the new resume into the collection
        new_resume = result.copy()
        new_resume["_id"] = str(ObjectId())  # Generate a new ID for the new document

        collection.insert_one(new_resume)

        # Convert the '_id' field of the newly inserted document to a string

        new_resume['_id'] = str(new_resume['_id'])

        # Add the new resume to the results list
        
        results.append(new_resume)
    
    # Clean up the temporary file
    os.remove(file_location)
    return


@app.post("/upload/")
async def upload_resumes(files: List[UploadFile] = File(...)):
    results = []
    print('Hello')
    for file in files:
        await fetch_data(file, results)
  
    return results


def serialize_document(doc):
    if '_id' in doc:
        doc['_id'] = str(doc['_id'])
    return doc


@app.get("/resumes/")
async def get_resumes():
    resumes = list(collection.find())
    if not resumes:
        raise HTTPException(status_code=404, detail="No resumes found.")
    return [serialize_document(resume) for resume in resumes]


if __name__ == '__main__':
    uvicorn.run(app, port=9000, host='0.0.0.0')    