import requests

# Define the URL of the FastAPI endpoint
url = "http://127.0.0.1:8000/upload-rcal/"

# Define the path to the file you want to upload
file_path = "data/rcal.csv"

# Open the file in binary mode
with open(file_path, "rb") as file:
    # Create a dictionary with the file
    files = {"file": (file_path, file, "text/csv")}

    # Send a POST request to the FastAPI endpoint
    response = requests.post(url, files=files)

# Print the response from the server
print(response.status_code)
print(response.json())
