**Assignment 1 Data Fellowship IYKRA** 

**Create a file upload from internet to Google Cloud Storage**


How To Use:

Docker:
1. Clone this repository
2. Open terminal and go to assignment_1 directories
3. Build Docker Image: `docker build -d uploader`
4. After success build the image, running docker with command: `docker run -ti -v ~/.config:/root/.config uploader'
5. Input "Bucket Name", "URL Source", and "File Name"

Python3:
1. Clone this repository
2. Open terminal and go to assignment_1 directories
3. Create virtual envirotnment: `python3 -m venv`
4. Activate the virtual environment with: `source venv/bin/activate'
5. Install dependencies: `pip install -r requirements.txt`
6. Running the app: `python uploader.py`
7. Input "Bucket Name", "URL Source", and "File Name"
