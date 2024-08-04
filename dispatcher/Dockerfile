# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire src directory into the container at /app/src
COPY src /app/src

# Run the dispatcher.py script
CMD ["python3", "src/dispatcher.py"]
