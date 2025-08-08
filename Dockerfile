# Use Apify's Python base image
FROM apify/actor-python:3.10

# Set working directory
WORKDIR /usr/src/app

# Copy requirements first for better caching
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install --with-deps

# Copy all source code
COPY . ./

# Make main.py executable
RUN chmod +x main.py

# Run the actor
CMD ["python", "main.py"]
