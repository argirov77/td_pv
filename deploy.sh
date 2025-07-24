
set -e

echo "Updating system packages..."
sudo apt-get update

echo "Checking for Python3..."
if ! command -v python3 &> /dev/null; then
    echo "Python3 not found. Installing Python3..."
    sudo apt-get install -y python3
fi

echo "Checking for pip3..."
if ! command -v pip3 &> /dev/null; then
    echo "pip3 not found. Installing pip3..."
    sudo apt-get install -y python3-pip
fi

echo "Checking for virtualenv..."
if ! python3 -m venv --help &> /dev/null; then
    echo "python3-venv not found. Installing python3-venv..."
    sudo apt-get install -y python3-venv
fi

# Create a virtual environment if it doesn't exist.
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

echo "Starting the FastAPI application..."
uvicorn app:app --host 0.0.0.0 --port 8000
