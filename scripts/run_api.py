import sys
import os

# Add the project root directory to sys.path
current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_path, ".."))
sys.path.insert(0, project_root)  # Add project root at the start of sys.path

from api import create_app 

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
