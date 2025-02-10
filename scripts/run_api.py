import sys
import os
from api import create_app 

current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_path, ".."))
sys.path.insert(0, project_root)
app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
