import sys
import os

current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_path, ".."))
sys.path.insert(0, project_root)  

from api import create_app 

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
