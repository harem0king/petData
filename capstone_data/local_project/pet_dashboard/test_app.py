from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, Flask is running!'

if __name__ == '__main__':
    print("Attempting to run test_app.py...")
    app.run(debug=True, host='0.0.0.0', port=5000)
