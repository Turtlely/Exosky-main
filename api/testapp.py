from flask import Flask, jsonify

# Initialize Flask app
app = Flask(__name__)

# Test route
@app.route('/')
def home():
    return jsonify(message="Flask server is running!")

# Another test route
@app.route('/ping')
def ping():
    return jsonify(message="pong")

if __name__ == '__main__':
    # Run the app
    app.run(host='0.0.0.0', port=8080)
