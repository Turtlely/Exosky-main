from api.app import app

# Run this command: ```gunicorn -b 0.0.0.0:8080 api.wsgi:app```

if __name__ == '__main__':
	app.run(debug=True,threaded=True)