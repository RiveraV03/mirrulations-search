bind = "0.0.0.0:80"
workers = 4
timeout = 120
daemon = True
pidfile = "gunicorn.pid"
errorlog = "/tmp/gunicorn-error.log"
capture_output = True
