import http.server
import json
import os

PORT = 5001
LOGS_DIR = 'logs'

class LogsHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        return super().end_headers()

    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            try:
                files = [f for f in os.listdir(LOGS_DIR) if f.endswith('.json')]
            except FileNotFoundError:
                files = []
            self.wfile.write(json.dumps({'files': files}).encode())
        else:
            # Handle specific log files
            file_name = os.path.basename(self.path)
            if not file_name.endswith('.json'):
                self.send_response(400)
                self.end_headers()
                return
                
            file_path = os.path.join(LOGS_DIR, file_name)
            if os.path.exists(file_path):
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()

if __name__ == '__main__':
    server = http.server.HTTPServer(('0.0.0.0', PORT), LogsHandler)
    print(f"Serving logs at http://localhost:{PORT}")
    server.serve_forever()
