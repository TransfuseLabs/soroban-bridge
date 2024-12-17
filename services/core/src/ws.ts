import WebSocket from "ws";

const ws = new WebSocket('ws://localhost:3000'); 

// Event listener for WebSocket connection open
ws.on('open', function open() {
  console.log('Connected to WebSocket server');
  
  // Send a test message
  ws.send('Hello, WebSocket server!');
});

// Event listener for incoming messages
ws.on('message', function message(data) {
  console.log('received: %s', data);
});

// Event listener for WebSocket connection close
ws.on('close', function close() {
  console.log('Disconnected from WebSocket server');
});

// Event listener for WebSocket connection errors
ws.on('error', function error(error) {
  console.error('WebSocket error:', error);
});
