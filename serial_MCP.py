import asyncio
import logging
import traceback
import os
import sys
import json
import time
import random
from collections import deque
from fastmcp import FastMCP
from pydantic import BaseModel
from enum import Enum
from typing import List, Dict, Any, Optional, Union
from contextlib import asynccontextmanager
import string
from datetime import datetime

# Configure logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
# Create logs directory if it doesn't exist
logs_dir = os.path.join(script_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)
# Set log file path
log_file = os.path.join(logs_dir, 'serial_MCP.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('SerialMCP')

# Add request ID to log context
class RequestIdFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = 'N/A'
        return True

logger.addFilter(RequestIdFilter())

# Default environment variable names
ENV_PORT = 'SERIAL_PORT'
ENV_BAUD = 'SERIAL_BAUD_RATE'
ENV_BUFFER = 'SERIAL_BUFFER_LENGTH'

# Default values
DEFAULT_PORT = '/dev/tty.usb*'  # Updated to match both usbserial and usbmodem
DEFAULT_BAUD = 9600
DEFAULT_BUFFER = 100

# Connection Mode enum
class ConnectionMode(str, Enum):
    REAL = "real"           # Real serial connection
    DISCONNECTED = "disconnected" # Not connected

# Server State
class ServerState:
    def __init__(self):
        self.mode = ConnectionMode.DISCONNECTED
        self.port = None
        self.baudrate = DEFAULT_BAUD
        self.buffer_length = DEFAULT_BUFFER
        self.serial_available = False
        self.last_error = None
        self.available_ports = []
        self.last_ports_check = 0
        self.ports_check_interval = 5  # seconds

    def to_dict(self):
        """Convert state to a dictionary for API responses"""
        return {
            "mode": self.mode,
            "port": self.port,
            "baudrate": self.baudrate,
            "buffer_length": self.buffer_length,
            "serial_available": self.serial_available,
            "last_error": self.last_error,
            "available_ports": self.available_ports,
            "last_ports_check": self.last_ports_check
        }

    def set_error(self, error_msg):
        """Set error message and log it"""
        self.last_error = error_msg
        logger.error(error_msg)

    def clear_error(self):
        """Clear error state"""
        self.last_error = None

# Initialize server state
server_state = ServerState()

# Try to import serial module - but don't fail if unavailable
try:
    import serial
    import serial.tools.list_ports
    server_state.serial_available = True
    logger.info("Serial module is available")
except ImportError:
    server_state.serial_available = False
    server_state.set_error("Serial module not available. Can run in simulation mode.")
    logger.warning("Serial module not available. Can run in simulation mode.")

class SerialClientError(Exception):
    """Base exception class for SerialClient errors."""
    pass

class SerialConnectionError(SerialClientError):
    """Exception raised when there are issues with the serial connection."""
    pass

class SerialTimeoutError(SerialClientError):
    """Exception raised when a serial operation times out."""
    pass

class SerialClient:
    def __init__(self):
        self.serial_port = None
        self.buffer = None
        self.buffer_length = 100
        self.read_task = None
        self.lock = asyncio.Lock()  # Add lock for thread safety
        self.is_closing = False
        self.receive_task = None  # Task for continuous message receiving
        
    async def _receive_loop(self):
        """Continuous loop that receives messages from the serial port."""
        if not server_state.serial_available or server_state.mode == ConnectionMode.DISCONNECTED:
            logger.warning("Cannot start receive loop - serial not available or disconnected")
            return
            
        logger.info("Serial receive loop started")
        read_errors = 0
        max_consecutive_errors = 5
        partial_message = ""
        
        while not self.is_closing:
            try:
                if self.serial_port and self.serial_port.is_open:
                    if self.serial_port.in_waiting:
                        try:
                            # Read raw bytes
                            raw_data = self.serial_port.read(self.serial_port.in_waiting)
                            
                            # Decode with error handling
                            try:
                                data = raw_data.decode('utf-8', errors='ignore')
                            except UnicodeDecodeError:
                                logger.warning("Failed to decode message, skipping")
                                continue
                                
                            # Handle partial messages
                            partial_message += data
                            
                            # Split into complete lines
                            lines = partial_message.split('\n')
                            
                            # If the last line doesn't end with \n, it's a partial message
                            if not partial_message.endswith('\n'):
                                partial_message = lines[-1]
                                lines = lines[:-1]
                            else:
                                partial_message = ""
                            
                            # Process complete lines
                            for line in lines:
                                line = line.strip()
                                if line:
                                    # Basic message validation
                                    if len(line) > 1000:  # Skip unreasonably long messages
                                        logger.warning(f"Skipping long message: {line[:50]}...")
                                        continue
                                        
                                    if any(c not in string.printable for c in line):  # Skip messages with non-printable chars
                                        logger.warning(f"Skipping message with non-printable characters: {line[:50]}...")
                                        continue
                                        
                                    # Add timestamp to message
                                    timestamp = datetime.now().isoformat()
                                    message_with_timestamp = {
                                        "timestamp": timestamp,
                                        "message": line
                                    }
                                        
                                    async with self.lock:
                                        self.buffer.append(message_with_timestamp)
                                        logger.debug(f"Received at {timestamp}: {line}")
                                        
                            # Reset error counter on successful read
                            read_errors = 0
                            
                        except serial.SerialException as e:
                            read_errors += 1
                            logger.warning(f"Serial read error ({read_errors}/{max_consecutive_errors}): {str(e)}")
                            if read_errors >= max_consecutive_errors:
                                logger.error("Too many consecutive read errors, stopping receive loop")
                                server_state.set_error(f"Serial connection failed: {str(e)}")
                                server_state.mode = ConnectionMode.DISCONNECTED
                                break
                            await asyncio.sleep(0.1)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                logger.info("Receive loop task cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in receive loop: {str(e)}")
                logger.debug(traceback.format_exc())
                read_errors += 1
                if read_errors >= max_consecutive_errors:
                    logger.error("Too many errors in receive loop, stopping")
                    server_state.set_error(f"Serial receive loop error: {str(e)}")
                    server_state.mode = ConnectionMode.DISCONNECTED
                    break
                await asyncio.sleep(0.5)
                
        logger.info("Serial receive loop ended")

    async def init(self, port, baudrate, buffer_length):
        """
        Initialize the serial connection.
        
        Args:
            port (str): Serial port name or path
            baudrate (int): Communication speed in bauds
            buffer_length (int): Maximum size of the message buffer
            
        Returns:
            dict: Status of the initialization
        """
        global server_state
        server_state.clear_error()
        
        # Handle wildcard in port name
        if '*' in port:
            try:
                import glob
                matching_ports = glob.glob(port)
                if matching_ports:
                    port = matching_ports[0]  # Use the first matching port
                    logger.info(f"Found matching port: {port}")
                else:
                    logger.warning(f"No ports matching pattern: {port}")
            except Exception as e:
                logger.warning(f"Error resolving wildcard port pattern: {str(e)}")

        try:
            # Check if we should run in simulated mode
            if not server_state.serial_available:
                logger.warning("Serial module not available")
                server_state.mode = ConnectionMode.DISCONNECTED
                server_state.port = port
                server_state.baudrate = baudrate
                return {"status": "error", "error": "Serial module not available"}
                
            # Validate input parameters
            if not port:
                raise ValueError("Port name cannot be empty")
            if not isinstance(baudrate, int) or baudrate <= 0:
                raise ValueError(f"Invalid baudrate: {baudrate}. Must be a positive integer.")
            if not isinstance(buffer_length, int) or buffer_length <= 0:
                raise ValueError(f"Invalid buffer length: {buffer_length}. Must be a positive integer.")
                
            # Close existing connection if any
            await self.close()
            
            # Try to open the serial port
            try:
                self.serial_port = serial.Serial(port, baudrate, timeout=0.1)
                server_state.mode = ConnectionMode.REAL
            except serial.SerialException as e:
                logger.warning(f"Failed to open serial port {port}: {str(e)}")
                
                # Check if port exists at all
                ports = await self.list_ports()
                exists = any(p.get("device") == port for p in ports.get("ports", []))
                
                if not exists:
                    logger.warning(f"Port {port} does not exist")
                    server_state.mode = ConnectionMode.DISCONNECTED
                    server_state.port = port
                    server_state.baudrate = baudrate
                    return {"status": "error", "error": f"Port {port} not found"}
                else:
                    raise SerialConnectionError(f"Failed to open serial port {port}: {str(e)}")
            
            # Initialize buffer and start receive task
            self.buffer = deque(maxlen=buffer_length)
            self.buffer_length = buffer_length
            self.is_closing = False
            self.receive_task = asyncio.create_task(self._receive_loop())
            
            # Update server state
            server_state.mode = ConnectionMode.REAL
            server_state.port = port
            server_state.baudrate = baudrate
            server_state.buffer_length = buffer_length
            
            logger.info(f"Serial initialized on {port} at {baudrate} baud")
            return {"status": "initialized", "mode": "real", "port": port, "baudrate": baudrate}
        except SerialClientError as e:
            server_state.set_error(f"Serial client error: {str(e)}")
            server_state.mode = ConnectionMode.DISCONNECTED
            return {"status": "error", "error_type": e.__class__.__name__, "error": str(e)}
        except ValueError as e:
            server_state.set_error(f"Invalid parameter: {str(e)}")
            return {"status": "error", "error_type": "ValueError", "error": str(e)}
        except Exception as e:
            server_state.set_error(f"Unexpected error initializing serial: {str(e)}")
            logger.debug(traceback.format_exc())
            server_state.mode = ConnectionMode.DISCONNECTED
            return {"status": "error", "error_type": "UnexpectedError", "error": str(e)}

    async def read(self, wait=False, timeout=1.0):
        """
        Read messages from the buffer.
        
        Args:
            wait (bool): Whether to wait for messages if buffer is empty
            timeout (float): Time to wait for messages in seconds
            
        Returns:
            dict: Messages or status
        """
        try:
            # Validate input
            if not isinstance(wait, bool):
                raise ValueError("Wait parameter must be a boolean")
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise ValueError(f"Invalid timeout: {timeout}. Must be a positive number.")
            
            # Check if serial is initialized
            if not self.buffer:
                raise SerialConnectionError("Serial not initialized")
            
            # Non-waiting read
            if not wait:
                async with self.lock:
                    if self.buffer:
                        # Read all available messages
                        messages = []
                        while self.buffer:
                            messages.append(self.buffer.popleft())
                        return {"status": "success", "mode": "real", "messages": messages}
                return {"status": "no_messages", "mode": "real", "messages": []}
            
            # Waiting read with timeout
            try:
                end_time = asyncio.get_event_loop().time() + timeout
                while asyncio.get_event_loop().time() < end_time:
                    async with self.lock:
                        if self.buffer:
                            # Read all available messages
                            messages = []
                            while self.buffer:
                                messages.append(self.buffer.popleft())
                            return {"status": "success", "mode": "real", "messages": messages}
                    await asyncio.sleep(0.05)
                return {"status": "timeout", "mode": "real", "messages": []}
            except asyncio.CancelledError:
                raise
            
        except SerialClientError as e:
            logger.error(f"Serial client error: {str(e)}")
            return {"status": "error", "error_type": e.__class__.__name__, "error": str(e)}
        except ValueError as e:
            logger.error(f"Invalid parameter: {str(e)}")
            return {"status": "error", "error_type": "ValueError", "error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error reading messages: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"status": "error", "error_type": "UnexpectedError", "error": str(e)}

    async def close(self):
        """Close the serial connection and clean up resources."""
        try:
            # If already disconnected, do nothing
            if server_state.mode == ConnectionMode.DISCONNECTED:
                return {"status": "already_closed"}
                
            self.is_closing = True
            
            # Cancel receive task if running
            if self.receive_task:
                try:
                    self.receive_task.cancel()
                    await asyncio.wait_for(asyncio.shield(self.receive_task), timeout=2.0)
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for receive task to cancel")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error cancelling receive task: {str(e)}")
                finally:
                    self.receive_task = None
            
            # Close serial port
            if self.serial_port:
                try:
                    if self.serial_port.is_open:
                        self.serial_port.close()
                        logger.info("Serial connection closed")
                except Exception as e:
                    logger.error(f"Error closing serial port: {str(e)}")
                finally:
                    self.serial_port = None
            
            # Update server state
            server_state.mode = ConnectionMode.DISCONNECTED
            server_state.port = None
                    
            return {"status": "closed", "mode": "real"}
        except Exception as e:
            logger.error(f"Unexpected error closing serial: {str(e)}")
            logger.debug(traceback.format_exc())
            server_state.mode = ConnectionMode.DISCONNECTED
            server_state.set_error(f"Error closing connection: {str(e)}")
            return {"status": "error", "error": str(e)}

    async def list_ports(self):
        """List available serial ports on the system."""
        try:
            if not server_state.serial_available:
                # Return empty list if serial is not available
                server_state.available_ports = []
                server_state.last_ports_check = time.time()
                return {"status": "error", "error": "Serial module not available"}
                
            # Check if we should refresh ports or use cached data
            current_time = time.time()
            if (current_time - server_state.last_ports_check) < server_state.ports_check_interval and server_state.available_ports:
                ports = []
                for port_name in server_state.available_ports:
                    ports.append({
                        "device": port_name,
                        "name": port_name,
                        "description": "Cached port entry",
                        "hwid": "CACHED"
                    })
                return {"status": "success", "ports": ports, "cached": True}
                
            # Get fresh port list
            ports = []
            try:
                # First try the standard method
                available_ports = list(serial.tools.list_ports.comports())
                logger.info(f"Found {len(available_ports)} ports using standard method")
                
                # Also try glob pattern for USB devices
                import glob
                usb_ports = glob.glob('/dev/tty.usb*')
                logger.info(f"Found {len(usb_ports)} USB ports using glob pattern")
                
                # Combine both methods
                all_ports = set()
                for port in available_ports:
                    all_ports.add(port.device)
                for port in usb_ports:
                    all_ports.add(port)
                
                # Create detailed port info
                for port_path in all_ports:
                    try:
                        port_info = {
                            "device": port_path,
                            "name": os.path.basename(port_path),
                            "description": f"Serial port {port_path}",
                            "hwid": "UNKNOWN"
                        }
                        
                        # Try to get more info if available
                        for p in available_ports:
                            if p.device == port_path:
                                port_info["description"] = p.description
                                port_info["hwid"] = p.hwid
                                break
                                
                        ports.append(port_info)
                    except Exception as e:
                        logger.warning(f"Error getting info for port {port_path}: {str(e)}")
                
                logger.info(f"Total unique ports found: {len(ports)}")
                
            except Exception as e:
                logger.error(f"Error listing ports: {str(e)}")
                logger.debug(traceback.format_exc())
            
            # Update state cache
            server_state.available_ports = [p["device"] for p in ports]
            server_state.last_ports_check = current_time
            
            if not ports:
                logger.warning("No serial ports found")
                
            return {"status": "success", "ports": ports, "cached": False}
        except Exception as e:
            logger.error(f"Error listing serial ports: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"status": "error", "error": str(e)}

    def get_state(self):
        """Return the current state of the serial client"""
        return {
            "mode": server_state.mode,
            "port": server_state.port,
            "baudrate": server_state.baudrate,
            "buffer_length": server_state.buffer_length,
            "serial_available": server_state.serial_available,
            "last_error": server_state.last_error,
            "available_ports": server_state.available_ports
        }

    async def send(self, message: str, timeout: float = 1.0) -> dict:
        """Send a message through the serial port.
        
        Args:
            message (str): Message to send
            timeout (float): Timeout in seconds for the send operation
            
        Returns:
            dict: Status of the send operation
        """
        try:
            # Validate input
            if not isinstance(message, str):
                raise ValueError("Message must be a string")
            if not message:
                raise ValueError("Message cannot be empty")
            if len(message) > 1000:
                raise ValueError("Message too long (max 1000 characters)")
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise ValueError("Timeout must be a positive number")
                
            # Check if serial is initialized
            if not self.serial_port or not self.serial_port.is_open:
                raise SerialConnectionError("Serial port not initialized or not open")
                
            # Send the message with timeout
            try:
                # Set write timeout
                original_timeout = self.serial_port.write_timeout
                self.serial_port.write_timeout = timeout
                
                # Send the message
                bytes_written = self.serial_port.write(message.encode('utf-8'))
                self.serial_port.flush()  # Ensure data is sent
                
                # Restore original timeout
                self.serial_port.write_timeout = original_timeout
                
                logger.info(f"Sent message: {message} ({bytes_written} bytes)")
                return {
                    "status": "success",
                    "mode": "real",
                    "bytes_written": bytes_written
                }
            except serial.SerialTimeoutException:
                logger.error(f"Send operation timed out after {timeout} seconds")
                return {
                    "status": "error",
                    "error_type": "SerialTimeoutException",
                    "error": f"Send operation timed out after {timeout} seconds"
                }
            except serial.SerialException as e:
                logger.error(f"Serial write error: {str(e)}")
                raise SerialConnectionError(f"Failed to write to serial port: {str(e)}")
                
        except SerialClientError as e:
            logger.error(f"Serial client error: {str(e)}")
            return {
                "status": "error",
                "error_type": e.__class__.__name__,
                "error": str(e)
            }
        except ValueError as e:
            logger.error(f"Invalid parameter: {str(e)}")
            return {
                "status": "error",
                "error_type": "ValueError",
                "error": str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error sending message: {str(e)}")
            logger.debug(traceback.format_exc())
            return {
                "status": "error",
                "error_type": "UnexpectedError",
                "error": str(e)
            }

# Create a single instance of the client
serial_client = SerialClient()

# Create FastMCP server
mcp = FastMCP("serial_MCP")

# Define input models for tools
class InitSerialInput(BaseModel):
    port: str
    baudrate: int = DEFAULT_BAUD
    buffer_length: int = DEFAULT_BUFFER

class SendMessageInput(BaseModel):
    message: str
    wait_for_response: bool = False
    response_timeout: float = 0.5
    send_timeout: float = 1.0  # Timeout for the send operation itself

class ReadMessageInput(BaseModel):
    wait: bool = False
    timeout: float = 1.0

class ConfigureSerialInput(BaseModel):
    port: Optional[str] = None
    baudrate: Optional[int] = None
    list_ports: bool = False

class DelayInput(BaseModel):
    delay: float

@mcp.tool()
async def init_serial(input: InitSerialInput) -> dict:
    """Initialize the serial connection with the specified parameters."""
    try:
        logger.info(f"Initializing serial connection on port {input.port} with baudrate {input.baudrate}")
        
        result = await serial_client.init(input.port, input.baudrate, input.buffer_length)
        
        if result.get("status") == "initialized":
            mode = result.get("mode", "unknown")
            logger.info(f"Serial connection {mode} successfully")
            return {
                "success": True, 
                "status": result.get("status"), 
                "mode": mode,
                "port": input.port,
                "baudrate": input.baudrate,
                "message": f"Serial connection initialized in {mode} mode on {input.port} at {input.baudrate} baud",
                "warning": result.get("warning", None)
            }
        else:
            logger.warning(f"Serial initialization returned {result}")
            return {
                "success": False, 
                "status": result.get("status"), 
                "message": f"Failed to initialize: {result.get('error', 'Unknown error')}",
                "error_details": result
            }
    except Exception as e:
        logger.error(f"Failed to initialize serial connection: {str(e)}")
        return {"success": False, "status": "error", "message": str(e)}

@mcp.tool()
async def send_message(input: SendMessageInput) -> dict:
    """Send a message through the serial connection and optionally wait for a response."""
    # Check if we're in disconnected mode and need to handle gracefully
    if server_state.mode == ConnectionMode.DISCONNECTED:
        logger.warning("Attempted to send message while disconnected")
        
        # Try to list available ports for helpful error message
        ports_result = await serial_client.list_ports()
        available_ports = [p.get("device") for p in ports_result.get("ports", [])]
        
        return {
            "success": False, 
            "status": "not_connected",
            "message": "Serial not connected. Please initialize a connection first using init_serial.",
            "available_ports": available_ports,
            "mode": server_state.mode,
            "write_status": "failed",
            "connection_state": "disconnected"
        }
        
    try:
        logger.debug(f"Sending message: {input.message}")
        result = await serial_client.send(input.message, timeout=input.send_timeout)
        
        if result.get("status") == "success":
            mode = result.get("mode", "unknown")
            bytes_written = result.get("bytes_written", 0)
            logger.info(f"Message sent ({mode} mode): {input.message}")
            
            # Check for response if requested
            response_status = "not_checked"
            response_messages = []
            
            if input.wait_for_response:
                try:
                    # First wait for the specified delay before checking for response
                    await asyncio.sleep(input.response_timeout)
                    
                    # Then check for any messages that arrived during the wait
                    response = await serial_client.read(wait=False)
                    if response.get("status") == "success" and response.get("messages"):
                        response_status = "received"
                        response_messages = response.get("messages", [])
                    else:
                        response_status = "no_response"
                except Exception as e:
                    logger.warning(f"Error checking for response: {str(e)}")
                    response_status = "error"
            
            return {
                "success": True, 
                "status": result.get("status"), 
                "mode": mode,
                "write_status": "success",
                "connection_state": "connected",
                "response_status": response_status,
                "response_messages": response_messages,
                "bytes_written": bytes_written,
                "message": f"Message sent successfully in {mode} mode"
            }
        else:
            logger.warning(f"Send operation returned: {result}")
            
            # Determine the specific failure reason
            error_type = result.get("error_type", "unknown")
            if error_type == "NotConnected":
                connection_state = "disconnected"
            elif error_type == "SerialTimeoutException":
                connection_state = "connected"
                write_status = "timeout"
            else:
                connection_state = "error"
                write_status = "failed"
            
            return {
                "success": False, 
                "status": result.get("status"), 
                "mode": result.get("mode", "unknown"),
                "write_status": write_status,
                "connection_state": connection_state,
                "error_type": error_type,
                "message": f"Failed to send: {result.get('error', 'Unknown error')}",
                "error_details": result
            }
    except Exception as e:
        logger.error(f"Failed to send message: {str(e)}")
        return {
            "success": False, 
            "status": "error", 
            "write_status": "failed",
            "connection_state": "error",
            "error_type": "UnexpectedError",
            "message": str(e)
        }

@mcp.tool()
async def read_message(input: ReadMessageInput) -> dict:
    """Read messages from the serial connection."""
    # Check if we're in disconnected mode and need to handle gracefully
    if server_state.mode == ConnectionMode.DISCONNECTED:
        logger.warning("Attempted to read message while disconnected")
        
        # Try to list available ports for helpful error message
        ports_result = await serial_client.list_ports()
        available_ports = [p.get("device") for p in ports_result.get("ports", [])]
        
        return {
            "success": False, 
            "status": "not_connected",
            "message": "Serial not connected. Please initialize a connection first using init_serial.",
            "available_ports": available_ports,
            "mode": server_state.mode
        }
    
    try:
        logger.debug(f"Reading messages (wait={input.wait}, timeout={input.timeout})")
        result = await serial_client.read(input.wait, input.timeout)
        
        if result.get("status") == "success":
            messages = result.get("messages", [])
            mode = result.get("mode", "unknown")
            
            if messages:
                logger.info(f"Read {len(messages)} messages ({mode} mode)")
                return {
                    "success": True, 
                    "status": "success", 
                    "mode": mode,
                    "messages": messages,
                    "message": f"Read {len(messages)} messages"
                }
            else:
                return {
                    "success": True, 
                    "status": "no_messages", 
                    "mode": mode,
                    "messages": [],
                    "message": f"No messages available ({mode} mode)"
                }
        else:
            logger.warning(f"Read operation returned: {result}")
            return {
                "success": False, 
                "status": result.get("status"), 
                "message": f"Failed to read: {result.get('error', 'Unknown error')}",
                "error_details": result
            }
    except Exception as e:
        logger.error(f"Failed to read messages: {str(e)}")
        return {"success": False, "status": "error", "message": str(e)}

@mcp.tool()
async def close_serial() -> dict:
    """Close the serial connection."""
    try:
        logger.info("Closing serial connection")
        result = await serial_client.close()
        
        if result.get("status") in ["closed", "already_closed", "disconnected"]:
            logger.info("Serial connection closed successfully")
            return {
                "success": True, 
                "status": result.get("status"), 
                "mode": result.get("mode", "unknown"),
                "message": "Serial connection closed successfully"
            }
        else:
            logger.warning(f"Close operation returned: {result}")
            return {
                "success": False, 
                "status": result.get("status"), 
                "message": f"Error closing connection: {result.get('error', 'Unknown error')}"
            }
    except Exception as e:
        logger.error(f"Failed to close serial connection: {str(e)}")
        return {"success": False, "status": "error", "message": str(e)}

@mcp.tool()
async def list_serial_ports() -> dict:
    """List available serial ports."""
    try:
        # Get actual ports
        result = await serial_client.list_ports()
        
        if result.get("status") in ["success", "simulated"]:
            port_details = result.get("ports", [])
            port_list = [port.get("device") for port in port_details]
            
            message = f"Found {len(port_list)} serial ports"
            if result.get("status") == "simulated":
                message += " (simulated)"
            if result.get("cached", False):
                message += " (cached)"
                
            return {
                "success": True,
                "status": result.get("status"),
                "ports": port_list,
                "details": port_details,
                "message": message,
                "mode": server_state.mode
            }
        else:
            return {
                "success": False,
                "status": result.get("status"),
                "message": f"Error listing ports: {result.get('error', 'Unknown error')}",
                "mode": server_state.mode
            }
    except Exception as e:
        logger.error(f"Failed to list serial ports: {str(e)}")
        return {"success": False, "status": "error", "message": str(e), "ports": []}

@mcp.tool()
async def configure_serial(input: ConfigureSerialInput) -> dict:
    """Configure the serial port after server is already running.
    Use this to change ports or baudrate without restarting the server.
    If list_ports is True, it will just list available ports.
    """
    try:
        if input.list_ports:
            # Just list the ports
            logger.info("Listing available serial ports")
            return await list_serial_ports()
                
        # Configure port and baudrate
        if not input.port and input.baudrate is None:
            # Return current configuration if nothing specified
            current_state = serial_client.get_state()
            return {
                "success": True,
                "message": "Current serial configuration",
                "mode": current_state["mode"],
                "port": current_state["port"],
                "baudrate": current_state["baudrate"],
                "buffer_length": current_state["buffer_length"],
                "serial_available": current_state["serial_available"]
            }
            
        # Get current configuration
        current_port = server_state.port or os.environ.get(ENV_PORT, DEFAULT_PORT)
        current_baudrate = server_state.baudrate or int(os.environ.get(ENV_BAUD, DEFAULT_BAUD))
        current_buffer = server_state.buffer_length or int(os.environ.get(ENV_BUFFER, DEFAULT_BUFFER))
        
        # Update with new values if provided
        config_port = input.port if input.port is not None else current_port
        config_baudrate = input.baudrate if input.baudrate is not None else current_baudrate
        
        # Initialize with updated configuration
        logger.info(f"Reconfiguring serial: {config_port} at {config_baudrate} baud")
        
        # Use the init_serial tool for consistent behavior
        init_result = await init_serial(InitSerialInput(
            port=config_port,
            baudrate=config_baudrate,
            buffer_length=current_buffer
        ))
        
        if init_result.get("success"):
            return {
                "success": True,
                "status": "reconfigured",
                "mode": init_result.get("mode"),
                "port": config_port,
                "baudrate": config_baudrate,
                "message": f"Serial reconfigured to {config_port} at {config_baudrate} baud",
                "warning": init_result.get("warning")
            }
        else:
            return {
                "success": False,
                "status": "error",
                "message": f"Failed to reconfigure: {init_result.get('message')}",
                "error_details": init_result.get("error_details", {})
            }
            
    except Exception as e:
        logger.error(f"Error in configure_serial: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "message": f"Configuration error: {str(e)}"
        }

@mcp.tool()
async def get_serial_status() -> dict:
    """Get the current status of the serial connection."""
    try:
        state = serial_client.get_state()
        
        # Add extra information about port availability
        port_info = await serial_client.list_ports()
        available_ports = [p.get("device") for p in port_info.get("ports", [])]
        
        status_message = f"Serial status: {state['mode']}"
        if state["port"]:
            status_message += f" on {state['port']} at {state['baudrate']} baud"
        
        if state["last_error"]:
            status_message += f" (Error: {state['last_error']})"
            
        return {
            "success": True,
            "mode": state["mode"],
            "port": state["port"],
            "baudrate": state["baudrate"],
            "serial_available": state["serial_available"],
            "buffer_length": state["buffer_length"],
            "last_error": state["last_error"],
            "available_ports": available_ports,
            "message": status_message
        }
    except Exception as e:
        logger.error(f"Error getting serial status: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "message": f"Error getting status: {str(e)}"
        }

@mcp.tool()
async def delay(input: DelayInput) -> dict:
    """Wait for a specified number of seconds.
    
    Args:
        delay (float): Number of seconds to wait
        
    Returns:
        dict: Status and timing information
    """
    try:
        # Validate input
        if not isinstance(input.delay, (int, float)) or input.delay < 0:
            raise ValueError("Delay must be a positive number")
            
        start_time = time.time()
        await asyncio.sleep(input.delay)
        end_time = time.time()
        actual_delay = end_time - start_time
        
        return {
            "success": True,
            "status": "completed",
            "requested_delay": input.delay,
            "actual_delay": actual_delay,
            "message": f"Waited for {actual_delay:.3f} seconds"
        }
    except ValueError as e:
        return {
            "success": False,
            "status": "error",
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"Error in delay tool: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "error": str(e)
        }

@mcp.tool()
async def help() -> dict:
    """Returns detailed instructions on how to use the Serial MCP server.
    
    Returns:
        dict: Structured help information including tool descriptions, parameters, and examples
    """
    help_info = {
        "description": "Serial MCP is a robust serial communication server that provides a reliable interface for serial port communication.",
        "tools": {
            "delay": {
                "description": "Wait for a specified number of seconds",
                "parameters": {
                    "delay": "Number of seconds to wait (must be positive)"
                },
                "example": {
                    "input": {"delay": 2.5},
                    "output": {
                        "success": True,
                        "status": "completed",
                        "requested_delay": 2.5,
                        "actual_delay": 2.501,
                        "message": "Waited for 2.501 seconds"
                    }
                }
            },
            "init_serial": {
                "description": "Initialize a serial connection with specified parameters",
                "parameters": {
                    "port": "Serial port device path (e.g., '/dev/tty.usbmodem1101')",
                    "baudrate": "Communication speed in bauds (default: 9600)",
                    "buffer_length": "Maximum number of messages to buffer (default: 100)"
                },
                "example": {
                    "input": {
                        "port": "/dev/tty.usbmodem1101",
                        "baudrate": 9600,
                        "buffer_length": 100
                    }
                }
            },
            "send_message": {
                "description": "Send a message through the serial connection and optionally wait for a response",
                "parameters": {
                    "message": "Message to send",
                    "wait_for_response": "Whether to wait for a response (default: False)",
                    "response_timeout": "Time in seconds to wait before checking for a response (default: 0.5). This is a pure delay after the message is sent.",
                    "send_timeout": "Timeout in seconds for the send operation itself (default: 1.0)"
                },
                "example": {
                    "input": {
                        "message": "Hello",
                        "wait_for_response": True,
                        "response_timeout": 0.5,
                        "send_timeout": 1.0
                    },
                    "sequence": [
                        "1. Send message with send_timeout (1.0s)",
                        "2. If wait_for_response is True, wait for response_timeout (0.5s)",
                        "3. Check buffer for any messages that arrived during the wait",
                        "4. Return results"
                    ]
                }
            },
            "read_message": {
                "description": "Read messages from the buffer",
                "parameters": {
                    "wait": "Whether to wait for messages if buffer is empty (default: False)",
                    "timeout": "Time to wait for messages in seconds (default: 1.0)"
                },
                "example": {
                    "input": {
                        "wait": False,
                        "timeout": 1.0
                    }
                }
            },
            "list_serial_ports": {
                "description": "List all available serial ports on the system",
                "parameters": "None",
                "example": "await list_serial_ports()"
            },
            "get_serial_status": {
                "description": "Get the current status of the serial connection",
                "parameters": "None",
                "example": "await get_serial_status()"
            },
            "configure_serial": {
                "description": "Configure the serial connection after initialization",
                "parameters": {
                    "port": "New port to use (optional)",
                    "baudrate": "New baudrate to use (optional)",
                    "list_ports": "Whether to just list ports (default: False)"
                },
                "example": {
                    "input": {
                        "port": "/dev/tty.usbmodem1101",
                        "baudrate": 9600,
                        "list_ports": False
                    }
                }
            },
            "close_serial": {
                "description": "Close the current serial connection",
                "parameters": "None",
                "example": "await close_serial()"
            }
        },
        "common_responses": {
            "success": {
                "description": "Operation completed successfully",
                "fields": ["success", "status", "message"]
            },
            "error": {
                "description": "Operation failed",
                "fields": ["success", "status", "error_type", "message", "error_details"]
            },
            "not_connected": {
                "description": "Serial port is not connected",
                "fields": ["success", "status", "message", "available_ports"]
            }
        },
        "connection_states": {
            "connected": "Serial port is connected and available",
            "disconnected": "No active serial connection",
            "error": "Connection in error state"
        },
        "response_statuses": {
            "not_checked": "Response check was not requested",
            "received": "Response was received",
            "no_response": "No response received within timeout",
            "error": "Error occurred while checking for response"
        },
        "usage_notes": [
            "Always initialize the connection using init_serial before sending messages",
            "Check connection status using get_serial_status if unsure about connection state",
            "Use list_serial_ports to discover available serial ports",
            "Messages in the buffer include timestamps and are limited to 1000 characters",
            "The buffer can hold up to 100 messages by default",
            "Non-printable characters in messages are skipped"
        ]
    }
    
    return {
        "success": True,
        "status": "success",
        "help": help_info
    }

if __name__ == "__main__":
    try:
        # Log whether serial is available at startup
        if server_state.serial_available:
            print(f"Serial module is available", file=sys.stderr)
            print(f"Server will start with no active connection - use init_serial to connect", file=sys.stderr)
            
            # List available ports at startup for convenience
            try:
                ports = list(serial.tools.list_ports.comports())
                if ports:
                    print(f"Available serial ports: {[port.device for port in ports]}", file=sys.stderr)
                else:
                    print("No serial ports found. You can connect later when a port becomes available.", file=sys.stderr)
            except Exception as e:
                print(f"Error listing serial ports: {str(e)}", file=sys.stderr)
        else:
            print("Serial module not available - running in simulation mode", file=sys.stderr)
            print("You can still use all tools, but they will simulate serial behavior", file=sys.stderr)
        
        # Start the server
        mcp.run()
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1) 