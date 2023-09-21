#!/usr/bin/python3
"""
Interface with an Iskraemeco MT174 electricity meter via a serial connection
"""

# Imports: standard library
import time
import re
import logging
from typing import Dict, Sequence

# Imports: Pip packages
# Package: pyserial
import serial

class MT174:
    """
    This class handles communications with the meter.

    Example meter output:
    1-0:0.9.1*255(202511)
    1-0:0.9.2*255(0230921)
    1-0:0.2.0*255(1.03)
    0-0:C.1.0*255(62807889)
    0-0:C.1.6*255(FDF5)
    0-0:C.51.1*255(20)
    0-0:C.51.2*255(0230920104504)
    0-0:C.51.2*01(0230920104504)
    0-0:C.51.2*02(0230920094212)
    0-0:C.51.2*03(0230920094158)
    0-0:C.51.3*255(0)
    0-0:C.51.4*255()
    0-0:C.51.4*01()
    0-0:C.51.4*02()
    0-0:C.51.4*03()
    0-0:C.51.5*255(0)
    0-0:C.51.6*255()
    0-0:C.51.6*01()
    0-0:C.51.6*02()
    0-0:C.51.6*03()
    1-0:31.7.0*255(10.40*A)
    1-0:51.7.0*255(6.40*A)
    1-0:71.7.0*255(5.28*A)
    1-0:31.6.0*255(1291.20*A)
    1-0:51.6.0*255(1556.96*A)
    1-0:71.6.0*255(1748.48*A)
    1-0:32.7.0*255(241.0*V)
    1-0:52.7.0*255(242.1*V)
    1-0:72.7.0*255(241.2*V)
    1-0:1.7.0*255(3.920*kW)
    1-0:1.8.0*255(0692930.505*kWh)
    1-0:2.7.0*255(0.000*kW)
    1-0:2.8.0*255(0015803.862*kWh)
    1-0:15.8.0*255(0708734.368*kWh)
    1-0:5.8.0*255(0235104.302*kVArh)
    1-0:6.8.0*255(0062222.609*kVArh)
    1-0:7.8.0*255(0001449.672*kVArh)
    1-0:8.8.0*255(0058260.488*kVArh)
    1-0:9.8.0*255(0906977.544*kVAh)
    0-0:F.F.0*255(0000000)
    """

    ACK = b'\x06'
    STX = b'\x02'
    ETX = b'\x03'
    DELAY = 0.02
    BAUDRATE = 9600

    # e.g. 1-0:1.8.1*255(0001798.478*kWh)
    DATABLOCK_REGEX = re.compile(r"(?:\d-\d:)?(\S+\.\S+\.\d+)(?:\*255)?\((.+)\)")

    def __init__(self, port: int):
        self.__port = port
        logging.info("Created MT174, port = %s", port)

    @staticmethod
    def __delay() -> None:
        """Sleep for a short interval to allow the meter time to process"""
        time.sleep(MT174.DELAY)

    def read(self) -> str:
        """Perform a handshake sequence with the meter in order to read a data block"""
        logging.debug("Opening serial port %s", self.__port)
        mt174 = serial.Serial(port = self.__port, baudrate=MT174.BAUDRATE, bytesize=7, parity='E', stopbits=1, timeout=1.5)
        try:
            # 1 ->
            logging.debug("Writing hello message")
            message = b'/?!\r\n' # IEC 62056-21:2002(E) 6.3.1
            mt174.write(message)
            # 2 <-
            logging.debug("Receiving meter identification")
            MT174.__delay()
            message = mt174.readline() # IEC 62056-21:2002(E) 6.3.2
            if len(message) == 0:
                raise Exception("Empty string instead of identification")
            logging.debug("Got reply: %s", message)
            # Remove possible garbage bytes. Not sure why we're getting these.
            if message[0] != ord('/') and b'/' in message:
                message_start_idx = message.index(b'/')
                discarded = message[0:message_start_idx]
                message = message[message_start_idx:]
                logging.debug("Discarding %s garbage bytes: %s, new message is %s", message_start_idx, discarded, message)
            if message[0] != ord('/'):
                raise Exception("No identification message")
            if len(message) < 7:
                raise Exception("Identification message too short")
            # 3 ->
            logging.debug("Writing acknowledgement message")
            message = MT174.ACK + b'000\r\n' # IEC 62056-21:2002(E) 6.3.3
            mt174.write(message)
            MT174.__delay()
            # 4 <-
            logging.debug("Receiving datablock")
            datablock = b""
            if mt174.read() == MT174.STX:
                curr_byte = mt174.read()
                if len(curr_byte) == 0:
                    raise Exception("Empty string instead of data")
                # bcc is the Block Check Character
                bcc = 0
                while curr_byte != b'!':
                    bcc = bcc ^ ord(curr_byte)
                    datablock = datablock + curr_byte
                    curr_byte = mt174.read()
                    if len(curr_byte) == 0:
                        raise Exception("Empty string instead of data")
                while curr_byte != MT174.ETX:
                    bcc = bcc ^ ord(curr_byte) # ETX itself is part of block check
                    curr_byte = mt174.read()
                    if len(curr_byte) == 0:
                        raise Exception("Empty string instead of data")
                bcc = bcc ^ ord(curr_byte)
                curr_byte = mt174.read() # curr_byte is now the Block Check Character
                # last character is read, could close connection here
                if bcc != ord(curr_byte): # received correctly?
                    datablock = ""
                    raise Exception("Result not OK, try again")
            else:
                logging.warning("No STX found, not handled")
            return datablock.decode('us-ascii')
        finally:
            if mt174.isOpen():
                mt174.close()

    @staticmethod
    def datablock_to_dict(data: str) -> Dict[ str, str ]:
        """
        Utility method to break the datablock string up into a dict of field
        codes -> field values
        """
        data_dict = {}
        for line in data.split():
            match = MT174.DATABLOCK_REGEX.match(line)
            if match:
                data_dict[match.group(1)] = match.group(2)
        return data_dict


class Processor:
    """Base class for processors used by the Scheduler"""
    def __init__(self, name: str):
        self.__name = name

    def get_name(self) -> str:
        """Used for logging"""
        return self.__name

    def process(self, timestamp: float, data: str) -> None:
        """Main entry point for subclasses, called from Scheduler"""

class FileLogger(Processor):
    """A simple logger which writes to a file in /tmp"""
    def __init__(self, filename):
        Processor.__init__(self, "file-logger")
        self.__filename = filename
        logging.info("Created FileLogger, filename = %s", filename)

    def process(self, timestamp: float, data: str) -> None:
        """Main entry point for subclasses, called from Scheduler"""
        filename = "%s-%s.log" % (self.__filename, time.strftime("%Y-%m"))
        with open(filename, "a+", encoding="us-ascii") as output:
            output.write("%d: %s\n" % (timestamp, MT174.datablock_to_dict(data)))
            logging.debug("Written data to %s", filename)

class Scheduler:
    """
    A simple scheduler which runs the MT174 meter reading code at a
    configurable interval, then feeds the resulting data block to a list of
    processors (e.g. a local logger and an MQTT client)
    """
    SLEEP_TIME = 0.1

    def __init__(self, mt174: MT174, processors: Sequence[ Processor ], interval: float = 60):
        self.mt174 = mt174
        self.processors = processors
        self.interval = interval
        logging.info("Created scheduler, interval = %ds", interval)

    def execute(self, timestamp: float) -> None:
        try:
            begin = time.time()
            data = self.mt174.read()
            end = time.time()
            logging.info("Read data in %.3fs", (end - begin))
            logging.debug("Data: %s", data)
            for processor in self.processors:
                try:
                    begin = time.time()
                    processor.process(timestamp, data)
                    end = time.time()
                    logging.info("Processor (%s) in %.3fs", processor.get_name(), (end - begin))
                except KeyboardInterrupt:
                    raise
                except Exception:
                    logging.exception("Error in processor")
        except KeyboardInterrupt:
            raise
        except Exception:
            logging.exception("Error in meter reader")

    def run(self) -> None:
        start = 0
        try:
            while True:
                while (time.time() - start) < self.interval:
                    time.sleep(Scheduler.SLEEP_TIME)
                start = time.time()
                self.execute(start)
        except KeyboardInterrupt:
            return 0
        except Exception:
            logging.exception("Error. Exiting...")
            return 1



if __name__ == "__main__":
    logging.basicConfig(format = "%(levelname)s: %(message)s")
    logging.getLogger().setLevel(logging.DEBUG)

    INTERVAL = 60
    mt174 = MT174("/dev/ttyUSB0")
    processors = [FileLogger("/tmp/data")]
    scheduler = Scheduler(mt174, processors, INTERVAL)
    scheduler.run()
