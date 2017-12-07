import asyncio
import sys, json, datetime, signal
import logging
import aiohttp
import async_timeout

PORT_NUM = {
    'Alford':  8888,
    'Ball': 8889,
    'Hamilton': 8890,
    'Holiday': 8891,
    'Welsh': 8892
}

NEIGHBORS = {
    'Alford': ['Hamilton', 'Welsh'],
    'Ball': ['Holiday', 'Welsh'],
    'Hamilton': ['Alford', 'Holiday'],
    'Welsh': ['Alford', 'Ball'],
    'Holiday': ['Ball', 'Hamilton']
}


# Global cache to store the information
global_cache = {}



def logger_create(server_name):
    file_name = server_name + str(datetime.datetime.now())+".log"
    logger = logging.getLogger('asyncio')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(file_name)
    fh.setLevel(logging.INFO)
    #create format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

# helper function for the aiohttp method
async def operation(url, transport, whatsatmsg, logger):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print(resp.status)
            json_msg = await resp.json()
            final_msg = "{0}{1}\n\n".format(whatsatmsg, json_msg)
            transport.write(final_msg.encode())
            logger.info("write to the client: {0}".format(final_msg))
            print("finished writng to the client")
            print("close client socket")
            transport.close()


class ProxyHerdClientProtocol(asyncio.Protocol):
    def __init__(self, message):
        self.message = message

    def connection_made(self, transport):
        transport.write(self.message.encode())
        print('Data sent to peer servers: {!r}'.format(self.message))
        print('Close temporary connection')
        self.transport = transport
        self.transport.close()


class ProxyHerdProtocol(asyncio.Protocol):

    def __init__(self, server_name, loop, cache, logger):
        self.loop = loop
        self.server_name = server_name
        self.lat, self.lng = None, None
        self.client_time = None
        self.cache = cache
        self.logger = logger
        # file_name = server_name + str(datetime.datetime.now())+".log"
        # self.logger = logging.getLogger('asyncio_Server')
        # self.logger.setLevel(logging.INFO)
        # self.fh = logging.FileHandler(file_name)
        # self.fh.setLevel(logging.INFO)
        # #create format
        # self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # self.fh.setFormatter(formatter)
        # self.logger.addHandler(self.handler)
        # self.logger.info('begin blog')


    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, data):
        print("data is received")
        msg = data.decode()
        print("message is {0}\n".format(msg))
        split_data = msg.split()
        if len(split_data) < 2:
            print("? {0}".format(data))
            return
        error_f = False
        if split_data[0] == "IAMAT":
            print("IAMAT received\n")
            if self.checkIAMATinput(split_data[1:]):
                print("sending AT msg")
                self.IAMAT(split_data[1:])
            else: error_f = True
        elif split_data[0] == "WHATSAT":
            print("WHATSAT received")
            if self.checkWHATSATinput(split_data[1:]):
                self.WHATSAT(split_data[1:])
            else: error_f = True
        elif split_data[0] == "AT":
            print("AT received")
            if self.checkATinput(split_data):
                self.AT(data)
            else: error_f = True
        if error_f:
            print("? {0}".format(data))
        # print("close client socket")
        # self.transport.close()



    #cache client update
    def updateCache(self,  client, msg):
        if not self.cache.get(client,0):
            self.cache[client] = msg
            self.cache[client+'lat'] = self.lat
            self.cache[client+'lng'] = self.lng
            self.logger.info("Adding message {0} to cache field {1}".format(msg, client))
        else:
            stored_time = float(self.cache[client].split()[-2])
            current_time = float(msg.split()[-2])
            if store_time < current_time:
                self.cache[client] = msg
                self.cache[client+'lat'] = self.lat
                self.cache[client+'lng'] = self.lng
                self.logger.info("update message")
            else:
                self.logger.info("stored timestamp is smaller")

    def endPropagation(self, client, msg):

        stored_server_time = float(self.cache[client].split()[-1])
        passed_server_time = float(msg[-1])
        return True if stored_server_time == passed_server_time else False

    # computnig floating point time value given POSIX time
    def getTimeDiff(self, givenTime):
        diff = datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(givenTime)
        if diff.total_seconds() >= 0 :
            return '+' + str(diff.total_seconds())
        else: return str(diff.total_seconds())

    # check format of longitude and lattitude: of course to be floats
    def LocationCheck(self, location):
        print("checking GPS location\n")
        lat, lng = [], []
        i = 1
        for item in location[i:]:
            if item in ['-', '+']: break
            i += 1
        lat, lng = location[:i], location[i:]
        try:
            float_lat, float_lng = float(lat), float(lng)
            if float_lat > 90 or float_lat < - 90: return False
            if float_lng > 180 or float_lng < -180: return False
            self.lat = float_lat
            self.lng = float_lng
            return True
        except ValueError:
            self.logger.error("IAMAT_ERR: {0} and {1} not valid coordinates".format(lat,lng))
            return False


    # check time as a float
    def TimeCheck(self, Time):
        print("checking time format")
        try:
            float(Time)
            self.client_time = float(Time)
            return True
        except ValueError:
            self.logger.error("IAMAT_TIME: {0} is not in valid format".format(TIME))
            return False

    # check IAMAT
    # the time difference is computed in response message due to async
    def checkIAMATinput(self, data):
        if len(data) != 3:
            self.logger.error("IAMAT_LENGTH: {0} does not equal 3".format(data))
            return False
        if not self.LocationCheck(data[1]):
            self.logger.error("IAMAT_LOC: {0} not correct location format".format(data[1]))
            return False
        if not self.TimeCheck(data[-1]):
            self.logger.error("IAMAT_TIME: {0} not correct time format".format(data[-1]))
            return False
        return True

    # check WHATSAT
    def checkWHATSATinput(self, data):
        print("checking {0}".format(data))
        if len(data) != 3:
            print("length error")
            self.logger.error("WHATSAT_LENGTH: {0} does not equal 3".format(data))
            return False
        if int(data[1]) > 50:
            print("radius error")
            self.logger.error("WHATSAT_RADIUS: {0} not valid".format(data[1]))
            return False
        if int(data[-1]) > 20:
            print("limit error")
            self.logger.error("WHATSAT_LIMIT: {0} not valid".format(data[-1]))
            return False
        return True


    # check AT
    def checkATinput(self, data):
        if len(data) != 7:
            self.logger.error("AT_LENGTH: {0} size not equal to 7".format(data))
            return False
        try:
            time_diff = float(data[2])
            server_time = float(data[-1])
        except ValueError:
            self.logger.error("TIME in {0} is not valid format".format(data))
            return False
        return self.checkIAMATinput(data[3:-1])

    #IAMAT handle
    def IAMAT(self, data):
        iamat_msg = "AT {0} {1} {2}".format(self.server_name,
                                              self.getTimeDiff(self.client_time),
                                              ' '.join(data))
        self.logger.info("IAMAT: Sending {0} to client".format(iamat_msg))
        self.transport.write(iamat_msg.encode())
        print("close client socket")
        self.transport.close()
        curr_time = (datetime.datetime.utcnow() - datetime.datetime(1970,1,1)).total_seconds()
        save_msg = iamat_msg + ' ' + str(float(curr_time))
        self.updateCache(data[0], save_msg)
        self.propagate(self.cache[data[0]])

    # flooding
    def propagate(self, msg):
        print("propagating to other servers")
        neighbors = NEIGHBORS[self.server_name]
        for neighbor in neighbors:
            self.logger.info("CONNECT: connecting to {0} to propagate {1}".format(neighbor, msg))
            print("message to send: {0}".format(msg))
            coro = self.loop.create_connection(lambda: ProxyHerdClientProtocol(msg), '127.0.0.1', PORT_NUM[neighbor])
            self.loop.create_task(coro)

    def AT(self, data):
        split_data = data.split()
        client = split_data[3]
        self.updateCache(client, data)
        if self.endPropagation(client, split_data):
            self.logger.info("endPropagation")
            return
        else:
            prop_data = self.cache[client]
            self.logger.info("PROPAGATE: propagate {0} forward to neighbors for cache field {1}".format(prop_data, client))
            self.propagate(prop_data)


    def WHATSAT(self, data):
            base_url="https://maps.googleapis.com/maps/api/place/nearbysearch/json?"
            location = "location="+str(self.cache[data[0]+'lat']) + "," + str(self.cache[data[0]+'lng'])
            print(location)
            radius = "&radius="+str(int(data[1])*1000)
            print(radius)
            key = "&key=AIzaSyBrjgm2fzBGdHQ2H07R0nDhodIoeQjMbyc"
            url = base_url + location + radius + key
            self.logger.info("GET: HTTP nearby information")
            print("executing the GET request")
            stored_msg = self.cache[data[0]]
            whatsatmsg = " ".join(stored_msg.split()[:-1])
            task = self.loop.create_task(operation(url, self.transport, whatsatmsg, self.logger))


def main():
    if len(sys.argv) != 2:
        sys.stderr.write("Wrong number of arguments: {0}\n".format(len(sys.argv)))
        exit(1)
    serverName = sys.argv[1]
    if serverName not in PORT_NUM:
        sys.stderr.write("Invalid Server Name" + serverName + ".\n")
        exit(1)
    portNum = PORT_NUM[serverName]

    # run event loops to serve the protocol
    # and at the same time issue HTTP GET as a client
    loop = asyncio.get_event_loop()
    message = "Server running"
    logger = logger_create(serverName)
    coro = loop.create_server(lambda: ProxyHerdProtocol(serverName, loop, global_cache, logger),
                              '127.0.0.1', portNum)
    server = loop.run_until_complete(coro)

    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == "__main__": main()
