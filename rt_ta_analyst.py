#!/usr/bin/env python
# coding:utf-8


from asyncio import run as asyncioRun, Lock as asyncioLock, start_server as asyncioStart_server


# relative import
from sys import path;path.extend("..")
from common.Helpers.helpers import init_logger
from common.Helpers.network_helpers import SafeAsyncSocket
from analyst.Analysts.base.ta_base import TA_RealTime



config = None
logger = None
asyncLock = asyncioLock()
TA_RealTime_List = {}


async def process_data(deserialized_data, sock_requester):
    global asyncLock ; global TA_RealTime_List
    async with asyncLock:
        key = deserialized_data.pop(0)

        if key.lower().startswith("get|"):
            broTick = key.split('|')[1] ; indic = "all"
            if "|" in broTick: broTick, indic = broTick.split('|')
            try:
                current_TA_obj = TA_RealTime_List[broTick]
            except:
                sock_requester.send_data("TA indicators for {0} currently not calculated...".format(broTick))
                pass
            try:
                if indic == "all": 
                    sock_requester.send_data(current_TA_obj._latest)
                else: 
                    sock_requester.send_data(current_TA_obj._latest[indic])
            except Exception as e:
                sock_requester.send_data("error while trying to get indicator '{0}' data for {1} : {2}".format(indic, broTick, e))

        else:
            try:
                current_TA_obj = TA_RealTime_List[key]
            except:
                current_TA_obj = TA_RealTime(name=key, config=config, logger=logger)
                TA_RealTime_List[key] = current_TA_obj
            await current_TA_obj.add_async_data(deserialized_data, sock_requester)

########################################################################
# Async Tcp server
async def handle_TCP_client(reader, writer):
    asyncSock = SafeAsyncSocket(reader=reader, writer=writer)
    data = await asyncSock.receive_data()
    if not data:
        asyncSock.writer.close()
        await asyncSock.writer.wait_closed()
        return
    clientName, host, port = data.split(':') ; port = int(port)      
    await logger.asyncInfo("{0} : '{1}' has established connection without encryption from '{2}' destport '{3}'".format(name, clientName, host, port))
    while True:
        data = await asyncSock.receive_data()
        if not data:
            break
        await process_data(deserialized_data=data, sock_requester=asyncSock)
    asyncSock.writer.close()
    await asyncSock.writer.wait_closed()

async def start_tcp_server():
    global async_tcp_server
    try:
        async_TCP_IP = config.parser["REALTIMETA"]["RT_TA_SERVER"] ; async_TCP_port = int(config.parser["REALTIMETA"]["RT_TA_PORT"])
        async_tcp_server = await asyncioStart_server(handle_TCP_client, async_TCP_IP, async_TCP_port)
        await logger.asyncInfo("{0} async TCP : socket async TCP handler is open : {1}, srcport {2}".format(name, async_TCP_IP, async_TCP_port))
        await async_tcp_server.serve_forever()
    except KeyboardInterrupt:
        pass 
    except Exception as e:
        await logger.asyncError("{0} : error while trying to start TCP server : '{1}'".format(name, e))
        exit(1)


#================================================================
if __name__ == "__main__":
    from sys import argv
    CONFIGFILE = "analyst"

    from os.path import basename as osPathBasename
    name = (osPathBasename(__file__)).split('.')[0]

    if len(argv) == 2: 
        name = argv[1]
    name = name.lower()

    # loading config and logger
    config, logger = init_logger(name=name, config=CONFIGFILE)
    mem_section = name.upper()

    # start TCP Server
    asyncioRun(start_tcp_server())


    

