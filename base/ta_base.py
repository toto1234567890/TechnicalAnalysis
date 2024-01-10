#!/usr/bin/env python
# coding:utf-8

from numpy import empty as npEmpty, roll as npRoll
from talib import stream, get_function_groups as talibGet_function_groups
from talib.abstract import Function as TA_libFunction


class TA_RealTime:
    """
        optional init named parameters : 
                -  config : config
                -  logger : logger
                -  return_result : True / False, return result after calculation
                -  buffer_size : size of the buffer
                -  indicators : list of indicators [MACD,ICHIKOMU,...]
                -  QPush : asyncioQueue to share results in another parent class
    """ 
    Name = "TA-RealTime"
    QPush = None
    config = None
    logger = None
    def __init__(self, name, **kwargs):
        self.Name = "{1}-{0}".format(self.Name, name)
        self.ticker = name
        self._RT_indicators = {} ; self._indicator_params = {} ; self._infos = {} ; self.df_like = {}

        if "config" in kwargs: self.config = kwargs["config"] 
        if "logger" in kwargs: self.logger = kwargs["logger"] 
        
        self.return_result = kwargs["return_result"] if "return_result" in kwargs else True
        self.ndArray_size = kwargs["buffer_size"] if "buffer_size" in kwargs else 200

        if "QPush" in kwargs: self.QPush = kwargs["QPush"] 
        self._load_indicators(kwargs["indicators"]) if "indicators" in kwargs else self._load_indicators()

        self.ndArray = npEmpty(shape=(6, self.ndArray_size), dtype=float) ; self.ndArray_index = -1 ; self.num_records = 0
        self._latest = {}  

    def _C_Buffer(self, data):
        # circular buffer
        if self.num_records > self.ndArray_size-1:
            self.ndArray[0, self.ndArray_index] = data[0]
            self.ndArray[1, self.ndArray_index] = data[1]
            self.ndArray[2, self.ndArray_index] = data[2]
            self.ndArray[3, self.ndArray_index] = data[3]
            self.ndArray[4, self.ndArray_index] = data[4]
            self.ndArray[5, self.ndArray_index] = data[5]
            self.ndArray_index = (self.ndArray_index + 1) % self.ndArray_size
        else:
            self.ndArray[0, self.ndArray_index] = data[0]
            self.ndArray[1, self.ndArray_index] = data[1]
            self.ndArray[2, self.ndArray_index] = data[2]
            self.ndArray[3, self.ndArray_index] = data[3]
            self.ndArray[4, self.ndArray_index] = data[4]
            self.ndArray[5, self.ndArray_index] = data[5]
            self.ndArray_index = (self.ndArray_index + 1)
            self.num_records += 1

    def _RT_calculate_indicators(self):
        # start calculation if buffer full
        if self.num_records > self.ndArray_size-1:
            self.df_like['price'] = npRoll(self.ndArray[0], -self.ndArray_index)
            self.df_like['open'] = npRoll(self.ndArray[1], -self.ndArray_index)
            self.df_like['high'] = npRoll(self.ndArray[2], -self.ndArray_index)
            self.df_like['low'] = npRoll(self.ndArray[3], -self.ndArray_index)
            self.df_like['close'] = npRoll(self.ndArray[4], -self.ndArray_index)
            self.df_like['volume'] = npRoll(self.ndArray[5], -self.ndArray_index)

            temp = {}
            for name, func in self._RT_indicators.items():
                params = self._indicator_params[name].get("prices") or self._indicator_params[name].get("price") 
                if type(params) == str : params = [params]
                if name != "stream_MAVP" and name != "stream_BETA" and name != "stream_CORREL" and name != "stream_OBV":
                    if len(params) > 0:
                        value = func(*[self.df_like[param] for param in params])
                    else : 
                        value = func()
                    self._latest[name] = value
                    temp[name] = value
            if not self.QPush is None:
                if len(temp) > 0:
                    self.QPush.put_nowait({self.ticker:temp})
        else:
            self.df_like['price'] = self.ndArray[0, 0:self.ndArray_index]
            self.df_like['open'] = self.ndArray[1, 0:self.ndArray_index]
            self.df_like['high'] = self.ndArray[2, 0:self.ndArray_index]
            self.df_like['low'] = self.ndArray[3, 0:self.ndArray_index]
            self.df_like['close'] = self.ndArray[4, 0:self.ndArray_index]
            self.df_like['volume'] = self.ndArray[5, 0:self.ndArray_index]

    def _load_indicators(self, indicators=None):
        if not indicators is None:
            for key, func_list in talibGet_function_groups().items():
                if not key.startswith("Math"):
                    for name in func_list:
                        if name in indicators:
                            try:
                                stream_func = getattr(stream, name)
                                self._RT_indicators[stream_func.__name__] = stream_func
                                self._indicator_params[stream_func.__name__] = TA_libFunction(name).input_names
                            except Exception as e:
                                if not self.logger is None:
                                    self.logger.error("{0} : error while trying to load real time indicator '{1}' : {2}".format(self.Name, name, e))
                                continue
            
        else:
            for key, func_list in talibGet_function_groups().items():
                if not key.startswith("Math"):
                    for name in func_list:
                        try:
                            stream_func = getattr(stream, name)
                            self._RT_indicators[stream_func.__name__] = stream_func
                            self._indicator_params[stream_func.__name__] = TA_libFunction(name).input_names
                        except Exception as e:
                            if not self.logger is None:
                                self.logger.error("{0} : error while trying to load real time indicator '{1}' : {2}".format(self.Name, name, e))
                            continue
    
    # can or should be overloaded in child class
    async def add_async_data(self, data, sock_requester):
        self._C_Buffer(data)
        self._RT_calculate_indicators()
        if self.return_result:
            return await sock_requester.send_data(self._latest)
        
    # can or should be overloaded in child class
    def add_data(self, data):
        self._C_Buffer(data)
        self._RT_calculate_indicators()
        if self.return_result: 
            return self._latest
