from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Set, Union, Tuple
from enum import Enum
from queue import PriorityQueue, Empty
from collections import deque

########################################################################
# Node:     a node can accept data, and/or generate output
# Sink:     a node that does not generate output
# Source:   a node that does not accept input
# Port:     a port of a node
#
# User do not derive class from Port
########################################################################

DEFAULT_PORT_NAME = "default"

class PortType(Enum):
    INPUT = 1
    OUTPUT = 2

class _PORT_DICT:
    def __init__(self, v):
        self.v = v
        for port_name, port in v.items():
            setattr(self, port_name, port)
    
    def __getitem__(self, key):
        return self.v[key]
    
    
    
class Pipe(ABC):
    # Each input port has a unique name among input ports
    # Each output port has a unique name among output ports
    # Sink does not have output ports
    # Source does not have input ports
    def __init__(self, pipeline:"Pipeline", *, input_names:List[str]=[DEFAULT_PORT_NAME], output_names:List[str]=[DEFAULT_PORT_NAME]):
        self._input_port_dict:Dict[str, "Port"] = {}
        self._output_port_dict:Dict[str, "Port"] = {}
        self.pipeline = pipeline
        
        _input_names = []
        _output_names = []

        for name in input_names:
            if name in self._input_port_dict:
                raise Exception(f"Duplicate input port name: {name}")
            self._input_port_dict[name] = Port(PortType.INPUT, name, self)
            _input_names.append(name)
        self._input_port_names = tuple(_input_names)
        
        for name in output_names:
            if name in self._output_port_dict:
                raise Exception(f"Duplicate output port name: {name}")
            self._output_port_dict[name] = Port(PortType.OUTPUT, name, self)
            _output_names.append(name)
        self._output_port_names = tuple(_output_names)
        
        self.INPUT  = _PORT_DICT(self._input_port_dict)
        self.OUTPUT = _PORT_DICT(self._output_port_dict)
        self._emit_count = 0

    @property
    def input_port_names(self):
        return self._input_port_names

    @property
    def output_port_names(self):
        return self._output_port_names
    
    @property
    def input(self):
        return self.get_input_port(DEFAULT_PORT_NAME)
    
    @property
    def output(self):
        return self.get_output_port(DEFAULT_PORT_NAME)
   
    def get_input_port(self, name:str=DEFAULT_PORT_NAME) -> "Port":
        return self._input_port_dict[name]

    def get_output_port(self, name:str=DEFAULT_PORT_NAME) -> "Port":
        return self._output_port_dict[name]

    @abstractmethod
    def on_data_arrival(self, name:str, data:Any):
        # data arrives form input port with port name
        pass

    def connect(self, *dest_ports:Union["Pipe", "Port"]) -> "Pipe":
        self.get_output_port().connect(*dest_ports)
        return self

    def emit(self, data:Any, name:str=DEFAULT_PORT_NAME):
        self.get_output_port(name).emit(data)
        self._emit_count += 1
        return self

class Sink(Pipe):
    def __init__(self, pipeline:"Pipeline", *, input_names:List[str]=[DEFAULT_PORT_NAME]):
        super().__init__(pipeline, input_names=input_names, output_names=[])

    def create_output_port(self, name:str=DEFAULT_PORT_NAME):
        raise Exception("Cannot create output port for sink")

    def get_output_port(self, name:str=DEFAULT_PORT_NAME) -> "Port":
        raise Exception("Sink do not have output port")

class Source(Pipe):
    def __init__(self, pipeline:"Pipeline", *, output_names:List[str]=[DEFAULT_PORT_NAME]):
        super().__init__(pipeline, input_names=[], output_names=output_names)

    def create_input_port(self, name:str=DEFAULT_PORT_NAME):
        raise Exception("Cannot create input port for source")

    def get_input_port(self, name:str=DEFAULT_PORT_NAME) -> "Port":
        raise Exception("Source do not have input port")

    def on_data_arrival(self, name:str, data:Any):
        raise Exception("Source cannot process data")

    def pump_wrapper(self) -> bool:
        self._emit_count = 0
        self.pump()
        return self._emit_count > 0

    @abstractmethod
    def pump(self):
        """
        Let the source to collect data and emit data
        """
        pass


class Port:
    def __init__(self, type:PortType, name:str, owner:Pipe):
        self.type = type
        self.name:str = name
        self.owner:Pipe = owner
        self.connected_ports:Set["Port"] = set()
        self.buffer:deque = deque()

    def is_buffer_empty(self):
        try:
            _ = self.buffer[0]
            return False
        except IndexError:
            return True
        
    def connect(self, *ports: Union["Port", Pipe]):
        for port in ports:
            actual_port = None
            if isinstance(port, Port):
                actual_port = port
            elif isinstance(port, Pipe):
                if self.type == PortType.INPUT:
                    actual_port = port.get_output_port()
                elif self.type == PortType.OUTPUT:
                    actual_port = port.get_input_port()
                else:
                    raise Exception("bad port type!")
            if actual_port.type == self.type:
                raise Exception("Cannot connect between port of the same type!")
            actual_port.connected_ports.add(self)
            self.connected_ports.add(actual_port)
    
    def emit(self, data:Any):
        if self.type != PortType.OUTPUT:
            raise Exception("Can only emit data to output port!")

        pipeline = self.owner.pipeline

        for port in self.connected_ports:
            # port.owner.on_data_arrival(port.name, data)
            seq = pipeline.get_and_inc_event_seq()
            input_was_empty = self.is_buffer_empty()
            port.buffer.append((seq, data))
            if input_was_empty:
                pipeline.input_q.put(InputPortWrapper(port))



class Splitter(Pipe):
    """
    Copy data from inport port to output port
    routing controls the copy direction
    e.g.
    if routing = {"A": ("X", "Y"), "B": ("T", "V")}
    data from input port "A" will be copied to output port "X" and "Y"
    data from input port "B" will be copied to output port "T" and "V"
    """
    def __init__(self, pipeline:"Pipeline", *, input_names:List[str]=[DEFAULT_PORT_NAME], output_names:List[str]=[DEFAULT_PORT_NAME], routing: Dict[str, Tuple[str]]):
        super().__init__(pipeline, input_names=input_names, output_names=output_names)
        self.routing = routing
        for input_port_name, mapping in routing.items():
            if input_port_name not in self.input_port_names:
                raise Exception(f"{input_port_name} is not a valid input port")
            for output_port_name in mapping:
                if output_port_name not in self.output_port_names:
                    raise Exception(f"{output_port_name} is not a valid output port")


    def on_data_arrival(self, name:str, data:Any):
        output_port_names = self.routing[name]
        for output_port_name in output_port_names:
            self.get_output_port(output_port_name).emit(data)


class InputPortWrapper:
    def __init__(self, port):
        self.port = port
    
    def __gt__(self, other:"InputPortWrapper"):
        src_data_seq = self.port.buffer[0][0]
        dst_data_seq = other.port.buffer[0][0]
        return src_data_seq > dst_data_seq

class Pipeline:
    def __init__(self):
        self.pipe_dict:Dict[str, Pipe] = {}
        self.input_q = PriorityQueue()
        self.event_seq = 0
    
    def get_and_inc_event_seq(self):
        v = self.event_seq
        self.event_seq + 1
        return v
    
    def create_pipe(self, name, klass, *args, **kwargs):
        pipe = klass(self, *args, **kwargs)
        self.pipe_dict[name] = pipe
        return self

    def register(self, name:str, pipe:Pipe):
        self.pipe_dict[name] = pipe
        return self
    
    def connect(self, *, src_pipe_name, src_port_name=DEFAULT_PORT_NAME, dst_pipe_name, dst_port_name=DEFAULT_PORT_NAME):
        src_port = self.pipe_dict[src_pipe_name].OUTPUT[src_port_name]
        dst_port = self.pipe_dict[dst_pipe_name].INPUT[dst_port_name]
        src_port.connect(dst_port)
        return self
    
    def pump(self, on_idle=None):
        while True:
            # let all source to pump data into the pipeline
            for _, pipe in self.pipe_dict.items():
                if isinstance(pipe, Source):
                    while True:
                        if not pipe.pump_wrapper():
                            break
            # process the buffered data
            while True:
                try:
                    ipw = self.input_q.get(block=False)
                    port = ipw.port
                    pipe = port.owner
                    _, data = port.buffer.popleft()
                    pipe.on_data_arrival(port.name, data)
                    if not port.is_buffer_empty():
                        self.input_q.put(ipw)
                except Empty:
                    break
            
            if on_idle is not None:
                on_idle()
    
           
    
