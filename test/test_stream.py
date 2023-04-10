#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from typing import List, Any, Optional
from dltools.streaming import Pipe, Source, Sink, Pipeline
from datetime import datetime, timedelta
import time
import random
from functools import reduce

class BankDepositStream(Source):
    def __init__(self, pipeline:"Pipeline"):
        super().__init__(pipeline)
        self.last_emit_time = datetime.utcnow()

    def pump(self):
        now = datetime.utcnow()
        if now - self.last_emit_time >= timedelta(seconds=1):
            self.emit({
                "time": now,
                "amount": random.randrange(1, 100)
            })
            self.last_emit_time = now

class Square(Pipe):
    def on_data_arrival(self, name:str, data:Any):
        self.output.emit(data*2)

class Counter(Pipe):
    def __init__(self, pipeline:Pipeline):
        super().__init__(pipeline)
        self.deposits = []

    def on_data_arrival(self, name:str, data:Any):
        now = datetime.utcnow()
        cut_off = now - timedelta(minutes=1)
        self.deposits.append(data)
        self.deposits = [d for d in self.deposits if d['time'] >= cut_off]
        amount = reduce(lambda sum, v:sum+v['amount'], self.deposits, 0)
        self.emit(
            {
                "time": now,
                "amount": amount
            }
        )

class PirntSink(Sink):
    def on_data_arrival(self, name:str, data:Any):
        print(f"Sink: {data}")


def main():
    random.seed()
    pipeline = Pipeline()

    pipeline\
        .create_pipe("g", BankDepositStream)\
        .create_pipe("c", Counter)\
        .create_pipe("p", PirntSink)

    pipeline\
        .connect(src_pipe_name="g", dst_pipe_name="c")\
        .connect(src_pipe_name="c", dst_pipe_name="p")

    # simulate bank deposits
    try:
        pipeline.pump(on_idle=lambda : time.sleep(0.5))
    except KeyboardInterrupt:
        print("\nDone")


if __name__ == '__main__':
    main()
