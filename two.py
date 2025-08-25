from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.common import Configuration, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types

# from statefun import *
import os
import time


class SquareWithState(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # # This state will be stored in RocksDB
        # ttl_config = (
        #     StateTtlConfig.new_builder(Duration.ofSeconds(3))
        #     .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
        #     .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        #     .build()
        # )
        descriptor = ValueStateDescriptor("last_square", Types.INT())
        # descriptor.enable_time_to_live(ttl_config)
        self.state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        # Compute square
        square = value * value
        # time.sleep(0.0000000008 * square)
        if square == 4998 * 4998:
            time.sleep(60)
        # Update state
        self.state.update(square)
        # Emit squared value
        yield square


def main():
    # conf = Configuration()
    # conf.set_string("state.backend", "rocksdb")
    # conf.set_string("state.backend.rocksdb.localdir", "/tmp/rocksdb")
    # conf.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")
    # conf.set_boolean("state.backend.incremental", True)
    # conf.set_string("log4j.rootLogger", "WARN, console")  # high log level

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(200, CheckpointingMode.AT_LEAST_ONCE)

    # t_env = StreamTableEnvironment.create(env)

    numbers = range(1, 5000)
    ds = env.from_collection(numbers, type_info=Types.INT())
    # ds2 = env.from_collection(numbers, type_info=Types.INT())
    # ds2 = env.from_source(1)

    # Key by modulo 2 to create keyed state
    keyed_ds = ds.key_by(lambda x: x % 2)
    # keyed_ds2 = ds.key_by(lambda x: x)

    # Apply stateful process function
    result_ds = keyed_ds.process(SquareWithState(), output_type=Types.INT())
    # .process(
    #     SquareWithState(), output_type=Types.INT()
    # )
    result_ds.print()

    env.execute("Square Numbers with RocksDB Keyed State")


if __name__ == "__main__":
    main()
