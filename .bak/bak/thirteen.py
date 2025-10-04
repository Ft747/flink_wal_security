from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common import Configuration, Types

# from pyflink.datastream.state import RocksDBStateBackend
from pyflink.datastream.state import ValueStateDescriptor


class CountFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        self.counter = runtime_context.get_state(
            ValueStateDescriptor("counter", Types.INT())
        )

    def process_element(self, value, ctx):
        current = self.counter.value() or 0
        self.counter.update(current + 1)  # Triggers WAL write
        yield f"Key: {value[0]}, Count: {self.counter.value()}"


# Configure environment
config = Configuration()
# config.set_string("state.backend.rocksdb.predefined-options", "FLASH_SSD_OPTIMIZED")
# config.set_string("state.backend.rocksdb.localdir", "/data/flink/rocksdb")
# config.set_string("state.backend.rocksdb.memory.fixed-per-slot", "128m")

env = StreamExecutionEnvironment.get_execution_environment(config)
# state_backend = RocksDBStateBackend("file:///checkpoint-dir/", True)
# env.set_state_backend(state_backend)
# env.get_checkpoint_config().set_checkpoint_storage("file:///checkpoint-dir/")

# Create and process stream
ds = env.from_collection(
    [(f"user{i}", i) for i in range(10000)],
    type_info=Types.TUPLE([Types.STRING(), Types.INT()]),
)
ds.key_by(lambda x: x[0]).process(CountFunction()).print()

env.execute("RocksDB Example")
