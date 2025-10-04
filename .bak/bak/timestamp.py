from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor


class MyKeyedProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        self.last_timestamp = runtime_context.get_state(
            ValueStateDescriptor("lastTimestamp", Types.LONG())
        )

    def process_element(self, value, ctx):
        # Update state (triggers WAL write)
        self.last_timestamp.update(value[1])
        yield f"Key: {value[0]}, Last Timestamp: {self.last_timestamp.value()}"


env = StreamExecutionEnvironment.get_execution_environment()
# state_backend = RocksDBStateBackend("file:///checkpoint-dir/", True)
# env.set_state_backend(state_backend)

# Example stream: (key, timestamp)
ds = env.from_collection(
    [("user1", 123456), ("user2", 123457)],
    type_info=Types.TUPLE([Types.STRING(), Types.LONG()]),
)
ds.key_by(lambda x: x[0]).process(MyKeyedProcessFunction()).print()
env.execute()
