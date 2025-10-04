from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common.time import Time


class TtlFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        ttl_config = (
            StateTtlConfig.new_builder(Time.hours(24))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor = ValueStateDescriptor("lastTimestamp", Types.LONG())
        descriptor.enable_time_to_live(ttl_config)
        self.state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        self.state.update(value[1])  # Triggers WAL write
        yield f"Key: {value[0]}, Value: {self.state.value()}"


env = StreamExecutionEnvironment.get_execution_environment()
# state_backend = RocksDBStateBackend("file:///checkpoint-dir/", True)
# env.set_state_backend(state_backend)
ds = env.from_collection(
    [("user1", 123456), ("user2", 123457)],
    type_info=Types.TUPLE([Types.STRING(), Types.LONG()]),
)
ds.key_by(lambda x: x[0]).process(TtlFunction()).print()
env.execute()
